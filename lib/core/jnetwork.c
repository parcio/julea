/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021 Julian Benda
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

/// \todo update to JULEA style

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>

#include <netinet/in.h>

#include <jnetwork.h>

#include <jconfiguration.h>
#include <jhelper.h>
#include <jtrace.h>

/**
 * \addtogroup JNetwork Network
 *
 * @{
 **/

#define KEY_MIN 1

/**
 * Highest number of j_network_connection_send() calls before a j_network_connection_wait_for_completion().
 **/
#define J_NETWORK_CONNECTION_MAX_SEND 2

/**
 * Highest number of j_network_connection_recv() calls before a j_network_connection_wait_for_completion().
 **/
#define J_NETWORK_CONNECTION_MAX_RECV 1

/**
 * Fabrics address data needed to send a connection request.
 **/
struct JNetworkFabricAddr
{
	gpointer addr;
	guint32 addr_len;
	guint32 addr_format;
};

typedef struct JNetworkFabricAddr JNetworkFabricAddr;

/**
 * Flag used to different between client and server fabric.
 **/
enum JNetworkFabricSide
{
	JF_SERVER,
	JF_CLIENT
};

typedef enum JNetworkFabricSide JNetworkFabricSide;

struct JNetworkFabric
{
	struct fi_info* info;
	struct fi_info* hints;
	struct fid_fabric* fabric;
	struct fid_eq* pep_eq;
	struct fid_pep* pep;
	JNetworkFabricAddr fabric_addr_network;
	JConfiguration* config;
	JNetworkFabricSide con_side;
};

/**
 * Possible events for listening fabrics
 **/
enum JNetworkFabricEvents
{
	J_FABRIC_EVENT_ERROR = 0, ///< An error was reported, fabric is probably in a invalid state!
	J_FABRIC_EVENT_TIMEOUT, ///< No event received in the given time frame.
	J_FABRIC_EVENT_CONNECTION_REQUEST, ///< A connection request was received.
	J_FABRIC_EVENT_SHUTDOWN ///< fabric socket was closed
};

typedef enum JNetworkFabricEvents JNetworkFabricEvents;

struct JNetworkConnection
{
	JNetworkFabric* fabric;
	struct fi_info* info;
	struct fid_domain* domain;
	struct fid_ep* ep;
	struct fid_eq* eq;
	struct
	{
		struct fid_cq* tx;
		struct fid_cq* rx;
	} cq;
	gsize inject_size;
	struct
	{
		gboolean active;
		struct fid_mr* mr;
		gpointer buffer;
		gsize used;
		gsize buffer_size;
		gsize rx_prefix_size;
		gsize tx_prefix_size;
	} memory;
	struct
	{
		struct
		{
			gpointer context;
			gpointer dest;
			gsize len;
		} msg_entry[J_NETWORK_CONNECTION_MAX_RECV + J_NETWORK_CONNECTION_MAX_SEND];
		gint msg_len;
		gint rma_len;
	} running_actions;
	guint next_key;
	gboolean closed;
};

/// Possible events for paired connections
/** \ingroup network
 *  \sa j_network_connection_sread_event, j_network_connection_read_event
 */
enum JNetworkConnectionEvents
{
	J_CONNECTION_EVENT_ERROR = 0, ///< An error was reported, the connection is probably in an invalid state!
	J_CONNECTION_EVENT_TIMEOUT, ///< there was no event to read in the given time frame
	J_CONNECTION_EVENT_CONNECTED, ///< First event after successful established connection.
	J_CONNECTION_EVENT_SHUTDOWN ///< connection was closed
};

typedef enum JNetworkConnectionEvents JNetworkConnectionEvents;

#define EXE(cmd, ...) \
	do \
	{ \
		if ((cmd) == FALSE) \
		{ \
			g_warning(__VA_ARGS__); \
			goto end; \
		} \
	} while (FALSE)

#define CHECK(msg) \
	do \
	{ \
		if (res < 0) \
		{ \
			g_warning("%s: " msg "\t(%s:%d)\nDetails:\t%s", "??TODO??", __FILE__, __LINE__, fi_strerror(-res)); \
			goto end; \
		} \
	} while (FALSE)

#define G_CHECK(msg) \
	do \
	{ \
		if (error != NULL) \
		{ \
			g_warning(msg "\n\tWith:%s", error->message); \
			g_error_free(error); \
			goto end; \
		} \
	} while (FALSE)

static void
free_dangling_infos(struct fi_info* info)
{
	J_TRACE_FUNCTION(NULL);

	fi_freeinfo(info->next);
	info->next = NULL;
}

/**
 * Reads one event from connection (blocking).
 * Used to check wait for connected state, and recognized errors and shutdown signals.
 * For a version which returns immediate when no event exists, see j_network_connection_read_event.
 *
 * \param this A connection.
 * \param[in] timeout A timeout. Set to -1 for no timeout.
 * \param[out] event An event read from event queue.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_connection_sread_event(JNetworkConnection* this, gint timeout, JNetworkConnectionEvents* event)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	guint32 fi_event;
	gboolean ret = FALSE;
	struct fi_eq_cm_entry entry;

	res = fi_eq_sread(this->eq, &fi_event, &entry, sizeof(entry), timeout, 5);
	if (res == -FI_EAGAIN)
	{
		*event = J_CONNECTION_EVENT_TIMEOUT;
		ret = TRUE;
		goto end;
	}
	else if (res == -FI_EAVAIL)
	{
		struct fi_eq_err_entry error = { 0 };
		*event = J_CONNECTION_EVENT_ERROR;
		do
		{
			res = fi_eq_readerr(this->eq, &error, 0);
			CHECK("Failed to read error!");
			g_warning("event queue contains following error (%s|c:%p):\n\t%s",
				  fi_strerror(FI_EAVAIL), error.context,
				  fi_eq_strerror(this->eq, error.prov_errno, error.err_data, NULL, 0));
		} while (res > 0);
		goto end;
	}
	CHECK("Failed to read event of connection!");

	switch (fi_event)
	{
		case FI_CONNECTED:
			*event = J_CONNECTION_EVENT_CONNECTED;
			break;
		case FI_SHUTDOWN:
			*event = J_CONNECTION_EVENT_SHUTDOWN;
			break;
		default:
			g_assert_not_reached();
			goto end;
	}
	ret = TRUE;
end:
	return ret;
}

/**
 * Reads one event from connection (non-blocking).
 * Returns a J_CONNECTION_EVENT_TIMEOUT event if no event is ready to report.
 *
 * \param this A connection.
 * \param[out] event An event read from event queue.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
/// \todo remove?
/*
static gboolean
j_network_connection_read_event(JNetworkConnection* this, JNetworkConnectionEvents* event)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	guint32 fi_event;
	gboolean ret = FALSE;

	res = fi_eq_read(this->eq, &fi_event, NULL, 0, 0);
	if (res == -FI_EAGAIN)
	{
		*event = J_CONNECTION_EVENT_TIMEOUT;
		ret = TRUE;
		goto end;
	}
	else if (res == -FI_EAVAIL)
	{
		struct fi_eq_err_entry error = { 0 };
		*event = J_CONNECTION_EVENT_ERROR;
		res = fi_eq_readerr(this->eq, &error, 0);
		CHECK("Failed to read error!");
		g_warning("event queue contains following error (%s|c:%p):\n\t%s",
			  fi_strerror(FI_EAVAIL), error.context,
			  fi_eq_strerror(this->eq, error.prov_errno, error.err_data, NULL, 0));
		ret = TRUE;
		goto end;
	}
	CHECK("Failed to read event of connection!");
	switch (fi_event)
	{
		case FI_CONNECTED:
			*event = J_CONNECTION_EVENT_CONNECTED;
			break;
		case FI_SHUTDOWN:
			*event = J_CONNECTION_EVENT_SHUTDOWN;
			break;
		default:
			g_assert_not_reached();
			goto end;
	}
	ret = TRUE;
end:
	return ret;
}
*/

/**
 * Reads an event of a listening fabric.
 * Only usable for fabrics initialized with j_network_fabric_init_server.
 *
 * \param this A fabric.
 * \param[in] timeout A timeout. Set to -1 for no timeout.
 * \param[out] event An event read from event queue.
 * \param[out] con_req A connection request (if event == J_FABRIC_EVENT_CONNECTION_REQUEST).
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_fabric_sread_event(JNetworkFabric* this, gint timeout, JNetworkFabricEvents* event, struct fi_eq_cm_entry* con_req)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	gboolean ret = FALSE;
	guint32 fi_event;

	res = fi_eq_sread(this->pep_eq, &fi_event,
			  con_req, sizeof(*con_req),
			  timeout, 0);
	if (res == -FI_EAGAIN)
	{
		*event = J_FABRIC_EVENT_TIMEOUT;
		ret = TRUE;
		goto end;
	}
	else if (res == -FI_EAVAIL)
	{
		struct fi_eq_err_entry error = { 0 };
		*event = J_FABRIC_EVENT_ERROR;
		res = fi_eq_readerr(this->pep_eq, &error, 0);
		CHECK("Failed to read error!");
		g_warning("event queue contains following error (%s|c:%p):\n\t%s",
			  fi_strerror(FI_EAVAIL), error.context,
			  fi_eq_strerror(this->pep_eq, error.prov_errno, error.err_data, NULL, 0));
		ret = TRUE;
		goto end;
	}
	CHECK("failed to read pep event queue!");
	switch (fi_event)
	{
		case FI_CONNREQ:
			*event = J_FABRIC_EVENT_CONNECTION_REQUEST;
			break;
		case FI_SHUTDOWN:
			*event = J_FABRIC_EVENT_SHUTDOWN;
			break;
		default:
			g_assert_not_reached();
			goto end;
	}
	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_fabric_init_server(JConfiguration* configuration, JNetworkFabric** instance_ptr)
{
	J_TRACE_FUNCTION(NULL);

	JNetworkFabric* this = NULL;
	struct fi_info* hints;
	gint res = 0;
	gsize addrlen = 0;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	this->config = configuration;
	this->con_side = JF_SERVER;

	/// \todo deduplicate FI_VERSION and hints initialization
	hints = fi_allocinfo();
	hints->caps = FI_MSG | FI_SEND | FI_RECV | FI_READ | FI_RMA | FI_REMOTE_READ;
	hints->mode = FI_MSG_PREFIX;
	hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
	hints->ep_attr->type = FI_EP_MSG;
	/// \todo allow setting provider
	hints->fabric_attr->prov_name = NULL;

	res = fi_getinfo(
		FI_VERSION(1, 11),
		NULL, NULL, 0,
		hints,
		&this->info);
	CHECK("Failed to find fabric for server!");
	free_dangling_infos(this->info);

	// no context needed, because we are in sync
	res = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
	CHECK("Failed to open server fabric!");

	// wait obj defined to use synced actions
	res = fi_eq_open(this->fabric,
			 &(struct fi_eq_attr){ .wait_obj = FI_WAIT_UNSPEC },
			 &this->pep_eq, NULL);
	CHECK("failed to create eq for fabric!");
	res = fi_passive_ep(this->fabric, this->info, &this->pep, NULL);
	CHECK("failed to create pep for fabric!");
	res = fi_pep_bind(this->pep, &this->pep_eq->fid, 0);
	CHECK("failed to bind event queue to pep!");

	// initelize addr field!
	res = fi_getname(&this->pep->fid, NULL, &addrlen);
	if (res != -FI_ETOOSMALL)
	{
		CHECK("failed to fetch address len from libfabirc!");
	}
	this->fabric_addr_network.addr_len = addrlen;
	this->fabric_addr_network.addr = malloc(this->fabric_addr_network.addr_len);
	res = fi_getname(&this->pep->fid, this->fabric_addr_network.addr,
			 &addrlen);
	CHECK("failed to fetch address from libfabric!");

	res = fi_listen(this->pep);
	CHECK("failed to start listening on pep!");

	this->fabric_addr_network.addr_len = htonl(this->fabric_addr_network.addr_len);
	this->fabric_addr_network.addr_format = htonl(this->info->addr_format);

	return TRUE;
end:
	return FALSE;
}

/**
 * Initializes a fabric for the client side.
 * Contains data to building a paired connection.
 * Addess of JULEA server is needed, to enforce that both communicate via the same network.
 *
 * \warning Will be called from j_network_connection_init_client, so no need for explicit calls.
 *
 * \param[in] configuration A configuration.
 * \param[in] addr A fabric address.
 * \param[out] instance A pointer to the resulting fabric.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_fabric_init_client(JConfiguration* configuration, JNetworkFabricAddr* addr, JNetworkFabric** instance_ptr)
{
	J_TRACE_FUNCTION(NULL);

	JNetworkFabric* this;
	struct fi_info* hints;
	gint res;
	gboolean ret = FALSE;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	this->config = configuration;
	this->con_side = JF_CLIENT;

	/// \todo deduplicate FI_VERSION and hints initialization
	hints = fi_allocinfo();
	hints->caps = FI_MSG | FI_SEND | FI_RECV | FI_READ | FI_RMA | FI_REMOTE_READ;
	hints->mode = FI_MSG_PREFIX;
	hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
	hints->ep_attr->type = FI_EP_MSG;
	/// \todo allow setting provider
	hints->fabric_attr->prov_name = NULL;

	this->hints = hints;
	this->hints->addr_format = addr->addr_format;
	this->hints->dest_addr = addr->addr;
	this->hints->dest_addrlen = addr->addr_len;

	res = fi_getinfo(
		FI_VERSION(1, 11),
		NULL, NULL, 0,
		this->hints, &this->info);
	CHECK("Failed to find fabric!");
	free_dangling_infos(this->info);

	res = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
	CHECK("failed to initelize client fabric!");

	ret = TRUE;
end:
	return ret;
}

/**
 * Closes a fabirc and frees used memory.
 *
 * \pre Finish all connections created from this fabric.
 *
 * \param this A fabric.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_fabric_fini(JNetworkFabric* this)
{
	J_TRACE_FUNCTION(NULL);

	gint res;

	fi_freeinfo(this->info);
	this->info = NULL;
	if (this->con_side == JF_SERVER)
	{
		res = fi_close(&this->pep->fid);
		CHECK("Failed to close PEP!");
		this->pep = NULL;

		res = fi_close(&this->pep_eq->fid);
		CHECK("Failed to close EQ for PEP!");
		this->pep_eq = NULL;
	}

	res = fi_close(&this->fabric->fid);
	CHECK("failed to close fabric!");
	this->fabric = NULL;
	if (this->hints)
	{
		fi_freeinfo(this->hints);
		this->hints = NULL;
	}
	free(this);
	return TRUE;
end:
	return FALSE;
}

/**
 * Allocates and binds memory needed for message transfer.
 *
 * \param this A connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_connection_create_memory_resources(JNetworkConnection* this)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;
	gint res;
	guint64 op_size, size;
	gsize prefix_size;
	gboolean tx_prefix, rx_prefix;

	op_size = j_configuration_get_max_operation_size(this->fabric->config);
	tx_prefix = (this->info->tx_attr->mode & FI_MSG_PREFIX) != 0;
	rx_prefix = (this->info->rx_attr->mode & FI_MSG_PREFIX) != 0;
	prefix_size = this->info->ep_attr->msg_prefix_size;

	if (op_size + (tx_prefix | rx_prefix) * prefix_size > this->info->ep_attr->max_msg_size)
	{
		guint64 max_size = this->info->ep_attr->max_msg_size - (tx_prefix | rx_prefix) * prefix_size;
		g_critical("Fabric supported memory size is too smal! please configure a max operation size less equal to %lu! instead of %lu",
			   max_size, op_size + (tx_prefix | rx_prefix) * prefix_size);
		goto end;
	}
	size = 0;

	if (this->info->domain_attr->mr_mode & FI_MR_LOCAL)
	{
		size +=
			(rx_prefix * prefix_size) + J_NETWORK_CONNECTION_MAX_RECV * op_size
			+ (tx_prefix * prefix_size) + J_NETWORK_CONNECTION_MAX_SEND * op_size;
		this->memory.active = TRUE;
		this->memory.used = 0;
		this->memory.buffer_size = size;
		this->memory.buffer = malloc(size);
		this->memory.rx_prefix_size = rx_prefix * prefix_size;
		this->memory.tx_prefix_size = tx_prefix * prefix_size;
		res = fi_mr_reg(this->domain, this->memory.buffer, this->memory.buffer_size,
				FI_SEND | FI_RECV, 0, 0, 0, &this->memory.mr, NULL);
		CHECK("Failed to register memory for msg communication!");
	}
	else
	{
		this->memory.active = FALSE;
	}

	ret = TRUE;
end:
	return ret;
}

/**
 * Initializes common parts of a connection.
 * This will create the following libfabric resources:
 * * The domain
 * * An event queue
 * * An RX and TX completion queue
 * * The endpoint
 * It will also bind them accordingly and enable the endpoint.
 *
 * \param this A connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_connection_init(JNetworkConnection* this)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;
	gint res;

	this->running_actions.msg_len = 0;
	this->running_actions.rma_len = 0;
	this->next_key = KEY_MIN;

	res = fi_eq_open(this->fabric->fabric, &(struct fi_eq_attr){ .wait_obj = FI_WAIT_UNSPEC },
			 &this->eq, NULL);
	CHECK("Failed to open event queue for connection!");
	res = fi_domain(this->fabric->fabric, this->info,
			&this->domain, NULL);
	CHECK("Failed to open connection domain!");

	this->inject_size = this->fabric->info->tx_attr->inject_size;
	EXE(j_network_connection_create_memory_resources(this),
	    "Failed to create memory resources for connection!");

	res = fi_cq_open(this->domain, &(struct fi_cq_attr){ .wait_obj = FI_WAIT_UNSPEC, .format = FI_CQ_FORMAT_CONTEXT, .size = this->info->tx_attr->size },
			 &this->cq.tx, &this->cq.tx);
	res = fi_cq_open(this->domain, &(struct fi_cq_attr){ .wait_obj = FI_WAIT_UNSPEC, .format = FI_CQ_FORMAT_CONTEXT, .size = this->info->rx_attr->size },
			 &this->cq.rx, &this->cq.rx);
	CHECK("Failed to create completion queue!");
	res = fi_endpoint(this->domain, this->info, &this->ep, NULL);
	CHECK("Failed to open endpoint for connection!");
	res = fi_ep_bind(this->ep, &this->eq->fid, 0);
	CHECK("Failed to bind event queue to endpoint!");
	res = fi_ep_bind(this->ep, &this->cq.tx->fid, FI_TRANSMIT);
	CHECK("Failed to bind tx completion queue to endpoint!");
	res = fi_ep_bind(this->ep, &this->cq.rx->fid, FI_RECV);
	CHECK("Failed to bind rx completion queue to endpoint!");
	res = fi_enable(this->ep);
	CHECK("Failed to enable connection!");

	this->closed = FALSE;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_connection_init_client(JConfiguration* configuration, JBackendType backend, guint index, JNetworkConnection** instance_ptr)
{
	J_TRACE_FUNCTION(NULL);

	JNetworkConnection* this;
	gboolean ret = FALSE;
	gint res;
	g_autoptr(GSocketClient) g_client = NULL;
	GSocketConnection* g_connection;
	GInputStream* g_input;
	GError* error = NULL;
	const gchar* server;
	JNetworkFabricAddr jf_addr;
	JNetworkConnectionEvents event;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	g_client = g_socket_client_new();
	server = j_configuration_get_server(configuration, backend, index);
	g_connection = g_socket_client_connect_to_host(g_client,
						       server,
						       j_configuration_get_port(configuration), NULL, &error);
	G_CHECK("Failed to build gsocket connection to host");
	if (g_connection == NULL)
	{
		g_warning("Can not connect to %s.", server);
		goto end;
	}

	j_helper_set_nodelay(g_connection, TRUE);

	g_input = g_io_stream_get_input_stream(G_IO_STREAM(g_connection));

	g_input_stream_read(g_input, &jf_addr.addr_format, sizeof(jf_addr.addr_format), NULL, &error);
	G_CHECK("Failed to read addr format from g_connection!");
	jf_addr.addr_format = ntohl(jf_addr.addr_format);

	g_input_stream_read(g_input, &jf_addr.addr_len, sizeof(jf_addr.addr_len), NULL, &error);
	G_CHECK("Failed to read addr len from g_connection!");
	jf_addr.addr_len = ntohl(jf_addr.addr_len);

	jf_addr.addr = malloc(jf_addr.addr_len);
	g_input_stream_read(g_input, jf_addr.addr, jf_addr.addr_len, NULL, &error);
	G_CHECK("Failed to read addr from g_connection!");

	g_input_stream_close(g_input, NULL, &error);
	G_CHECK("Failed to close input stream!");
	g_io_stream_close(G_IO_STREAM(g_connection), NULL, &error);
	G_CHECK("Failed to close gsocket!");

	EXE(j_network_fabric_init_client(configuration, &jf_addr, &this->fabric),
	    "Failed to initelize fabric for client!");
	this->info = this->fabric->info;

	EXE(j_network_connection_init(this),
	    "Failed to initelze connection!");

	res = fi_connect(this->ep, jf_addr.addr, NULL, 0);
	CHECK("Failed to fire connection request!");

	do
	{
		EXE(j_network_connection_sread_event(this, 1, &event),
		    "Failed to read event queue, waiting for CONNECTED signal!");
	} while (event == J_CONNECTION_EVENT_TIMEOUT);

	if (event != J_CONNECTION_EVENT_CONNECTED)
	{
		g_warning("Failed to connect to host!");
		goto end;
	}

	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_connection_init_server(JNetworkFabric* fabric, GSocketConnection* gconnection, JNetworkConnection** instance_ptr)
{
	J_TRACE_FUNCTION(NULL);

	JNetworkConnection* this;
	gboolean ret = FALSE;
	gint res;
	GOutputStream* g_out;
	GError* error = NULL;
	JNetworkFabricAddr* addr = &fabric->fabric_addr_network;
	JNetworkFabricEvents event;
	JNetworkConnectionEvents con_event;
	struct fi_eq_cm_entry request;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	// send addr
	g_out = g_io_stream_get_output_stream(G_IO_STREAM(gconnection));
	g_output_stream_write(g_out, &addr->addr_format, sizeof(addr->addr_format), NULL, &error);
	G_CHECK("Failed to write addr_format to stream!");
	g_output_stream_write(g_out, &addr->addr_len, sizeof(addr->addr_len), NULL, &error);
	G_CHECK("Failed to write addr_len to stream!");
	g_output_stream_write(g_out, addr->addr, ntohl(addr->addr_len), NULL, &error);
	G_CHECK("Failed to write addr to stream!");
	g_output_stream_close(g_out, NULL, &error);
	G_CHECK("Failed to close output stream!");

	do
	{
		EXE(j_network_fabric_sread_event(fabric, 2, &event, &request),
		    "Failed to wait for connection request");
	} while (event == J_FABRIC_EVENT_TIMEOUT);
	if (event != J_FABRIC_EVENT_CONNECTION_REQUEST)
	{
		g_warning("expected an connection request and nothing else! (%i)", event);
		goto end;
	}
	this->fabric = fabric;
	this->info = request.info;

	EXE(j_network_connection_init(this), "Failed to initelize connection server side!");

	res = fi_accept(this->ep, NULL, 0);
	CHECK("Failed to accept connection!");
	EXE(j_network_connection_sread_event(this, 2, &con_event), "Failed to verify connection!");
	if (con_event != J_CONNECTION_EVENT_CONNECTED)
	{
		g_warning("expected and connection ack and nothing else!");
		goto end;
	}

	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_connection_send(JNetworkConnection* this, gconstpointer data, gsize data_len)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	gboolean ret = FALSE;
	gpointer context;
	gsize size;

	// we used paired endponits -> inject and send don't need destination addr (last parameter)

	if (data_len < this->inject_size)
	{
		do
		{
			res = fi_inject(this->ep, data, data_len, 0);
		} while (res == -FI_EAGAIN);
		CHECK("Failed to inject data!");
		ret = TRUE;
		goto end;
	}

	// normal send
	if (this->memory.active)
	{
		uint8_t* segment = (uint8_t*)this->memory.buffer + this->memory.used;
		context = segment;
		memcpy(segment + this->memory.tx_prefix_size,
		       data, data_len);
		size = data_len + this->memory.tx_prefix_size;
		do
		{
			res = fi_send(this->ep, segment, size, fi_mr_desc(this->memory.mr), 0, context);
		} while (res == -FI_EAGAIN);
		CHECK("Failed to initelize sending!");
		this->memory.used += size;
		g_assert_true(this->memory.used <= this->memory.buffer_size);
		g_assert_true(this->running_actions.msg_len + 1 < J_NETWORK_CONNECTION_MAX_SEND + J_NETWORK_CONNECTION_MAX_RECV);
	}
	else
	{
		context = (gpointer)((const char*)data - (const char*)NULL);
		size = data_len;
		do
		{
			res = fi_send(this->ep, data, size, NULL, 0, context);
		} while (res == -FI_EAGAIN);
	}
	this->running_actions.msg_entry[this->running_actions.msg_len].context = context;
	this->running_actions.msg_entry[this->running_actions.msg_len].dest = NULL;
	this->running_actions.msg_entry[this->running_actions.msg_len].len = 0;
	++this->running_actions.msg_len;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_connection_recv(JNetworkConnection* this, gsize data_len, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;
	gint res;
	gpointer segment;
	gsize size;

	segment = this->memory.active ? (char*)this->memory.buffer + this->memory.used : data;
	size = data_len + this->memory.rx_prefix_size;
	res = fi_recv(this->ep, segment, size,
		      this->memory.active ? fi_mr_desc(this->memory.mr) : NULL,
		      0, segment);
	CHECK("Failed to initelized receiving!");

	if (this->memory.active)
	{
		this->memory.used += size;
		g_assert_true(this->memory.used <= this->memory.buffer_size);
		g_assert_true(this->running_actions.msg_len < J_NETWORK_CONNECTION_MAX_SEND + J_NETWORK_CONNECTION_MAX_RECV);
		this->running_actions.msg_entry[this->running_actions.msg_len].context = segment;
		this->running_actions.msg_entry[this->running_actions.msg_len].dest = data;
		this->running_actions.msg_entry[this->running_actions.msg_len].len = data_len;
	}
	else
	{
		this->running_actions.msg_entry[this->running_actions.msg_len].context = segment;
		this->running_actions.msg_entry[this->running_actions.msg_len].dest = NULL;
		this->running_actions.msg_entry[this->running_actions.msg_len].len = 0;
	}
	++this->running_actions.msg_len;
	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_connection_closed(JNetworkConnection* this)
{
	J_TRACE_FUNCTION(NULL);

	return this->closed;
}

gboolean
j_network_connection_wait_for_completion(JNetworkConnection* this)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;
	gint res;
	struct fi_cq_entry entry;
	gint i;

	while (this->running_actions.rma_len + this->running_actions.msg_len)
	{
		bool rx;
		do
		{
			rx = TRUE;
			res = fi_cq_read(this->cq.rx, &entry, 1);
			if (res == -FI_EAGAIN)
			{
				rx = FALSE;
				res = fi_cq_read(this->cq.tx, &entry, 1);
			}
		} while (res == -FI_EAGAIN);
		if (res == -FI_EAVAIL)
		{
			JNetworkConnectionEvents event;
			struct fi_cq_err_entry err_entry;

			j_network_connection_sread_event(this, 0, &event);
			if (event == J_CONNECTION_EVENT_SHUTDOWN)
			{
				this->closed = TRUE;
				goto end;
			}
			res = fi_cq_readerr(rx ? this->cq.rx : this->cq.tx,
					    &err_entry, 0);
			CHECK("Failed to read error of cq!");
			g_warning("Failed to read completion queue\nWidth:\t%s",
				  fi_cq_strerror(rx ? this->cq.rx : this->cq.tx,
						 err_entry.prov_errno, err_entry.err_data, NULL, 0));
			goto end;
		}
		else
		{
			CHECK("Failed to read completion queue!");
		}
		for (i = 0; i <= this->running_actions.msg_len; ++i)
		{
			if (i == this->running_actions.msg_len)
			{
				// If there is no match -> it's an rma transafre -> context = memory region
				res = fi_close(&((struct fid_mr*)entry.op_context)->fid);
				CHECK("Failed to free receiving memory!");
				--this->running_actions.rma_len;
			}
			if (this->running_actions.msg_entry[i].context == entry.op_context)
			{
				--this->running_actions.msg_len;
				if (this->running_actions.msg_entry[i].dest)
				{
					memcpy(this->running_actions.msg_entry[i].dest,
					       this->running_actions.msg_entry[i].context,
					       this->running_actions.msg_entry[i].len);
				}
				this->running_actions.msg_entry[i] = this->running_actions.msg_entry[this->running_actions.msg_len];
				break;
			}
		}
	}
	this->memory.used = 0;
	ret = TRUE;
end:
	// g_message("\t\toverhead: %f", (double)(sum)/CLOCKS_PER_SEC);
	return ret;
}

gboolean
j_network_connection_rma_register(JNetworkConnection* this, gconstpointer data, gsize data_len, JNetworkConnectionMemory* handle)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	gboolean ret = FALSE;

	res = fi_mr_reg(this->domain,
			data,
			data_len,
			FI_REMOTE_READ,
			0, this->next_key, 0, &handle->memory_region, NULL);
	CHECK("Failed to register memory region!");
	handle->addr = (this->info->domain_attr->mr_mode & FI_MR_VIRT_ADDR) ? (guint64)data : 0;
	handle->size = data_len;
	this->next_key += 1;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_network_connection_rma_unregister(JNetworkConnection* this, JNetworkConnectionMemory* handle)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	this->next_key = KEY_MIN;
	res = fi_close(&handle->memory_region->fid);
	CHECK("Failed to unregistrer rma memory!");
	return TRUE;
end:
	return FALSE;
}

gboolean
j_network_connection_memory_get_id(JNetworkConnectionMemory* this, JNetworkConnectionMemoryID* id)
{
	J_TRACE_FUNCTION(NULL);

	id->size = this->size;
	id->key = fi_mr_key(this->memory_region);
	id->offset = this->addr;
	return TRUE;
}

gboolean
j_network_connection_rma_read(JNetworkConnection* this, const JNetworkConnectionMemoryID* memoryID, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	gboolean ret = FALSE;
	struct fid_mr* mr;
	static unsigned key = 0;

	res = fi_mr_reg(this->domain, data, memoryID->size,
			FI_READ, 0, ++key, 0, &mr, 0);
	CHECK("Failed to register receiving memory!");
	do
	{
		res = fi_read(this->ep,
			      data,
			      memoryID->size,
			      fi_mr_desc(mr),
			      0,
			      memoryID->offset,
			      memoryID->key,
			      mr);
		if (res == -FI_EAGAIN)
		{
			/// \todo evaluate: only wait for partcial finished jobs
			j_network_connection_wait_for_completion(this);
		}
	} while (res == -FI_EAGAIN);
	CHECK("Failed to initiate reading");
	++this->running_actions.rma_len;
	ret = TRUE;
end:
	if (!ret)
	{
		fi_close(&mr->fid);
	}
	return ret;
}

gboolean
j_network_connection_fini(JNetworkConnection* this)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	gboolean ret = FALSE;

	res = fi_shutdown(this->ep, 0);
	CHECK("failed to send shutdown signal");

	if (this->memory.active)
	{
		res = fi_close(&this->memory.mr->fid);
		CHECK("failed to free memory region!");
		free(this->memory.buffer);
	}

	res = fi_close(&this->ep->fid);
	CHECK("failed to close endpoint!");
	res = fi_close(&this->cq.tx->fid);
	CHECK("failed to close tx cq!");
	res = fi_close(&this->cq.rx->fid);
	CHECK("failed to close rx cq!");
	res = fi_close(&this->eq->fid);
	CHECK("failed to close event queue!");
	res = fi_close(&this->domain->fid);
	CHECK("failed to close domain!");

	if (this->fabric->con_side == JF_CLIENT)
	{
		j_network_fabric_fini(this->fabric);
	}

	free(this);
	ret = TRUE;
end:
	return ret;
}

/**
 * @}
 **/
