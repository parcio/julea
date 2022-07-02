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
	JConfiguration* config;
	JNetworkFabricAddr fabric_addr_network;
	JNetworkFabricSide con_side;

	struct fi_info* info;
	struct fi_info* hints;

	struct fid_fabric* fabric;

	struct fid_eq* pep_eq;
	struct fid_pep* pep;
};

/**
 * Possible events for listening fabrics
 **/
enum JNetworkFabricEvents
{
	/**
	 * An error was reported, fabric is probably in a invalid state!
	 **/
	J_FABRIC_EVENT_ERROR = 0,
	/**
	 * No event received in the given time frame.
	 **/
	J_FABRIC_EVENT_TIMEOUT,
	/**
	 * A connection request was received.
	 **/
	J_FABRIC_EVENT_CONNECTION_REQUEST,
	/**
	 * Fabric socket was closed.
	 **/
	J_FABRIC_EVENT_SHUTDOWN
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

/**
 * Possible events for paired connections.
 */
enum JNetworkConnectionEvents
{
	/**
	 * An error was reported, the connection is probably in an invalid state!
	 **/
	J_CONNECTION_EVENT_ERROR = 0,
	/**
	 * There was no event to read in the given time frame.
	 **/
	J_CONNECTION_EVENT_TIMEOUT,
	/**
	 * First event after successful established connection.
	 **/
	J_CONNECTION_EVENT_CONNECTED,
	/**
	 * Connection was closed.
	 **/
	J_CONNECTION_EVENT_SHUTDOWN
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
j_network_connection_sread_event(JNetworkConnection* connection, gint timeout, JNetworkConnectionEvents* event)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	struct fi_eq_cm_entry entry;

	gint res;
	guint32 fi_event;

	res = fi_eq_sread(connection->eq, &fi_event, &entry, sizeof(entry), timeout, 5);

	if (res == -FI_EAGAIN)
	{
		ret = TRUE;
		*event = J_CONNECTION_EVENT_TIMEOUT;
		goto end;
	}
	else if (res == -FI_EAVAIL)
	{
		struct fi_eq_err_entry error = { 0 };

		*event = J_CONNECTION_EVENT_ERROR;

		do
		{
			res = fi_eq_readerr(connection->eq, &error, 0);
			CHECK("Failed to read error!");

			g_warning("event queue contains following error (%s|c:%p):\n\t%s", fi_strerror(FI_EAVAIL), error.context, fi_eq_strerror(connection->eq, error.prov_errno, error.err_data, NULL, 0));
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
 * \param connection A connection.
 * \param[out] event An event read from event queue.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
/// \todo remove?
/*
static gboolean
j_network_connection_read_event(JNetworkConnection* connection, JNetworkConnectionEvents* event)
{
	J_TRACE_FUNCTION(NULL);

	gint res;
	guint32 fi_event;
	gboolean ret = FALSE;

	res = fi_eq_read(connection->eq, &fi_event, NULL, 0, 0);
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
		res = fi_eq_readerr(connection->eq, &error, 0);
		CHECK("Failed to read error!");
		g_warning("event queue contains following error (%s|c:%p):\n\t%s",
			  fi_strerror(FI_EAVAIL), error.context,
			  fi_eq_strerror(connection->eq, error.prov_errno, error.err_data, NULL, 0));
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
j_network_fabric_sread_event(JNetworkFabric* fabric, gint timeout, JNetworkFabricEvents* event, struct fi_eq_cm_entry* con_req)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;
	guint32 fi_event;

	res = fi_eq_sread(fabric->pep_eq, &fi_event, con_req, sizeof(*con_req), timeout, 0);

	if (res == -FI_EAGAIN)
	{
		ret = TRUE;
		*event = J_FABRIC_EVENT_TIMEOUT;
		goto end;
	}
	else if (res == -FI_EAVAIL)
	{
		struct fi_eq_err_entry error = { 0 };

		*event = J_FABRIC_EVENT_ERROR;

		res = fi_eq_readerr(fabric->pep_eq, &error, 0);
		CHECK("Failed to read error!");

		g_warning("event queue contains following error (%s|c:%p):\n\t%s", fi_strerror(FI_EAVAIL), error.context, fi_eq_strerror(fabric->pep_eq, error.prov_errno, error.err_data, NULL, 0));

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

	JNetworkFabric* fabric = NULL;

	struct fi_info* hints;

	gint res = 0;
	gsize addrlen = 0;

	*instance_ptr = g_new0(JNetworkFabric, 1);

	fabric = *instance_ptr;
	fabric->config = configuration;
	fabric->con_side = JF_SERVER;

	/// \todo deduplicate FI_VERSION and hints initialization
	hints = fi_allocinfo();
	hints->caps = FI_MSG | FI_SEND | FI_RECV | FI_READ | FI_RMA | FI_REMOTE_READ;
	hints->mode = FI_MSG_PREFIX;
	hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
	hints->ep_attr->type = FI_EP_MSG;
	/// \todo allow setting provider
	hints->fabric_attr->prov_name = NULL;

	res = fi_getinfo(FI_VERSION(1, 11), NULL, NULL, 0, hints, &fabric->info);
	CHECK("Failed to find fabric for server!");

	free_dangling_infos(fabric->info);

	// no context needed, because we are in sync
	res = fi_fabric(fabric->info->fabric_attr, &fabric->fabric, NULL);
	CHECK("Failed to open server fabric!");

	// wait obj defined to use synced actions
	res = fi_eq_open(fabric->fabric, &(struct fi_eq_attr){ .wait_obj = FI_WAIT_UNSPEC }, &fabric->pep_eq, NULL);
	CHECK("failed to create eq for fabric!");

	res = fi_passive_ep(fabric->fabric, fabric->info, &fabric->pep, NULL);
	CHECK("failed to create pep for fabric!");

	res = fi_pep_bind(fabric->pep, &fabric->pep_eq->fid, 0);
	CHECK("failed to bind event queue to pep!");

	// initialize addr field!
	res = fi_getname(&fabric->pep->fid, NULL, &addrlen);

	if (res != -FI_ETOOSMALL)
	{
		CHECK("failed to fetch address len from libfabirc!");
	}

	fabric->fabric_addr_network.addr_len = addrlen;
	fabric->fabric_addr_network.addr = g_malloc(fabric->fabric_addr_network.addr_len);

	res = fi_getname(&fabric->pep->fid, fabric->fabric_addr_network.addr, &addrlen);
	CHECK("failed to fetch address from libfabric!");

	res = fi_listen(fabric->pep);
	CHECK("failed to start listening on pep!");

	fabric->fabric_addr_network.addr_len = htonl(fabric->fabric_addr_network.addr_len);
	fabric->fabric_addr_network.addr_format = htonl(fabric->info->addr_format);

	return TRUE;

end:
	return FALSE;
}

/**
 * Initializes a fabric for the client side.
 * Contains data to building a paired connection.
 * Adress of JULEA server is needed, to enforce that both communicate via the same network.
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

	JNetworkFabric* fabric;

	struct fi_info* hints;

	gint res;
	gboolean ret = FALSE;

	*instance_ptr = g_new0(JNetworkFabric, 1);

	fabric = *instance_ptr;
	fabric->config = configuration;
	fabric->con_side = JF_CLIENT;

	/// \todo deduplicate FI_VERSION and hints initialization
	hints = fi_allocinfo();
	hints->caps = FI_MSG | FI_SEND | FI_RECV | FI_READ | FI_RMA | FI_REMOTE_READ;
	hints->mode = FI_MSG_PREFIX;
	hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
	hints->ep_attr->type = FI_EP_MSG;
	/// \todo allow setting provider
	hints->fabric_attr->prov_name = NULL;

	fabric->hints = hints;
	fabric->hints->addr_format = addr->addr_format;
	fabric->hints->dest_addr = addr->addr;
	fabric->hints->dest_addrlen = addr->addr_len;

	res = fi_getinfo(FI_VERSION(1, 11), NULL, NULL, 0, fabric->hints, &fabric->info);
	CHECK("Failed to find fabric!");

	free_dangling_infos(fabric->info);

	res = fi_fabric(fabric->info->fabric_attr, &fabric->fabric, NULL);
	CHECK("failed to initelize client fabric!");

	ret = TRUE;

end:
	return ret;
}

/**
 * Closes a fabric and frees used memory.
 *
 * \pre Finish all connections created from this fabric.
 *
 * \param this A fabric.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_fabric_fini(JNetworkFabric* fabric)
{
	J_TRACE_FUNCTION(NULL);

	gint res;

	fi_freeinfo(fabric->info);
	fabric->info = NULL;

	if (fabric->con_side == JF_SERVER)
	{
		res = fi_close(&fabric->pep->fid);
		CHECK("Failed to close PEP!");
		fabric->pep = NULL;

		res = fi_close(&fabric->pep_eq->fid);
		CHECK("Failed to close EQ for PEP!");
		fabric->pep_eq = NULL;
	}

	res = fi_close(&fabric->fabric->fid);
	CHECK("failed to close fabric!");
	fabric->fabric = NULL;

	if (fabric->hints)
	{
		fi_freeinfo(fabric->hints);
		fabric->hints = NULL;
	}

	g_free(fabric);

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
j_network_connection_create_memory_resources(JNetworkConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;
	guint64 op_size, size;
	gsize prefix_size;
	gboolean tx_prefix, rx_prefix;

	op_size = j_configuration_get_max_operation_size(connection->fabric->config);
	tx_prefix = (connection->info->tx_attr->mode & FI_MSG_PREFIX) != 0;
	rx_prefix = (connection->info->rx_attr->mode & FI_MSG_PREFIX) != 0;
	prefix_size = connection->info->ep_attr->msg_prefix_size;

	if (op_size + (tx_prefix | rx_prefix) * prefix_size > connection->info->ep_attr->max_msg_size)
	{
		guint64 max_size = connection->info->ep_attr->max_msg_size - (tx_prefix | rx_prefix) * prefix_size;

		g_critical("Fabric supported memory size is too smal! please configure a max operation size less equal to %lu! instead of %lu", max_size, op_size + (tx_prefix | rx_prefix) * prefix_size);

		goto end;
	}

	size = 0;

	if (connection->info->domain_attr->mr_mode & FI_MR_LOCAL)
	{
		size += (rx_prefix * prefix_size) + J_NETWORK_CONNECTION_MAX_RECV * op_size + (tx_prefix * prefix_size) + J_NETWORK_CONNECTION_MAX_SEND * op_size;

		connection->memory.active = TRUE;
		connection->memory.used = 0;
		connection->memory.buffer_size = size;
		connection->memory.buffer = g_malloc(size);
		connection->memory.rx_prefix_size = rx_prefix * prefix_size;
		connection->memory.tx_prefix_size = tx_prefix * prefix_size;

		res = fi_mr_reg(connection->domain, connection->memory.buffer, connection->memory.buffer_size, FI_SEND | FI_RECV, 0, 0, 0, &connection->memory.mr, NULL);
		CHECK("Failed to register memory for msg communication!");
	}
	else
	{
		connection->memory.active = FALSE;
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
 * \param connection A connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_network_connection_init(JNetworkConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;

	connection->running_actions.msg_len = 0;
	connection->running_actions.rma_len = 0;
	connection->next_key = KEY_MIN;

	res = fi_eq_open(connection->fabric->fabric, &(struct fi_eq_attr){ .wait_obj = FI_WAIT_UNSPEC }, &connection->eq, NULL);
	CHECK("Failed to open event queue for connection!");

	res = fi_domain(connection->fabric->fabric, connection->info, &connection->domain, NULL);
	CHECK("Failed to open connection domain!");

	connection->inject_size = connection->fabric->info->tx_attr->inject_size;

	EXE(j_network_connection_create_memory_resources(connection), "Failed to create memory resources for connection!");

	res = fi_cq_open(connection->domain, &(struct fi_cq_attr){ .wait_obj = FI_WAIT_UNSPEC, .format = FI_CQ_FORMAT_CONTEXT, .size = connection->info->tx_attr->size }, &connection->cq.tx, &connection->cq.tx);
	res = fi_cq_open(connection->domain, &(struct fi_cq_attr){ .wait_obj = FI_WAIT_UNSPEC, .format = FI_CQ_FORMAT_CONTEXT, .size = connection->info->rx_attr->size }, &connection->cq.rx, &connection->cq.rx);
	CHECK("Failed to create completion queue!");

	res = fi_endpoint(connection->domain, connection->info, &connection->ep, NULL);
	CHECK("Failed to open endpoint for connection!");

	res = fi_ep_bind(connection->ep, &connection->eq->fid, 0);
	CHECK("Failed to bind event queue to endpoint!");

	res = fi_ep_bind(connection->ep, &connection->cq.tx->fid, FI_TRANSMIT);
	CHECK("Failed to bind tx completion queue to endpoint!");

	res = fi_ep_bind(connection->ep, &connection->cq.rx->fid, FI_RECV);
	CHECK("Failed to bind rx completion queue to endpoint!");

	res = fi_enable(connection->ep);
	CHECK("Failed to enable connection!");

	connection->closed = FALSE;

	ret = TRUE;

end:
	return ret;
}

gboolean
j_network_connection_init_client(JConfiguration* configuration, JBackendType backend, guint index, JNetworkConnection** instance_ptr)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	JNetworkFabricAddr jf_addr;
	JNetworkConnection* connection;
	JNetworkConnectionEvents event;

	GError* error = NULL;
	GInputStream* g_input;
	GSocketConnection* g_connection;
	g_autoptr(GSocketClient) g_client = NULL;

	gint res;
	gchar const* server;

	*instance_ptr = g_new0(JNetworkConnection, 1);
	connection = *instance_ptr;

	g_client = g_socket_client_new();
	server = j_configuration_get_server(configuration, backend, index);
	g_connection = g_socket_client_connect_to_host(g_client, server, j_configuration_get_port(configuration), NULL, &error);
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

	jf_addr.addr = g_malloc(jf_addr.addr_len);
	g_input_stream_read(g_input, jf_addr.addr, jf_addr.addr_len, NULL, &error);
	G_CHECK("Failed to read addr from g_connection!");

	g_input_stream_close(g_input, NULL, &error);
	G_CHECK("Failed to close input stream!");

	g_io_stream_close(G_IO_STREAM(g_connection), NULL, &error);
	G_CHECK("Failed to close gsocket!");

	EXE(j_network_fabric_init_client(configuration, &jf_addr, &connection->fabric), "Failed to initialize fabric for client!");
	connection->info = connection->fabric->info;

	EXE(j_network_connection_init(connection), "Failed to initelze connection!");

	res = fi_connect(connection->ep, jf_addr.addr, NULL, 0);
	CHECK("Failed to fire connection request!");

	do
	{
		EXE(j_network_connection_sread_event(connection, 1, &event), "Failed to read event queue, waiting for CONNECTED signal!");
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

	gboolean ret = FALSE;

	JNetworkConnection* connection;
	JNetworkConnectionEvents con_event;
	JNetworkFabricAddr* addr = &fabric->fabric_addr_network;
	JNetworkFabricEvents event;

	GError* error = NULL;
	GOutputStream* g_out;

	gint res;
	struct fi_eq_cm_entry request;

	*instance_ptr = g_new0(JNetworkConnection, 1);
	connection = *instance_ptr;

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
		EXE(j_network_fabric_sread_event(fabric, 2, &event, &request), "Failed to wait for connection request");
	} while (event == J_FABRIC_EVENT_TIMEOUT);

	if (event != J_FABRIC_EVENT_CONNECTION_REQUEST)
	{
		g_warning("expected an connection request and nothing else! (%i)", event);
		goto end;
	}

	connection->fabric = fabric;
	connection->info = request.info;

	EXE(j_network_connection_init(connection), "Failed to initelize connection server side!");

	res = fi_accept(connection->ep, NULL, 0);
	CHECK("Failed to accept connection!");

	EXE(j_network_connection_sread_event(connection, 2, &con_event), "Failed to verify connection!");

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
j_network_connection_send(JNetworkConnection* connection, gpointer data, gsize data_len)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;
	gpointer context;
	gsize size;

	// we used paired endponits -> inject and send don't need destination addr (last parameter)

	if (data_len < connection->inject_size)
	{
		do
		{
			res = fi_inject(connection->ep, data, data_len, 0);
		} while (res == -FI_EAGAIN);

		CHECK("Failed to inject data!");

		ret = TRUE;
		goto end;
	}

	// normal send
	if (connection->memory.active)
	{
		uint8_t* segment = (uint8_t*)connection->memory.buffer + connection->memory.used;

		context = segment;
		memcpy(segment + connection->memory.tx_prefix_size, data, data_len);
		size = data_len + connection->memory.tx_prefix_size;

		do
		{
			res = fi_send(connection->ep, segment, size, fi_mr_desc(connection->memory.mr), 0, context);
		} while (res == -FI_EAGAIN);

		CHECK("Failed to initelize sending!");

		connection->memory.used += size;

		g_assert_true(connection->memory.used <= connection->memory.buffer_size);
		g_assert_true(connection->running_actions.msg_len + 1 < J_NETWORK_CONNECTION_MAX_SEND + J_NETWORK_CONNECTION_MAX_RECV);
	}
	else
	{
		context = data;
		size = data_len;

		do
		{
			res = fi_send(connection->ep, data, size, NULL, 0, context);
		} while (res == -FI_EAGAIN);
	}

	connection->running_actions.msg_entry[connection->running_actions.msg_len].context = context;
	connection->running_actions.msg_entry[connection->running_actions.msg_len].dest = NULL;
	connection->running_actions.msg_entry[connection->running_actions.msg_len].len = 0;
	connection->running_actions.msg_len++;

	ret = TRUE;

end:
	return ret;
}

gboolean
j_network_connection_recv(JNetworkConnection* connection, gsize data_len, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;
	gpointer segment;
	gsize size;

	segment = connection->memory.active ? (char*)connection->memory.buffer + connection->memory.used : data;
	size = data_len + connection->memory.rx_prefix_size;

	res = fi_recv(connection->ep, segment, size, connection->memory.active ? fi_mr_desc(connection->memory.mr) : NULL, 0, segment);
	CHECK("Failed to initelized receiving!");

	if (connection->memory.active)
	{
		connection->memory.used += size;

		g_assert_true(connection->memory.used <= connection->memory.buffer_size);
		g_assert_true(connection->running_actions.msg_len < J_NETWORK_CONNECTION_MAX_SEND + J_NETWORK_CONNECTION_MAX_RECV);

		connection->running_actions.msg_entry[connection->running_actions.msg_len].context = segment;
		connection->running_actions.msg_entry[connection->running_actions.msg_len].dest = data;
		connection->running_actions.msg_entry[connection->running_actions.msg_len].len = data_len;
	}
	else
	{
		connection->running_actions.msg_entry[connection->running_actions.msg_len].context = segment;
		connection->running_actions.msg_entry[connection->running_actions.msg_len].dest = NULL;
		connection->running_actions.msg_entry[connection->running_actions.msg_len].len = 0;
	}

	connection->running_actions.msg_len++;

	return TRUE;

end:
	return ret;
}

gboolean
j_network_connection_closed(JNetworkConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	return connection->closed;
}

gboolean
j_network_connection_wait_for_completion(JNetworkConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	struct fi_cq_entry entry;

	gint res;
	gint i;

	while (connection->running_actions.rma_len + connection->running_actions.msg_len)
	{
		gboolean rx;

		do
		{
			rx = TRUE;
			res = fi_cq_read(connection->cq.rx, &entry, 1);

			if (res == -FI_EAGAIN)
			{
				rx = FALSE;
				res = fi_cq_read(connection->cq.tx, &entry, 1);
			}
		} while (res == -FI_EAGAIN);

		if (res == -FI_EAVAIL)
		{
			JNetworkConnectionEvents event;

			struct fi_cq_err_entry err_entry;

			j_network_connection_sread_event(connection, 0, &event);

			if (event == J_CONNECTION_EVENT_SHUTDOWN)
			{
				connection->closed = TRUE;
				goto end;
			}

			res = fi_cq_readerr(rx ? connection->cq.rx : connection->cq.tx, &err_entry, 0);
			CHECK("Failed to read error of cq!");

			g_warning("Failed to read completion queue\nWidth:\t%s", fi_cq_strerror(rx ? connection->cq.rx : connection->cq.tx, err_entry.prov_errno, err_entry.err_data, NULL, 0));

			goto end;
		}
		else
		{
			CHECK("Failed to read completion queue!");
		}

		for (i = 0; i <= connection->running_actions.msg_len; i++)
		{
			if (i == connection->running_actions.msg_len)
			{
				// If there is no match -> it's an rma transafer -> context = memory region
				res = fi_close(&((struct fid_mr*)entry.op_context)->fid);
				CHECK("Failed to free receiving memory!");

				connection->running_actions.rma_len--;
			}
			if (connection->running_actions.msg_entry[i].context == entry.op_context)
			{
				connection->running_actions.msg_len--;

				if (connection->running_actions.msg_entry[i].dest)
				{
					memcpy(connection->running_actions.msg_entry[i].dest, connection->running_actions.msg_entry[i].context, connection->running_actions.msg_entry[i].len);
				}

				connection->running_actions.msg_entry[i] = connection->running_actions.msg_entry[connection->running_actions.msg_len];

				break;
			}
		}
	}

	connection->memory.used = 0;

	return TRUE;

end:
	// g_message("\t\toverhead: %f", (double)(sum)/CLOCKS_PER_SEC);
	return ret;
}

gboolean
j_network_connection_rma_register(JNetworkConnection* connection, gconstpointer data, gsize data_len, JNetworkConnectionMemory* handle)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;

	res = fi_mr_reg(connection->domain, data, data_len, FI_REMOTE_READ, 0, connection->next_key, 0, &handle->memory_region, NULL);
	CHECK("Failed to register memory region!");

	handle->addr = (connection->info->domain_attr->mr_mode & FI_MR_VIRT_ADDR) ? (guint64)data : 0;
	handle->size = data_len;
	connection->next_key += 1;

	return TRUE;

end:
	return ret;
}

gboolean
j_network_connection_rma_unregister(JNetworkConnection* connection, JNetworkConnectionMemory* handle)
{
	J_TRACE_FUNCTION(NULL);

	gint res;

	connection->next_key = KEY_MIN;

	res = fi_close(&handle->memory_region->fid);
	CHECK("Failed to unregistrer rma memory!");

	return TRUE;

end:
	return FALSE;
}

gboolean
j_network_connection_memory_get_id(JNetworkConnectionMemory* memory, JNetworkConnectionMemoryID* id)
{
	J_TRACE_FUNCTION(NULL);

	id->size = memory->size;
	id->key = fi_mr_key(memory->memory_region);
	id->offset = memory->addr;

	return TRUE;
}

gboolean
j_network_connection_rma_read(JNetworkConnection* connection, const JNetworkConnectionMemoryID* memoryID, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	struct fid_mr* mr;

	gint res;
	/// \todo static? thread-safety
	static unsigned key = 0;

	res = fi_mr_reg(connection->domain, data, memoryID->size, FI_READ, 0, ++key, 0, &mr, 0);
	CHECK("Failed to register receiving memory!");

	do
	{
		res = fi_read(connection->ep, data, memoryID->size, fi_mr_desc(mr), 0, memoryID->offset, memoryID->key, mr);

		if (res == -FI_EAGAIN)
		{
			/// \todo evaluate: only wait for partcial finished jobs
			j_network_connection_wait_for_completion(connection);
		}
	} while (res == -FI_EAGAIN);

	CHECK("Failed to initiate reading");

	connection->running_actions.rma_len++;

	return TRUE;

end:
	if (!ret)
	{
		fi_close(&mr->fid);
	}

	return ret;
}

gboolean
j_network_connection_fini(JNetworkConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	gint res;

	res = fi_shutdown(connection->ep, 0);
	CHECK("failed to send shutdown signal");

	if (connection->memory.active)
	{
		res = fi_close(&connection->memory.mr->fid);
		CHECK("failed to free memory region!");
		g_free(connection->memory.buffer);
	}

	res = fi_close(&connection->ep->fid);
	CHECK("failed to close endpoint!");

	res = fi_close(&connection->cq.tx->fid);
	CHECK("failed to close tx cq!");

	res = fi_close(&connection->cq.rx->fid);
	CHECK("failed to close rx cq!");

	res = fi_close(&connection->eq->fid);
	CHECK("failed to close event queue!");

	res = fi_close(&connection->domain->fid);
	CHECK("failed to close domain!");

	if (connection->fabric->con_side == JF_CLIENT)
	{
		j_network_fabric_fini(connection->fabric);
	}

	g_free(connection);

	return TRUE;

end:
	return ret;
}

/**
 * @}
 **/
