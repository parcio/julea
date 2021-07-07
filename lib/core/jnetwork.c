#include <core/jnetwork.h>

#include <gio/gio.h>

#include <netinet/in.h>

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>

#include <core/jconfiguration.h>

/// Used to initialize common parts between different connection.
/** This will create the following libfabric resources:
 *  * the domain
 *  * a event queue
 *  * a rx and tx completion queue
 *  * the endpoint
 *
 *	Also it will bind them accordingly and enable the endpoint.
 *  \protected \memberof JConnection
 *  \retval FALSE if initialisation failed
 */
gboolean
j_connection_init(struct JConnection* instance);

/// Allocated and bind memory needed for message transfer.
/** 
 * \protected \memberof JConnection
 * \retval FALSE if allocation or binding failed
 */
gboolean
j_connection_create_memory_resources(struct JConnection* instance);

/// flag used to different between client and server fabric
enum JFabricSide {
	JF_SERVER,
	JF_CLIENT
};

struct JFabricAddr {
	void* addr;
	uint64_t addr_len;
	uint32_t addr_format;
};

struct JFabric {
	struct fi_info* info;
	struct fid_fabric* fabric;
	struct fid_eq* pep_eq;
	struct fid_pep* pep;
	struct JFabricAddr fabric_addr_network;
	struct JConfiguration* config;
	enum JFabricSide con_side;
};

struct JConnection {
	struct JFabric* fabric;
	struct fi_info* info;
	struct fid_domain* domain;
	struct fid_ep* ep;
	struct fid_eq* eq;
	struct fid_cq* cq;
	int inject_size;
	struct {
		struct fid_mr* mr;
		void* buffer;
		size_t used;
		size_t buffer_size;
		size_t rx_prefix_size;
		size_t tx_prefix_size;
	} memory;
};

struct JConnectionMemory {
	struct fid_mr* memory_region;
	void* addr;
	guint64 size;
};

#define EXE(cmd, ...) do { if (cmd == FALSE) { g_warning(__VA_ARGS__); goto end; } } while(FALSE)
#define CHECK(msg) do { if (res < 0) { g_warning("%s: "msg"\nDetails:\t%s", "??TODO??", fi_strerror(-res)); goto end; } } while(FALSE)
#define G_CHECK(msg) do { \
	if(error != NULL) {\
		g_warning(msg"\n\tWith:%s", error->message);\
		g_error_free(error);\
		goto end; } } while(FALSE)

void free_dangeling_infos(struct fi_info*);

void free_dangeling_infos(struct fi_info* info) {
	fi_freeinfo(info->next);
	info->next = NULL;
}

gboolean
j_fabric_init_server(struct JConfiguration* configuration, struct JFabric** instnace_ptr)
{
	struct JFabric* this;
	int res;

	*instnace_ptr = malloc(sizeof(*this));
	this = *instnace_ptr;
	memset(this, 0, sizeof(*this));

	this->config = configuration;
	this->con_side = JF_SERVER;

	res = fi_getinfo(
			j_configuration_get_libfabric_version(this->config),
			NULL, NULL, FI_SOURCE,
			j_configuration_get_libfabric_hints(this->config),
			&this->info);
	CHECK("Failed to find fabric for server!");
	free_dangeling_infos(this->info);

	// no context needed, because we are in sync
	res = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
	CHECK("Failed to open server fabric!");

	// wait obj defined to use synced actions
	res = fi_eq_open(this->fabric,
			&(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC},
			&this->pep_eq, NULL);
	CHECK("failed to create eq for fabric!");
	res= fi_passive_ep(this->fabric, this->info, &this->pep, NULL);
	CHECK("failed to create pep for fabric!");
	res = fi_pep_bind(this->pep, &this->pep_eq->fid, 0);
	CHECK("failed to bind event queue to pep!");
	res = fi_listen(this->pep);
	CHECK("failed to start listening on pep!");

	// initelize addr field!
	res = fi_getname(&this->pep->fid, NULL, &this->fabric_addr_network.addr_len);
	CHECK("failed to fetch address len from libfabirc!");
	this->fabric_addr_network.addr = malloc(this->fabric_addr_network.addr_len);
	res = fi_getname(&this->pep->fid, this->fabric_addr_network.addr,
			&this->fabric_addr_network.addr_len);
	CHECK("failed to fetch address from libfabric!");

	this->fabric_addr_network.addr_len = htonl(this->fabric_addr_network.addr_len);
	this->fabric_addr_network.addr_format = htonl(this->info->addr_format);

	return TRUE;
end:
	return FALSE;
}

gboolean
j_fabric_init_client(struct JConfiguration* configuration, struct JFabricAddr* addr, struct JFabric** instance_ptr)
{
	struct JFabric* this;
	struct fi_info* hints;
	int res;
	gboolean ret = FALSE;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	this->config = configuration;
	this->con_side = JF_CLIENT;

	hints = fi_dupinfo(j_configuration_get_libfabric_hints(configuration));
	hints->addr_format = addr->addr_format;
	hints->dest_addr = addr->addr;
	hints->dest_addrlen = addr->addr_len;

	res = fi_getinfo(
			j_configuration_get_libfabric_version(configuration),
			NULL, NULL, 0,
			hints, &this->info);
	CHECK("Failed to find fabric!");
	free_dangeling_infos(this->info);

	res = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
	CHECK("failed to initelize client fabric!");

	ret = TRUE;
end:
	fi_freeinfo(hints);
	return ret;
}

gboolean
j_fabric_fini(struct JFabric* this)
{
	int res;

	fi_freeinfo(this->info);
	this->info = NULL;
	if(this->con_side == JF_SERVER) {
		res = fi_close(&this->pep->fid);
		CHECK("Failed to close PEP!");
		this->pep = NULL;
		
		res =  fi_close(&this->pep_eq->fid);
		CHECK("Failed to close EQ for PEP!");
		this->pep_eq = NULL;
	}

	res = fi_close(&this->fabric->fid);
	CHECK("failed to close fabric!");
	this->fabric = NULL;
	free(this);
	return TRUE;
end:
	return FALSE;
}

///! \todo read error!
gboolean
j_fabric_sread_event(struct JFabric* this, int timeout, enum JFabricEvents* event, JFabricConnectionRequest* con_req)
{
	int res;
	gboolean ret = FALSE;
	uint32_t fi_event;

	res = fi_eq_sread(this->pep_eq, &fi_event,
			con_req, sizeof(*con_req),
			timeout, 0);
	if (res == -FI_EAGAIN) {
		*event = J_FABRIC_EVENT_TIMEOUT;
		ret = TRUE; goto end;
	} else if (res == -FI_EAVAIL) {
		*event = J_FABRIC_EVENT_ERROR;
		// TODO: fetch error!
		ret = TRUE; goto end;
	}
	CHECK("failed to read pep event queue!");
	switch(fi_event) {
		case FI_CONNREQ: *event = J_FABRIC_EVENT_CONNECTION_REQUEST; break;
		case FI_SHUTDOWN: *event = J_FABRIC_EVENT_SHUTDOWN; break;
		default: g_assert_not_reached(); goto end;
	}
	ret = TRUE;
end:
	return ret;
}

/** \todo Port is a magic number ! 4711 */
gboolean
j_connection_init_client(struct JConfiguration* configuration, enum JBackendType backend, guint index, struct JConnection** instance_ptr)
{
	struct JConnection* this;
	gboolean ret = FALSE;
	int res;
	g_autoptr(GSocketClient) g_client = NULL;
	GSocketConnection* g_connection;
	GInputStream* g_input;
	GError* error;
	const gchar* server;
	struct JFabricAddr jf_addr;
	enum JConnectionEvents event;

	*instance_ptr = malloc(sizeof(this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	g_client = g_socket_client_new();
	server = j_configuration_get_server(configuration, backend, index);
	g_connection = g_socket_client_connect_to_host(g_client,
			server,
			4711, NULL, &error);
	G_CHECK("Failed to build gsocket connection to host");
	if(g_connection == NULL) {
		g_warning("Can not connect to %s.", server);
		goto end;
	}

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

	EXE(j_fabric_init_client(configuration, &jf_addr, &this->fabric),
			"Failed to initelize fabric for client!");
	this->info = this->fabric->info;

	EXE(j_connection_init(this),
			"Failed to initelze connection!");

	res = fi_connect(this->ep, jf_addr.addr, NULL, 0);
	CHECK("Failed to fire connection request!");

	EXE(j_connection_sread_event(this, -1, &event),
			"Failed to read event queue, waiting for CONNECTED signal!");
	
	if(event != J_CONNECTION_EVENT_CONNECTED) {
		g_warning("Failed to connect to host!");
		goto end;
	}

	ret = TRUE;
end:
	/// FIXME memory leak on error in addr.addr
	return ret;
}

gboolean
j_connection_init_server(struct JFabric* fabric, GSocketConnection* gconnection, struct JConnection** instance_ptr)
{
	struct JConnection* this;
	gboolean ret = FALSE;
	int res;
	GOutputStream* g_out;
	GError* error;
	struct JFabricAddr* addr = &fabric->fabric_addr_network;
	enum JFabricEvents event;
	enum JConnectionEvents con_event;
	JFabricConnectionRequest request;

	instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));
	
	// send addr
	g_out = g_io_stream_get_output_stream(G_IO_STREAM(gconnection));
	g_output_stream_write(g_out, &addr->addr_format, sizeof(addr->addr_format), NULL, &error);
	G_CHECK("Failed to write addr_format to stream!");
	g_output_stream_write(g_out, &addr->addr_len, sizeof(addr->addr_len), NULL, &error);
	G_CHECK("Failed to write addr_len to stream!");
	g_output_stream_write(g_out, addr->addr, addr->addr_len, NULL, &error);
	G_CHECK("Failed to write addr to stream!");
	g_output_stream_close(g_out, NULL, &error);
	G_CHECK("Failed to close output stream!");

	EXE(j_fabric_sread_event(fabric, -1, &event, &request),
			"Failed to wait for connection request");
	if(event != J_FABRIC_EVENT_CONNECTION_REQUEST) {
		g_warning("expected an connection request and nothing else!");
		goto end;
	}
	this->fabric = fabric;
	this->info = request.info;

	EXE(j_connection_init(this), "Failed to initelize connection server side!");

	res = fi_accept(this->ep, NULL, 0);
	CHECK("Failed to accept connection!");
	EXE(j_connection_sread_event(this, -1, &con_event), "Failed to verify connection!");
	if(con_event != J_CONNECTION_EVENT_CONNECTED) {
		g_warning("expected and connection ack and nothing else!");
		goto end;
	}

	ret = TRUE;
end: 
	return ret;
}

gboolean
j_connection_init(struct JConnection* this)
{
	gboolean ret = FALSE; 
	int res;

	res = fi_eq_open(this->fabric->fabric, &(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC},
			&this->eq, NULL);
	CHECK("Failed to open event queue for connection!");
	res = fi_domain(this->fabric->fabric, this->info,
			&this->domain, NULL);
	CHECK("Failed to open connection domain!");
	
	this->inject_size = this->fabric->info->tx_attr->inject_size;
	EXE(j_connection_create_memory_resources(this),
			"Failed to create memory resources for connection!");

	res = fi_cq_open(this->domain, &(struct fi_cq_attr){
				.wait_obj = FI_WAIT_UNSPEC,
				.format = FI_CQ_FORMAT_CONTEXT,
				.size = this->info->tx_attr->size
			},
			&this->cq, &this->cq);
	// tx completion size differs from rx completion size!
	g_assert_true(this->info->tx_attr->size == this->info->rx_attr->size);
	CHECK("Failed to create completion queue!");
	res = fi_endpoint(this->domain, this->info, &this->ep, NULL);
	CHECK("Failed to open endpoint for connection!");
	res = fi_ep_bind(this->ep, &this->eq->fid, 0);
	CHECK("Failed to bind event queue to endpoint!");
	res = fi_ep_bind(this->ep, &this->cq->fid, FI_TRANSMIT | FI_RECV);
	CHECK("Failed to bind completion queue to endpoint!");
	res = fi_enable(this->ep);
	CHECK("Failed to enable connection!");

	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_create_memory_resources(struct JConnection* this)
{
	gboolean ret = FALSE;
	int res;
	guint64 op_size, size;
	size_t prefix_size;
	gboolean tx_prefix, rx_prefix;

	op_size = j_configuration_get_max_operation_size(this->fabric->config);
	tx_prefix = (this->info->tx_attr->mode & FI_MSG_PREFIX) != 0;
	rx_prefix = (this->info->rx_attr->mode & FI_MSG_PREFIX) != 0;
	prefix_size = this->info->ep_attr->msg_prefix_size;

	if(op_size + (tx_prefix | rx_prefix) * prefix_size > this->info->ep_attr->msg_prefix_size) {
		op_size = this->info->ep_attr->max_msg_size - (tx_prefix | rx_prefix) * prefix_size;
		g_critical("Fabric supported memory size is too smal! please configure a max operation size less equal to %lu!", op_size);
		goto end;
	}
	size = 0;

	if(this->info->domain_attr->mr_mode & FI_MR_LOCAL) {
		size +=
			  (rx_prefix * prefix_size) * J_CONNECTION_MAX_RECV * op_size
			+ (tx_prefix * prefix_size) * J_CONNECTION_MAX_SEND * op_size;
		this->memory.buffer_size = size;
		this->memory.buffer = malloc(size);
		this->memory.rx_prefix_size = rx_prefix * prefix_size;
		this->memory.tx_prefix_size = tx_prefix * prefix_size;
		this->memory.used = 0;
		res = fi_mr_reg(this->domain, this->memory.buffer, this->memory.buffer_size,
				FI_SEND | FI_RECV, 0, 0, 0, &this->memory.mr, NULL);
		CHECK("Failed to register memory for msg communication!");
	}

	ret = TRUE;
end:
	return ret;
}
