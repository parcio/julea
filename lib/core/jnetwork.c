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
	uint32_t addr_len;
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
	struct {
		struct fid_cq* tx;
		struct fid_cq* rx;
	} cq;
	size_t inject_size;
	struct {
		gboolean active;
		struct fid_mr* mr;
		void* buffer;
		size_t used;
		size_t buffer_size;
		size_t rx_prefix_size;
		size_t tx_prefix_size;
	} memory;
	struct {
		struct {
			void* 	context;
			void* 	dest;
			size_t  len;
			struct fid_mr* mr;
		} entry[J_CONNECTION_MAX_RECV + J_CONNECTION_MAX_SEND];
		int len;
	} running_actions;
	guint addr_offset;
	gboolean closed;
};

struct JConnectionMemory {
	struct fid_mr* memory_region;
	guint64 addr;
	guint64 size;
};

#define EXE(cmd, ...) do { if (cmd == FALSE) { g_warning(__VA_ARGS__); goto end; } } while(FALSE)
#define CHECK(msg) do { if (res < 0) { g_warning("%s: "msg"\t(%s:%d)\nDetails:\t%s", "??TODO??", __FILE__, __LINE__,fi_strerror(-res)); goto end; } } while(FALSE)
#define G_CHECK(msg) do { \
	if(error != NULL) {\
		g_warning(msg"\n\tWith:%s", error->message);\
		g_error_free(error);\
		goto end; } } while(FALSE)

void free_dangling_infos(struct fi_info*);

void free_dangling_infos(struct fi_info* info) {
	fi_freeinfo(info->next);
	info->next = NULL;
}

gboolean
j_fabric_init_server(struct JConfiguration* configuration, struct JFabric** instance_ptr)
{
	struct JFabric* this;
	int res;
	size_t addrlen;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	this->config = configuration;
	this->con_side = JF_SERVER;

	res = fi_getinfo(
			j_configuration_get_libfabric_version(this->config),
			NULL, NULL, 0,
			j_configuration_get_libfabric_hints(this->config),
			&this->info);
	CHECK("Failed to find fabric for server!");
	free_dangling_infos(this->info);

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

	// initelize addr field!
	res = fi_getname(&this->pep->fid, NULL, &addrlen);
	if(res != -FI_ETOOSMALL) { CHECK("failed to fetch address len from libfabirc!"); }
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

	hints = j_configuration_get_libfabric_hints(configuration);
	/// \todo check if verbs works without this!
	// Causes invalid state for tcp connection
	// hints->addr_format = addr->addr_format;
	// hints->dest_addr = addr->addr;
	// hints->dest_addrlen = addr->addr_len;

	res = fi_getinfo(
			j_configuration_get_libfabric_version(configuration),
			NULL, NULL, 0,
			hints, &this->info);
	CHECK("Failed to find fabric!");
	free_dangling_infos(this->info);

	res = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
	CHECK("failed to initelize client fabric!");

	ret = TRUE;
end:
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
		g_warning("error");
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

gboolean
j_connection_init_client(struct JConfiguration* configuration, enum JBackendType backend, guint index, struct JConnection** instance_ptr)
{
	struct JConnection* this;
	gboolean ret = FALSE;
	int res;
	g_autoptr(GSocketClient) g_client = NULL;
	GSocketConnection* g_connection;
	GInputStream* g_input;
	GError* error = NULL;
	const gchar* server;
	struct JFabricAddr jf_addr;
	enum JConnectionEvents event;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));

	g_client = g_socket_client_new();
	server = j_configuration_get_server(configuration, backend, index);
	g_connection = g_socket_client_connect_to_host(g_client,
			server,
			j_configuration_get_port(configuration), NULL, &error);
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

	do {EXE(j_connection_sread_event(this, 1, &event),
			"Failed to read event queue, waiting for CONNECTED signal!");
	} while(event == J_CONNECTION_EVENT_TIMEOUT);
	
	if(event != J_CONNECTION_EVENT_CONNECTED) {
		g_warning("Failed to connect to host!");
		goto end;
	}

	ret = TRUE;
end:
	/// \todo memory leak on error in addr.addr
	free(jf_addr.addr);
	return ret;
}

gboolean
j_connection_init_server(struct JFabric* fabric, GSocketConnection* gconnection, struct JConnection** instance_ptr)
{
	struct JConnection* this;
	gboolean ret = FALSE;
	int res;
	GOutputStream* g_out;
	GError* error = NULL;
	struct JFabricAddr* addr = &fabric->fabric_addr_network;
	enum JFabricEvents event;
	enum JConnectionEvents con_event;
	JFabricConnectionRequest request;

	*instance_ptr = malloc(sizeof(*this));
	this = *instance_ptr;
	memset(this, 0, sizeof(*this));
	
	// send addr
	{ // DEBUG TODO
		char* str = malloc(ntohl(addr->addr_len) * 3 + 1);
		size_t i;
		for(i = 0; i < ntohl(addr->addr_len); ++i) { snprintf(str+i*3, 4, "%02x ", ((uint8_t*)addr->addr)[i]); }
		free(str);
	}	
	g_out = g_io_stream_get_output_stream(G_IO_STREAM(gconnection));
	g_output_stream_write(g_out, &addr->addr_format, sizeof(addr->addr_format), NULL, &error);
	G_CHECK("Failed to write addr_format to stream!");
	g_output_stream_write(g_out, &addr->addr_len, sizeof(addr->addr_len), NULL, &error);
	G_CHECK("Failed to write addr_len to stream!");
	g_output_stream_write(g_out, addr->addr, ntohl(addr->addr_len), NULL, &error);
	G_CHECK("Failed to write addr to stream!");
	g_output_stream_close(g_out, NULL, &error);
	G_CHECK("Failed to close output stream!");

	do {EXE(j_fabric_sread_event(fabric, 2, &event, &request),
			"Failed to wait for connection request");
	} while(event == J_FABRIC_EVENT_TIMEOUT);
	if(event != J_FABRIC_EVENT_CONNECTION_REQUEST) {
		g_warning("expected an connection request and nothing else! (%i)", event);
		goto end;
	}
	this->fabric = fabric;
	this->info = request.info;

	EXE(j_connection_init(this), "Failed to initelize connection server side!");

	res = fi_accept(this->ep, NULL, 0);
	CHECK("Failed to accept connection!");
	EXE(j_connection_sread_event(this, 2, &con_event), "Failed to verify connection!");
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

	this->running_actions.len = 0;

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
			&this->cq.tx, &this->cq.tx);
	res = fi_cq_open(this->domain, &(struct fi_cq_attr){
				.wait_obj = FI_WAIT_UNSPEC,
				.format = FI_CQ_FORMAT_CONTEXT,
				.size = this->info->rx_attr->size
			},
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

	if(op_size + (tx_prefix | rx_prefix) * prefix_size > this->info->ep_attr->max_msg_size) {
		guint64 max_size = this->info->ep_attr->max_msg_size - (tx_prefix | rx_prefix) * prefix_size;
		g_critical("Fabric supported memory size is too smal! please configure a max operation size less equal to %lu! instead of %lu",
				max_size, op_size + (tx_prefix | rx_prefix) * prefix_size);
		goto end;
	}
	size = 0;

	if(this->info->domain_attr->mr_mode & FI_MR_LOCAL) {
		size +=
			  (rx_prefix * prefix_size) + J_CONNECTION_MAX_RECV * op_size
			+ (tx_prefix * prefix_size) + J_CONNECTION_MAX_SEND * op_size;
		this->memory.active = TRUE;
		this->memory.used = 0;
		this->memory.buffer_size = size;
		this->memory.buffer = malloc(size);
		this->memory.rx_prefix_size = rx_prefix * prefix_size;
		this->memory.tx_prefix_size = tx_prefix * prefix_size;
		res = fi_mr_reg(this->domain, this->memory.buffer, this->memory.buffer_size,
				FI_SEND | FI_RECV, 0, 0, 0, &this->memory.mr, NULL);
		CHECK("Failed to register memory for msg communication!");
	} else { this->memory.active = FALSE; }

	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_sread_event(struct JConnection* this, int timeout, enum JConnectionEvents* event)
{
	int res;
	uint32_t fi_event;
	gboolean ret = FALSE;
	struct fi_eq_cm_entry entry;


	res = fi_eq_sread(this->eq, &fi_event, &entry, sizeof(entry), timeout, 5);
	if(res == -FI_EAGAIN) {
		*event = J_CONNECTION_EVENT_TIMEOUT;
		ret = TRUE; goto end;
	} else if (res == -FI_EAVAIL) {
		struct fi_eq_err_entry error = {0};
		*event = J_CONNECTION_EVENT_ERROR;
		do {
		res = fi_eq_readerr(this->eq, &error, 0);
		CHECK("Failed to read error!");
		g_warning("event queue contains following error (%s|c:%p):\n\t%s",
				fi_strerror(FI_EAVAIL), error.context,
				fi_eq_strerror(this->eq, error.prov_errno, error.err_data, NULL, 0));
		} while(res > 0);
		goto end; /// \todo consider return TRUE on event queue error
	}
	CHECK("Failed to read event of connection!");

	switch(fi_event) {
		case FI_CONNECTED: *event = J_CONNECTION_EVENT_CONNECTED; break;
		case FI_SHUTDOWN: *event = J_CONNECTION_EVENT_SHUTDOWN; break;
		default: g_assert_not_reached(); goto end;
	}
	ret = TRUE;
end:
	return ret;
}

/// \todo check usage and maybe merge with j_connection_read_event()
gboolean
j_connection_read_event(struct JConnection* this, enum JConnectionEvents* event)
{
	int res;
	uint32_t fi_event;
	gboolean ret = FALSE;

	res = fi_eq_read(this->eq, &fi_event, NULL, 0, 0);
	if(res == -FI_EAGAIN) {
		*event = J_CONNECTION_EVENT_TIMEOUT;
		ret = TRUE; goto end;
	} else if (res == -FI_EAVAIL) {
		*event = J_CONNECTION_EVENT_ERROR;
		// TODO: fetch error!
		ret = TRUE; goto end;
	}
	CHECK("Failed to read event of connection!");
	switch(fi_event) {
		case FI_CONNECTED: *event = J_CONNECTION_EVENT_CONNECTED; break;
		case FI_SHUTDOWN: *event = J_CONNECTION_EVENT_SHUTDOWN; break;
		default: g_assert_not_reached(); goto end;
	}
	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_send(struct JConnection* this, const void* data, size_t data_len)
{
	int res;
	gboolean ret = FALSE;
	uint8_t* segment;
	size_t size;

	// we used paired endponits -> inject and send don't need destination addr (last parameter)

	if(data_len < this->inject_size) {
		do { res = fi_inject(this->ep, data, data_len, 0); } while(res == -FI_EAGAIN);
		CHECK("Failed to inject data!");
		ret = TRUE; goto end;
	}

	// normal send
	if (this->memory.active) {
		segment = (uint8_t*)this->memory.buffer + this->memory.used;
		memcpy(segment + this->memory.tx_prefix_size,
				data, data_len);
		size = data_len + this->memory.tx_prefix_size;
		do{ 
			res = fi_send(this->ep, segment, size, fi_mr_desc(this->memory.mr), 0, segment);
		} while(res == -FI_EAGAIN);
		CHECK("Failed to initelize sending!");
		this->memory.used += size;
		g_assert_true(this->memory.used <= this->memory.buffer_size);
		g_assert_true(this->running_actions.len + 1< J_CONNECTION_MAX_SEND + J_CONNECTION_MAX_RECV);
	} else {
		segment = (uint8_t*)data;
		size = data_len;
		do{
			res = fi_send(this->ep, segment, size, NULL, 0, segment);
		} while(res == -FI_EAGAIN);
	}
	this->running_actions.entry[this->running_actions.len].context = segment;
	this->running_actions.entry[this->running_actions.len].dest = NULL;
	this->running_actions.entry[this->running_actions.len].len = 0;
	this->running_actions.entry[this->running_actions.len].mr = NULL;
	++this->running_actions.len;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_recv(struct JConnection* this, size_t data_len, void* data)
{
	gboolean ret = FALSE;
	int res;
	void* segment;
	size_t size;

	segment = this->memory.active ? (char*)this->memory.buffer + this->memory.used : data;
	size = data_len + this->memory.rx_prefix_size;
	res = fi_recv(this->ep, segment, size,
			this->memory.active ? fi_mr_desc(this->memory.mr) : NULL,
			0, segment);
	CHECK("Failed to initelized receiving!");

	if (this->memory.active) {
		this->memory.used += size;
		g_assert_true(this->memory.used <= this->memory.buffer_size);
		g_assert_true(this->running_actions.len < J_CONNECTION_MAX_SEND + J_CONNECTION_MAX_RECV);
		this->running_actions.entry[this->running_actions.len].context = segment;
		this->running_actions.entry[this->running_actions.len].dest = data;
		this->running_actions.entry[this->running_actions.len].len = data_len;
		this->running_actions.entry[this->running_actions.len].mr = NULL;
	} else {
		this->running_actions.entry[this->running_actions.len].context = segment;
		this->running_actions.entry[this->running_actions.len].dest = NULL;
		this->running_actions.entry[this->running_actions.len].len = 0;
		this->running_actions.entry[this->running_actions.len].mr = NULL;
	}
	++this->running_actions.len;
	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_closed(struct JConnection* this)
{
	return this->closed;
}

gboolean
j_connection_wait_for_completion(struct JConnection* this)
{
	gboolean ret = FALSE;
	int res;
	struct fi_cq_entry entry;
	int i;
	
	while(this->running_actions.len) {
		bool rx;
		do {
			rx = TRUE;
			res = fi_cq_sread(this->cq.rx, &entry, 1, NULL, 2); /// \todo handle shutdown msgs!
			if(res == -FI_EAGAIN) {
				rx = FALSE;
				res = fi_cq_sread(this->cq.tx, &entry, 1, NULL, 2); /// \todo handle shutdown msgs!
			}
		} while (res == -FI_EAGAIN);
		/// \todo error message fetch!
		if(res == -FI_EAVAIL) {
			enum JConnectionEvents event;
			j_connection_sread_event(this, 0, &event);
			if(event == J_CONNECTION_EVENT_SHUTDOWN) { this->closed = TRUE; goto end; }
			struct fi_cq_err_entry err_entry;
			res = fi_cq_readerr(rx ? this->cq.rx : this->cq.tx,
					&err_entry, 0);
			CHECK("Failed to read error of cq!");
			g_warning("Failed to read completion queue\nWidth:\t%s",
					fi_cq_strerror(rx ? this->cq.rx : this->cq.tx,
						err_entry.prov_errno, err_entry.err_data, NULL, 0));
			goto end;
		} else { CHECK("Failed to read completion queue!"); }
		for(i = 0; i <= this->running_actions.len; ++i) {
			if(i == this->running_actions.len) {
				g_warning("unable to find completet context!");
				goto end;
			}
			if(this->running_actions.entry[i].context == entry.op_context) {
				--this->running_actions.len;
				if(this->running_actions.entry[i].dest) {
					memcpy(this->running_actions.entry[i].dest,
							this->running_actions.entry[i].context,
							this->running_actions.entry[i].len);
				}
				if (this->running_actions.entry[i].mr) {
					res = fi_close(&this->running_actions.entry[i].mr->fid);
					CHECK("Failed to free receiving memory!");
				}
				this->running_actions.entry[i] = this->running_actions.entry[this->running_actions.len];
				break;
			}
		}
	}

	this->running_actions.len = 0;
	this->memory.used = 0;
	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_rma_register(struct JConnection* this, const void* data, size_t data_len, struct JConnectionMemory* handle)
{
	int res;
	gboolean ret = FALSE;

	res = fi_mr_reg(this->domain,
			data,
			data_len,
			FI_REMOTE_READ,
			0, this->addr_offset, 0, &handle->memory_region, NULL);
	CHECK("Failed to register memory region!");
	/// \todo tcp/verbs why?
	handle->addr = this->addr_offset; // (guint64)data;
	handle->size = data_len;
	this->addr_offset += handle->size;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_rma_unregister(struct JConnection* this, struct JConnectionMemory* handle)
{
	int res;
	this->addr_offset = 0; /// \todo ensure that this works! (breaks one incremental freeing and realloc)
	res = fi_close(&handle->memory_region->fid);
	CHECK("Failed to unregistrer rma memory!");
	return TRUE;
end:
	return FALSE;
}

gboolean
j_connection_memory_get_id(struct JConnectionMemory* this, struct JConnectionMemoryID* id)
{
	id->size = this->size;
	id->key = fi_mr_key(this->memory_region);
	id->offset = this->addr;
	return TRUE;
}

gboolean
j_connection_rma_read(struct JConnection* this, const struct JConnectionMemoryID* memoryID, void* data)
{
	int res;
	gboolean ret = FALSE;
	struct fid_mr* mr;

	res = fi_mr_reg(this->domain, data, memoryID->size,
			FI_READ, 0, 0, 0, &mr, 0);
	CHECK("Failed to register receiving memory!");

	res = fi_read(this->ep,
			data,
			memoryID->size,
			fi_mr_desc(mr),
			0,
			0, // memoryID->offset,
			memoryID->key,
			data);
	CHECK("Failed to initate reading");
	this->running_actions.entry[this->running_actions.len].context = data;
	this->running_actions.entry[this->running_actions.len].dest = NULL;
	this->running_actions.entry[this->running_actions.len].len = 0;
	this->running_actions.entry[this->running_actions.len].mr = mr;
	++this->running_actions.len;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_connection_fini ( struct JConnection* this) {
	int res;
	gboolean ret = FALSE;

	res = fi_shutdown(this->ep, 0);
	CHECK("failed to send shutdown signal");

	if(this->memory.active)	 {
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

	if(this->fabric->con_side == JF_CLIENT) {
		j_fabric_fini(this->fabric);
	}

	free(this);
	ret = TRUE;
end:
	return ret; 
}
