#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <inttypes.h>

#include <glib.h>

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>

#include <sys/socket.h>
#include <sys/uio.h>

#include <netdb.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

// TODO: only no wait calls for cq?

#define EXE(a) do{if(!a) { goto end; }} while(FALSE)
#define CHECK(msg) do{if(res < 0) { g_error("%s: "msg"\nDetails:\t%s", location, fi_strerror(-res)); goto end; }} while(FALSE)
// TODO add to config!
#define PORT 47592
#define MSG_PARTS 2
#define MAX_SEGMENTS 5
#define ACK 42

const char* const usage = "call with: (server|client <server-ip>)";

enum ConnectionType {CLIENT, SERVER};
enum ConnectionDirection {TX, RX};
enum Event {ERROR = 0, CONNECTION_REQUEST, CONNECTED, SHUTDOWN};

struct addrinfo;

struct message { size_t len; struct { uint64_t buff_addr; size_t buff_size; uint64_t key; } *data; };

struct jfabric {
	struct fi_info* info;
	struct fid_fabric* fabric;
	struct fid_eq* pep_eq;
	struct fid_pep* pep;
	const char* location;
	enum ConnectionType con_type;
	struct Config* config;
};
struct EndpointMemory {
	struct fid_mr* mr;
	void* buffer;
	size_t buffer_size;
	size_t rx_prefix_size;
	size_t tx_prefix_size;
};

struct jendpoint {
	struct jfabric* fabric;
	struct fi_info* info;
	struct fid_domain* domain;
	struct fid_ep* ep;
	struct fid_eq* eq;
	struct EndpointMemory memory;
	struct fid_cq* txcq;
	struct fid_cq* rxcq;
	int inject_size;
};

struct MyThreadData {
	struct jfabric* fabric;
	struct fi_eq_cm_entry* con_req;
};

struct Config { struct fi_info* hints; int version; size_t max_op_size; };

struct Config* config_init(void);
void config_fini(struct Config*);
int config_get_version(struct Config*);
struct fi_info* config_get_hints(struct Config* conf);
void config_set_max_op_size(struct Config* conf, size_t size);
size_t config_get_max_op_size(struct Config* conf);

int config_get_version(struct Config* conf) { return conf->version; }
struct fi_info* config_get_hints(struct Config* conf) { return conf->hints; }
void config_set_max_op_size(struct Config* conf, size_t size) { conf->max_op_size = size; }
size_t config_get_max_op_size(struct Config* conf) { return conf->max_op_size; }
struct Config* config_init(void)
{
	struct Config* res = malloc(sizeof(struct Config));
	memset(res, 0, sizeof(struct Config));
	res->version = FI_VERSION(1, 11);
	res->hints = fi_allocinfo();
	res->hints->caps = FI_MSG | FI_SEND | FI_RECV | FI_READ | FI_RMA | FI_REMOTE_READ;
	res->hints->mode = FI_MSG_PREFIX;
	res->hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
	res->hints->ep_attr->type = FI_EP_MSG; 
	res->hints->fabric_attr->prov_name = g_strdup("verbs");
	res->max_op_size = 64;
	return res;
}
void config_fini(struct Config* config)
{
	fi_freeinfo(config->hints);
	free(config);
}


gboolean fabric_init_server(struct Config* config, struct jfabric** fabric);
gboolean fabric_init_client(struct Config* config, struct fi_info* hints, struct jfabric** fabirc);
gboolean fabric_fini(struct jfabric* fabric);
gboolean read_pep_eq(struct jfabric* fabric, enum Event* evt, struct fi_eq_cm_entry* entry);

gboolean fabric_init_client(struct Config* config, struct fi_info* hints, struct jfabric** fabric_ptr) {
	int res;
	struct jfabric* fabric;
	const char* location;

	*fabric_ptr = malloc(sizeof(struct jfabric));
	fabric = *fabric_ptr;
	memset(fabric, 0, sizeof(struct jfabric));

	fabric->config = config;
	fabric->location = "Client";
	fabric->con_type = CLIENT;

	location = fabric->location;

	res = fi_getinfo(
			config_get_version(config),
			NULL, NULL,  0,
			hints,
			&fabric->info);
	CHECK("failed to find fabric!");

	fi_freeinfo(fabric->info->next);
	fabric->info->next = NULL;

	res = fi_fabric(fabric->info->fabric_attr, &fabric->fabric, NULL);
	return TRUE;
end:
	*fabric_ptr = NULL;
	return FALSE;
}
gboolean fabric_init_server(struct Config* config, struct jfabric** fabric_ptr)
{
	int res;
	struct jfabric* fabric;
	const char* location;

	*fabric_ptr = malloc(sizeof(struct jfabric)); // TODO: alloc macro?
	fabric = *fabric_ptr;
	memset(fabric, 0, sizeof(struct jfabric));

	fabric->config = config;
	fabric->location = "Server";
	fabric->con_type = SERVER;

	location = fabric->location;

	res = fi_getinfo(
			config_get_version(config),
			NULL, NULL, FI_SOURCE, // node and service specification (we don't care?); TODO: maybe can replace socket?
			config_get_hints(config),
			&fabric->info
			);
	CHECK("Failed to get fabric info!");
	
	// throw other matches away, we only need one
	fi_freeinfo(fabric->info->next);
	fabric->info->next = NULL;

	res = fi_fabric(fabric->info->fabric_attr, &fabric->fabric, NULL); // no context needed
	CHECK("failed to open fabric!");

	res = fi_eq_open(
				fabric->fabric,
				&(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC},
				&fabric->pep_eq, NULL);
	CHECK("failed to create eq for fabric!");
	res = fi_passive_ep(fabric->fabric, fabric->info, &fabric->pep, NULL); // no context needed
	CHECK("failed to create pep");
	res = fi_pep_bind(fabric->pep, &fabric->pep_eq->fid, 0); // no special flags needed
	CHECK("failed to bind eq to pep");
	res = fi_listen(fabric->pep);
	CHECK("failed to set pep to listening!");

	return TRUE;
end:
	fabric_fini(fabric);
	*fabric_ptr = NULL;
	return FALSE;
}
gboolean fabric_fini(struct jfabric* fabric)
{
	int res;
	const char* location = fabric->location;

	g_message("close: %s", location);
	
	fi_freeinfo(fabric->info);
	fabric->info = NULL;
	if(fabric->con_type == SERVER) {
		res = fi_close(&fabric->pep->fid);
		fabric->pep = NULL;
		CHECK("failed to close pep!");
		res = fi_close(&fabric->pep_eq->fid);
		CHECK("failed to close pep_eq!");
		fabric->pep_eq = NULL;
	}
	res = fi_close(&fabric->fabric->fid);
	CHECK("failed to close fabric!");
	fabric->fabric = NULL;
	free(fabric);
	return TRUE;
end:	
	return FALSE;
}
gboolean read_pep_eq(struct jfabric* fabric, enum Event* event, struct fi_eq_cm_entry* entry)
{
	int res;
	const char* location;
	uint32_t fi_event;

	location = fabric->location;
	
	do{
		res = fi_eq_sread(fabric->pep_eq, &fi_event, entry, sizeof(struct fi_eq_cm_entry), -1, 0); // no timeout, no special flags
	} while(res == -FI_EAGAIN);
	CHECK("failed to read pep event queue!");
	
	switch(fi_event) {
		case FI_CONNREQ: *event = CONNECTION_REQUEST; break;
		case FI_CONNECTED: g_error("should not connect!"); goto end; break;
		case FI_SHUTDOWN: *event = SHUTDOWN; break;
		default: g_assert_not_reached(); goto end;
	}

	return TRUE;
end:
	*event = ERROR;
	memset(entry, 0, sizeof(struct fi_eq_cm_entry));
	return FALSE;
}

gboolean open_socket(int* fd, uint32_t port);
gboolean socket_wait_for_connection(int fd, int* active_socket);
gboolean socket_write_addr(int fd, struct jfabric* fabric);
gboolean socket_request_addr(char* addr, uint32_t port, struct fi_info* hints);
gboolean parse_address(char* addr, uint32_t port, struct addrinfo** infos);

gboolean open_socket(int* res, uint32_t port) {
	int fd;
	int error;
	struct sockaddr_in ctrl_addr = {0};
	int one = 1;

	fd = socket(AF_INET, SOCK_STREAM, 0); // orderd ipv4 socket
	if(fd == -1) { g_error("failed to open listening socket! error: %s", strerror(errno)); goto end; }
	error = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&one, sizeof(one));
	if(error == -1) { g_error("failed to set socket option! error: %s", strerror(errno)); goto end; }

	ctrl_addr.sin_family = AF_INET;
	ctrl_addr.sin_port = htons(port);
	ctrl_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	error = bind(fd, (struct sockaddr*)&ctrl_addr, sizeof(ctrl_addr));
	if(error == -1) { g_error("filed to set address for socket (binding)! error: %s", strerror(errno)); goto end; }

	*res = fd;
	return TRUE;
end:
	*res = -1;
	return FALSE;
}

gboolean socket_wait_for_connection(int fd, int* active_socket) 
{
	int error;
	int a_fd;

	g_message("wait for connection!");

	error = listen(fd, 0); // lagecy argument: length is now defined in system settings, this argument is ignored
	if(error == -1) { g_error("failed to listen for connections! error: %s", strerror(errno)); goto end; }
	g_message("listen finished");
	a_fd = accept(fd, NULL, 0); // we accept every thing what is comming!
	if(a_fd == -1) { g_error("failed to accept connection request! error: %s", strerror(errno)); goto end; }

	*active_socket = a_fd;
	return TRUE;
end:
	*active_socket = 0;
	return FALSE;
}
gboolean socket_write_addr(int fd, struct jfabric* fabric)
{
	int res;
	size_t addrlen = 0;
	const char* location;
	char* addr;
	uint32_t len;

	location = fabric->location;

	res = fi_getname(&fabric->pep->fid, NULL, &addrlen);
	if((res != -FI_ETOOSMALL) || addrlen <= 0) { CHECK("failed to fetch address size!"); }
	addr = malloc(addrlen);
	res = fi_getname(&fabric->pep->fid, addr, &addrlen);
	CHECK("failed to get addres!");

	// DEBUG:
	{ 
		char str[16*3 + 1];
		int i;
		for(i = 0; i < 16; ++i) { snprintf(str+i*3, 4, "%02x ", ((uint8_t*)addr)[i]); }
		g_message("SendAddr: len: %lu, data: %s", addrlen, str);
	}

	len = htonl(addrlen);
	res = send(fd, (char*)&len, sizeof(len), 0);
	if(res == -1 || (size_t)res != sizeof(len)) { g_error("failed to send addrlen!(%i)", res); goto end; }
	res = send(fd, &fabric->info->addr_format, sizeof(fabric->info->addr_format), 0);
	if(res == -1 || (size_t)res != sizeof(fabric->info->addr_format)) { g_error("failed to send addr format!(%i)", res); goto end; }
	res = send(fd, addr, addrlen, 0);
	if(res == -1 || (size_t)res != addrlen) { g_error("failed to send addr!(%i)", res); goto end; }

	return TRUE;
end:
	return FALSE;
}
gboolean parse_address(char* addr, uint32_t port, struct addrinfo** infos)
{
	const char* err_msg;
	char port_s[6];
	int res;

	struct addrinfo hints = {
		.ai_family = AF_INET,
		.ai_socktype = SOCK_STREAM,
		.ai_protocol = IPPROTO_TCP,
		.ai_flags = AI_NUMERICSERV
	};
	snprintf(port_s, 6, "%"PRIu16, port);
	res = getaddrinfo(addr, port_s, &hints, infos);
	if(res != 0) {
		err_msg = gai_strerror(res);
		g_error("failed to get socket addr: error: %s", err_msg);
		goto end;		
	} else if (*infos == NULL) {
		g_error("unable to find matching socket!");
		goto end;
	}
	return TRUE;
end:
	return FALSE;
}
gboolean socket_request_addr(char* s_addr, uint32_t s_port, struct fi_info* hints)
{
	int res;
	struct addrinfo* itr;
	int fd;
	uint32_t addr_len;
	uint32_t addr_format;
	void* addr = NULL;
	struct addrinfo* addresses;

	EXE(parse_address(s_addr, s_port, &addresses));

	for(itr = addresses; itr; itr = itr->ai_next) {
		fd = socket(itr->ai_family, itr->ai_socktype, itr->ai_protocol);
		if(fd == -1) { g_error("failed to open connection: error: %s", strerror(errno)); continue; }
		res = connect(fd, itr->ai_addr, itr->ai_addrlen);
		if(res == -1) {
			g_error("filed to connect to host: error: %s", strerror(errno)); 
			// close(fd);
			continue;
		}
		break;
	}
	if(!itr) { g_error("Unable to connect to any host!"); goto end; }
	freeaddrinfo(addresses);

	res = recv(fd, (void*)&addr_len, sizeof(addr_len), 0);
	if(res == -1 || res != sizeof(addr_len)) { g_error("failed to recive address len!"); goto end; }
	addr_len = ntohl(addr_len);
	res = recv(fd, (void*)&addr_format, sizeof(addr_format), 0);
	if(res == -1 || res != sizeof(addr_format)) { g_error("failed to recive address format!"); goto end; }
	addr = malloc(addr_len);
	res = recv(fd, addr, addr_len, 0);
	if(res == -1 || (uint32_t)res != addr_len) { g_error("failed to recive address!"); goto end; }

	hints->dest_addr = addr;
	hints->dest_addrlen = addr_len;
	hints->addr_format = addr_format;
	// DEBUG:
	{ 
		char str[16*3 + 1];
		int i;
		for(i = 0; i < 16; ++i) { snprintf(str+i*3, 4, "%02x ", ((uint8_t*)addr)[i]); }
		g_message("RecvAddr: len: %i, data: %s", addr_len, str);
	}
	return TRUE;
end:
	return FALSE;
}


gboolean endpoint_init_client(struct jendpoint** endpoint, struct Config* config, char* addr, uint32_t port);
gboolean endpoint_init_server(struct jendpoint** endpoint, struct jfabric*, struct fi_eq_cm_entry* con_req);
gboolean endpoint_fini(struct jendpoint* ednpoint);
// alloc memory, bind and create fi_ep
gboolean endpoint_create(struct jendpoint* endpoint);
gboolean _endpoint_create_mr(struct jendpoint* endpoint);
gboolean endpoint_check_connection(struct jendpoint* endpoint);
gboolean endpoint_read_eq(struct jendpoint* endpoint, enum Event* evt, struct fi_eq_cm_entry* entry);
gboolean endpoint_wait_for_completion(struct jendpoint* endpoint, enum ConnectionDirection con_dir, int len, gboolean* check, void** contexts);

gboolean endpoint_fini(struct jendpoint* endpoint)
{
	int res;
	const char* location = endpoint->fabric->location;

	res = fi_close(&endpoint->ep->fid);
	CHECK("failed to close ep!");
	res = fi_close(&endpoint->txcq->fid);
	CHECK("failed to close txcq!");
	res = fi_close(&endpoint->rxcq->fid);
	CHECK("failed to close rxqc!");
	res = fi_close(&endpoint->eq->fid);
	CHECK("failed to close eq!");
	if(endpoint->memory.mr) {
		res = fi_close(&endpoint->memory.mr->fid);
		CHECK("failed to close memory of endpoint!");
		free(endpoint->memory.buffer);
	}
	res = fi_close(&endpoint->domain->fid);
	CHECK("failed to close domain!");
	if(endpoint->fabric->con_type == CLIENT) {
		EXE(fabric_fini(endpoint->fabric));
	} else {
		fi_freeinfo(endpoint->info);
	}
	free(endpoint);
	return TRUE;
end:
	return FALSE;
}
gboolean _endpoint_create_mr(struct jendpoint* endpoint)
{

	int res;
	gboolean tx_prefix;
	gboolean rx_prefix;
	const char* location;
	size_t prefix_size;
	size_t size;

	location = endpoint->fabric->location;
	size = config_get_max_op_size(endpoint->fabric->config);
	tx_prefix = (endpoint->fabric->info->tx_attr->mode & FI_MSG_PREFIX) != 0;
	rx_prefix = (endpoint->fabric->info->rx_attr->mode & FI_MSG_PREFIX) != 0;
	prefix_size = endpoint->fabric->info->ep_attr->msg_prefix_size;
	g_message("%s: prefix_size: %lu", location, prefix_size);

	if(size + (tx_prefix | rx_prefix) * prefix_size > endpoint->fabric->info->ep_attr->max_msg_size) {
		// TODO: message size != operation size!
		size = endpoint->fabric->info->ep_attr->max_msg_size - (tx_prefix | rx_prefix) * prefix_size;
		config_set_max_op_size(endpoint->fabric->config, size);
	}

	if(endpoint->fabric->info->domain_attr->mr_mode & FI_MR_LOCAL) {
		g_message("%s: local memory!", location);
		size += (rx_prefix | tx_prefix) * prefix_size * MSG_PARTS;
		endpoint->memory.buffer_size = size;
		endpoint->memory.buffer = malloc(size);
		endpoint->memory.tx_prefix_size = tx_prefix * prefix_size;
		endpoint->memory.rx_prefix_size = rx_prefix * prefix_size;
		res = fi_mr_reg(endpoint->domain, endpoint->memory.buffer, endpoint->memory.buffer_size, FI_SEND | FI_RECV, 0, 0, 0, &endpoint->memory.mr, NULL);
		CHECK("failed to register memory for msg communication");
	}
	return TRUE;
end:
	if(endpoint->memory.buffer) { free(endpoint->memory.buffer); }
	return FALSE;
}

gboolean endpoint_create(struct jendpoint* endpoint)
{
	int res;
	const char* location;

	location = endpoint->fabric->location;

	res = fi_eq_open(
			endpoint->fabric->fabric,
			&(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC},
			&endpoint->eq, NULL);
	CHECK("failed to create eq!");
	res = fi_domain(endpoint->fabric->fabric, endpoint->info, &endpoint->domain, NULL);
	CHECK("failed to create domain!");

	endpoint->inject_size = endpoint->fabric->info->tx_attr->inject_size;

	EXE(_endpoint_create_mr(endpoint));

	res = fi_cq_open(endpoint->domain, &(struct fi_cq_attr){
				.wait_obj = FI_WAIT_UNSPEC,
				.format = FI_CQ_FORMAT_CONTEXT,
				.size = endpoint->info->tx_attr->size
			},
			&endpoint->txcq, &endpoint->rxcq);
	CHECK("failed to create txcq!");
	res = fi_cq_open(endpoint->domain, &(struct fi_cq_attr){
				.wait_obj = FI_WAIT_UNSPEC,
				.format = FI_CQ_FORMAT_CONTEXT,
				.size = endpoint->info->rx_attr->size
			},
			&endpoint->rxcq, &endpoint->rxcq);
	CHECK("failed to create rxcq!");
	res = fi_endpoint(endpoint->domain, endpoint->info, &endpoint->ep, NULL);
	CHECK("failed to create endpoint!");

	res = fi_ep_bind(endpoint->ep, &endpoint->eq->fid, 0);
	CHECK("failed to bind eq to ep");
	res = fi_ep_bind(endpoint->ep, &endpoint->txcq->fid, FI_TRANSMIT);
	CHECK("failed to bind txcq to ep");
	res = fi_ep_bind(endpoint->ep, &endpoint->rxcq->fid, FI_RECV);
	CHECK("failed to bind rxcq to ep");
	res = fi_enable(endpoint->ep);
	CHECK("failed to enable ep");

	return TRUE;
end:
	return FALSE;
}
gboolean endpoint_init_client(struct jendpoint** endpoint_ptr, struct Config* config, char* addr, uint32_t port)
{
	struct fi_info* hints;
	struct jendpoint* endpoint = NULL;
	int res;
	const char* location;
	enum Event event;
	struct fi_eq_cm_entry cm_entry;

	hints = fi_dupinfo(config_get_hints(config));
	EXE(socket_request_addr(addr, port, hints));			

	*endpoint_ptr = malloc(sizeof(struct jendpoint));
	endpoint = *endpoint_ptr;
	memset(endpoint, 0, sizeof(struct jendpoint));
	EXE(fabric_init_client(config, hints, &endpoint->fabric));

	location = endpoint->fabric->location;
	endpoint->info = endpoint->fabric->info;

	EXE(endpoint_create(endpoint));

	res = fi_connect(endpoint->ep, hints->dest_addr, NULL, 0); // TODO: con data id exchange?
	CHECK("failed to send connection request!");

	EXE(endpoint_read_eq(endpoint, &event, &cm_entry));
	if(event != CONNECTED || cm_entry.fid != &endpoint->ep->fid) {
		g_error("failed to connect!"); goto end;
	}
	
	fi_freeinfo(hints);
	return TRUE;
end:
	fi_freeinfo(hints);
	if(endpoint) {
		free(endpoint);
		*endpoint_ptr = NULL;
	}
	return FALSE;
}
gboolean endpoint_init_server(struct jendpoint** endpoint_ptr, struct jfabric* fabric, struct fi_eq_cm_entry* con_req)
{
	int res;
	const char* location;
	struct jendpoint* endpoint;
	enum Event event;
	struct fi_eq_cm_entry con_ack;

	location = fabric->location;
	*endpoint_ptr = malloc(sizeof(struct jendpoint));
	endpoint = *endpoint_ptr;
	memset(endpoint, 0, sizeof(struct jendpoint));
	
	endpoint->fabric = fabric;
	endpoint->info = con_req->info;

	EXE(endpoint_create(endpoint));
	
	res = fi_accept(endpoint->ep, NULL, 0); // TODO: con data exchange
	CHECK("failed to accept connection");
	EXE(endpoint_read_eq(endpoint, &event, &con_ack));
	if(event != CONNECTED || con_ack.fid != &endpoint->ep->fid) {
		g_error("failed to ack connection!"); goto end;
	}

	return TRUE;
end:
	free(endpoint);
	*endpoint_ptr = NULL;
	res = fi_reject(fabric->pep, con_req->fid, NULL, 0);
	if(res < 0) { g_error("%s: failed to reject connection:\n\tDetails: %s", location, fi_strerror(-res)); }
	return FALSE;
}
gboolean endpoint_read_eq(struct jendpoint* endpoint, enum Event* evt, struct fi_eq_cm_entry* entry)
{
	int res;
	const char* location;
	uint32_t fi_event;

	location = endpoint->fabric->location;

	do{
		res = fi_eq_sread(endpoint->eq, &fi_event, entry, sizeof(struct fi_eq_cm_entry), -1, 0);
	}while(res == -FI_EAGAIN);
	CHECK("failed to read endpoint eq!");

	switch(fi_event) {
		case FI_CONNREQ: g_error("endpoint can't  get connection request!"); goto end;
		case FI_CONNECTED: *evt = CONNECTED; break;
		case FI_SHUTDOWN: *evt = SHUTDOWN; break;
		default: g_assert_not_reached(); goto end;
	}

	return TRUE;
end:
	*evt = ERROR;
	memset(entry, 0, sizeof(struct fi_eq_cm_entry));
	return FALSE;
}

gboolean endpoint_check_connection(struct jendpoint* endpoint)
{
	return endpoint->ep != NULL;
}

gboolean endpoint_wait_for_completion(struct jendpoint* endpoint, enum ConnectionDirection con_dir, int len, gboolean* check, void** contexts)
{
	int i, count = 0;
	struct fid_cq* cq;
	struct fi_cq_err_entry err_entry;
	struct fi_cq_entry entry;
	int res;
	const char* location = endpoint->fabric->location;

	switch(con_dir) {
		case TX: cq = endpoint->txcq; break;
		case RX: cq = endpoint->rxcq; break;
		default: g_assert_not_reached(); goto end;
	}

	for(i = 0; i < len; ++i) {
		if(contexts[i] == NULL) { check[i] = TRUE; }
		if(check[i] == TRUE) {++count; }
	}
	while(count != len) {
		res = fi_cq_sread(cq, &entry, 1, NULL, -1);
		if(res == -FI_EAGAIN) { continue; }
		if(res < 0) {
			int err = res;
			res = fi_cq_readerr(cq, &err_entry, 0);
			CHECK("failed to fetch cq error!");
			g_error("%s: completion error!\n\tDetails:%s\n\tProvider:%s",
					location,
					fi_strerror(-err),
					fi_cq_strerror(cq, err_entry.prov_errno, err_entry.err_data, NULL, 0));
			goto end;
		}
		for(i = 0; i < len; ++i) {
			if(!check[i]) {
				if(contexts[i] == entry.op_context) {
					++count;
					check[i] = TRUE;
				}
			}
		}
	}
	return TRUE;
end:
	return FALSE;
}

gboolean ssend_message(struct jendpoint* endpoint, struct message* msg);
gboolean srecv_message(struct jendpoint* endpoint, struct message** msg);

gboolean ssend_message(struct jendpoint* endpoint, struct message* msg)
{
	int res;
	const char* location = endpoint->fabric->location;
	gboolean checks[2] = {FALSE, FALSE};
	void* contexts[2] = {NULL, NULL};
	int size = 0;
	int offset = 0;
	int i;
	struct fid_mr* mrs[MAX_SEGMENTS];
	int ack = ~ACK;

	for(i = 0; (size_t)i < msg->len; ++i) {
		res = fi_mr_reg(
				endpoint->domain,
				(void*)msg->data[i].buff_addr,
				msg->data[i].buff_size, FI_REMOTE_READ,
				0, 0, 0, &mrs[i], NULL);
		CHECK("failed to register memory for rma send!");
		msg->data[i].key = fi_mr_key(mrs[i]);
	}

	// don't need dst_addr because we are connected!
	size = sizeof(msg->len);
	if(size < endpoint->inject_size) {
		res = fi_inject(endpoint->ep, &msg->len, size, 0);
		CHECK("failed to inject msg header!");
	} else {
		memcpy(
				(char*)endpoint->memory.buffer + offset + endpoint->memory.tx_prefix_size,
				&msg->len, size);
		res = fi_send(endpoint->ep,
				(char*)endpoint->memory.buffer + offset,
				size + endpoint->memory.tx_prefix_size,
				fi_mr_desc(endpoint->memory.mr), 0, &msg->len);
		CHECK("failed to initeiate header send!");
		contexts[0] = &msg->len;
		offset += size + endpoint->memory.tx_prefix_size;
	}

	size = msg->len * sizeof(*msg->data);
	if(size < endpoint->inject_size) {
		res = fi_inject(endpoint->ep, msg->data, sizeof(*msg->data) * msg->len, 0);
		CHECK("failed to inject msg body!");
	} else {
		memcpy(
				(char*)endpoint->memory.buffer + offset + endpoint->memory.tx_prefix_size,
				msg->data, size);
		res = fi_send(endpoint->ep,
				(char*)endpoint->memory.buffer + offset,
				size + endpoint->memory.tx_prefix_size,
				fi_mr_desc(endpoint->memory.mr), 0, msg->data);
		CHECK("failed to initeiate body send!");
		contexts[1] = msg->data;
		offset += size + endpoint->memory.tx_prefix_size;
	}

	
	res = fi_recv(endpoint->ep, endpoint->memory.buffer, sizeof(int) + endpoint->memory.rx_prefix_size,
			fi_mr_desc(endpoint->memory.mr), 0, &ack);
	CHECK("failed to read ack flag!");
	checks[0] = FALSE; contexts[0] = &ack;
	EXE(endpoint_wait_for_completion(endpoint, RX, 1, checks, contexts));
	ack = *(int*)((char*)endpoint->memory.buffer + endpoint->memory.rx_prefix_size);
	if(ack != ACK) { g_error("recived wrong ack!: %i, expected: %i", ack, ACK); goto end; }

	for(i = 0; (size_t)i< msg->len; ++i) {
		res = fi_close(&mrs[i]->fid);
		CHECK("failed to free memory after rma send!");
	}

	EXE(endpoint_wait_for_completion(endpoint, TX, 2, checks, contexts));
	
	return TRUE;
end:
	return FALSE;
}

gboolean srecv_message(struct jendpoint* endpoint, struct message** msg_ptr)
{
	int res;
	const char* location = endpoint->fabric->location;
	gboolean checks[MAX_SEGMENTS] = {FALSE};
	void* contexts[MAX_SEGMENTS];
	int i;
	int ack = ACK;
	struct fid_mr* mrs[MAX_SEGMENTS];
	struct message* msg = malloc(sizeof(struct message));
	
	res = fi_recv(endpoint->ep, endpoint->memory.buffer, sizeof(msg->len) + endpoint->memory.rx_prefix_size, fi_mr_desc(endpoint->memory.mr), 0, &msg->len);
	CHECK("failed to recive msg len!");
	checks[0] = FALSE; contexts[0] = &msg->len;
	EXE(endpoint_wait_for_completion(endpoint, RX, 1, checks, contexts));
	memcpy(&msg->len, (char*)endpoint->memory.buffer + endpoint->memory.rx_prefix_size, sizeof(msg->len));
	
	res = fi_recv(endpoint->ep, endpoint->memory.buffer, sizeof(*msg->data)*msg->len + endpoint->memory.rx_prefix_size, fi_mr_desc(endpoint->memory.mr), 0, msg->data);
	CHECK("failed to recive body!");
	checks[0] = FALSE; contexts[0] = msg->data;
	msg->data = malloc(msg->len * sizeof(*msg->data));
	EXE(endpoint_wait_for_completion(endpoint, RX, 1, checks, contexts));
	memcpy(msg->data, (char*)endpoint->memory.buffer + endpoint->memory.rx_prefix_size, msg->len * sizeof(*msg->data));
	
	for( i = 0; (size_t)i < msg->len; ++i) {
		uint64_t addr = msg->data[i].buff_addr;
		contexts[i] = (void*) addr;
		checks[i] = FALSE;

		msg->data[i].buff_addr = (uint64_t)malloc(msg->data[i].buff_size);
		memset((void*)msg->data[i].buff_addr, 0, msg->data[i].buff_size);
		res = fi_mr_reg(endpoint->domain,
				(void*)msg->data[i].buff_addr,
				msg->data[i].buff_size, FI_READ,
				0, 0, 0, &mrs[i], 0);
		CHECK("failed to register recive memory for rma!");

		res = fi_read(endpoint->ep,
				(void*)msg->data[i].buff_addr,
				msg->data[i].buff_size,
				fi_mr_desc(mrs[i]), 0, addr, msg->data[i].key, contexts[i]);
		CHECK("failed to initeate rma read!");
	}

	EXE(endpoint_wait_for_completion(endpoint, TX, msg->len, checks, contexts));
	fi_inject(endpoint->ep, &ack, sizeof(ack), 0);

	for(i = 0; (size_t)i < msg->len; ++i) {
		res = fi_close(&mrs[i]->fid);
		CHECK("failed to unregister memory!");
	}
	
	*msg_ptr = msg;
	return TRUE;
end:
	*msg_ptr = NULL;
	if(msg->data) { free(msg->data); }
	free(msg);
	return FALSE;
}

void server(void);
void client(char* addr, uint32_t port);
struct message* construct_message(void);
void free_message(struct message* msg, gboolean sender);
void print_message(struct message* msg);
gboolean start_new_thread(struct jfabric* fabric, struct fi_eq_cm_entry* con_req);
void* thread(void* thread_data);

const char* const buffers[] = {"Hello", "World!"};
struct message* construct_message(void) {
	struct message* msg = malloc(sizeof(struct message));

	msg->len = 2;
	msg->data = malloc(sizeof(*msg->data) * msg->len);
	msg->data[0].buff_size = 6; msg->data[0].buff_addr = (uint64_t)g_strdup(buffers[0]);
	msg->data[1].buff_size = 7; msg->data[1].buff_addr = (uint64_t)g_strdup(buffers[1]);
	return msg;
}
void free_message(struct message* msg, gboolean sender) {
	size_t i;
	if(msg == NULL) { return; }
	if(msg->data != NULL) {
		for(i=0; i < msg->len; ++i) { free((void*)msg->data[i].buff_addr); }
		free(msg->data);
	}
	free(msg);
}
void print_message(struct message* msg) {
	int i;

	g_print("message:\n\tlen: %lu\n\tdata:\n", msg->len);
	for(i = 0; (size_t)i < msg->len; ++i) {
		g_print("\t\tseg %i[l=%lu]: >%s<\n", i, msg->data[i].buff_size, (char*)msg->data[i].buff_addr);
		// g_print("\t\tseg %i[l=%lu]: addr=%lu\n", i, msg->data[i].buff_size, msg->data[i].buff_addr);
	}
}

void* thread(void* thread_data) {
	struct MyThreadData* data;
	struct jendpoint* endpoint;	
	struct message* msg;
	// enum Event event;

	data = thread_data;

	EXE(endpoint_init_server(&endpoint, data->fabric, data->con_req));
	while(endpoint_check_connection(endpoint))
	{
		EXE(srecv_message(endpoint, &msg));
		print_message(msg);
		free_message(msg, FALSE);
		msg = construct_message();
		g_print("SEND:");
		print_message(msg);
		EXE(ssend_message(endpoint, msg));
		free_message(msg, TRUE);
		/*EXE(endpoint_read_eq(endpoint, &event)); // TODO: check for shutdown
		switch(event) {
			case ERROR: goto end;
			case CONNECTED: run = TRUE; break;
			case SHUTDOWN: run = FALSE; break;
			case CONNECTION_REQUEST: g_error("ep can't recive an connection request?!"); goto end;
		}*/
		sleep(10);
	}
end:
	g_thread_exit(NULL);
	return NULL;
}

gboolean start_new_thread(struct jfabric* fabric, struct fi_eq_cm_entry* con_req) {
	struct MyThreadData* thread_data = malloc(sizeof(struct MyThreadData));
	thread_data->fabric = fabric;
	thread_data->con_req = con_req;
	g_thread_new(NULL, thread, (gpointer)thread_data);
	return TRUE;
}


void server(void) {
	struct jfabric* fabric;
	gboolean run;
	enum Event event;
	struct fi_eq_cm_entry con_req;
	int p_socket;
	int socket;
	struct Config* config;

	config = config_init();

	EXE(fabric_init_server(config, &fabric));
	EXE(open_socket(&p_socket, PORT));
	run = TRUE;
	do{
		EXE(socket_wait_for_connection(p_socket, &socket));
		g_message("write_addr");
		EXE(socket_write_addr(socket, fabric));
		close(socket);
		EXE(read_pep_eq(fabric, &event, &con_req));
		switch(event) {
			case ERROR: goto end;
			case CONNECTED: g_error("pep can't connect!?"); goto end;
			case CONNECTION_REQUEST: 
							EXE(start_new_thread(fabric, &con_req));
							break;
			case SHUTDOWN: run = FALSE; break;
			default: g_assert_not_reached(); goto end;
		}
	} while(run);
end:
	fabric_fini(fabric);
	config_fini(config);
}

void client(char* addr, uint32_t port) {
	struct jendpoint* endpoint;
	struct message* msg = NULL;
	struct Config* config;

	config = config_init();

	EXE(endpoint_init_client(&endpoint, config, addr, port));

	msg = construct_message();
	((char*)msg->data[0].buff_addr)[3] = 'w';
	((char*)msg->data[1].buff_addr)[3] = 'w';
	g_print("SEND");
	print_message(msg);
	EXE(ssend_message(endpoint, msg));
	free_message(msg, TRUE);
	EXE(srecv_message(endpoint, &msg));
	print_message(msg);
end:
	free_message(msg, FALSE);
	config_fini(config);
	endpoint_fini(endpoint);
	return;
}

int main(int argc, char** argv) {

	if(argc < 2) {
		fprintf(stderr, usage);
		return EXIT_FAILURE;
	}
	if(strcmp(argv[1], "server") == 0) {
		server();
	} else if (strcmp(argv[1], "client") == 0) {
		if(argc < 3) {
			fprintf(stderr, "client can only connect when ip address is given!");
			return EXIT_FAILURE;
		}
		client(argv[2], PORT);
	} else {
		fprintf(stderr, usage);
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}
