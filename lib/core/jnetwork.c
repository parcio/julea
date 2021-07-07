#include <core/jnetwork.h>

#include <gio/gio.h>

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>

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

struct JFabric {
	struct fi_info* info;
	struct fid_fabric* fabric;
	struct fid_eq* pep_eq;
	struct fid_pep* pep;
	struct JConfiguration* config;
	enum JFabricSide con_side;
};

struct JConnection {
	struct jfabric* fabric;
	struct fi_info* info;
	struct fid_domain* domain;
	struct fid_ep* ep;
	struct fid_eq* eq;
	struct fid_cq* cq;
	int inject_size;
	struct {
		struct fid_mr* mr;
		void* buffer;
		size_t buffer_size;
		size_t rx_prefix_size;
		size_t tx_prefix_size;
	} rx_mem, tx_mem;
};

struct JConnectionMemory {
	struct fid_mr* memory_region;
	void* addr;
	guint64 size;
};

struct JFabricAddr {
	void* addr;
	uint64_t addr_len;
	uint32_t addr_format;
};

struct JFabricConnectionRequest {
	struct fi_eq_cm_entry request_entry;
};

#define EXE(cmd, ...) do { if (cmd == FALSE) { g_warning(__VA_ARGS__); goto end; } } while(FALSE)
#define CHECK(msg) do { if (res < 0) { g_warning("%s: "msg"\nDetails:\t%s", "??TODO??", fi_strerror(-res)); goto end; } } while(FALSE)
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
j_fabric_sread_event(struct JFabric* this, int timeout, enum JFabricEvents* event, struct JFabricConnectionRequest* con_req)
{
	int res;
	gboolean ret = FALSE;
	uint32_t fi_event;

	res = fi_eq_sread(this->pep_eq, &fi_event,
			&con_req->request_entry, sizeof(con_req->request_entry),
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
