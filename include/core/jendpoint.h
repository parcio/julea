/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#ifndef JULEA_ENDPOINT_H
#define JULEA_ENDPOINT_H

#include <glib.h>
#include <gio/gio.h>

#include <jstatistics.h>
#include <jconfiguration.h>

#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>

#include <netinet/in.h>
#include <arpa/inet.h>

#include <jdomain_manager.h>

enum JConnectionRet
{
	J_CON_ACCEPTED,
	J_CON_MSG_REFUSED,
	J_CON_RDMA_REFUSED,
	J_CON_FAILED
};
typedef enum JConnectionRet JConnectionRet;

struct JConData;
typedef struct JConData JConData;

struct JEndpoint;
typedef struct JEndpoint JEndpoint;

gboolean j_endpoint_init(JEndpoint**, JFabric*, JDomainManager*, JConfiguration*, struct fi_info*, struct fi_info*, const gchar*);
void j_endpoint_fini(JEndpoint*, JDomainManager*, gboolean, gboolean, const gchar*);

gboolean j_endpoint_read_completion_queue(struct fid_cq*, int, const gchar*, const gchar*);
gboolean j_endpoint_read_event_queue(struct fid_eq*, uint32_t*, void*, size_t, int, struct fi_eq_err_entry*, const gchar*, const gchar*);

JConnectionRet j_endpoint_connect(JEndpoint*, struct sockaddr_in*, const gchar*);

gboolean j_endpoint_shutdown_test(JEndpoint*, const gchar*);

// getters
gboolean j_endpoint_get_connection_status(JEndpoint*, JConnectionType);
struct fid_ep* j_endpoint_get_endpoint(JEndpoint*, JConnectionType);
struct fi_info* j_endpoint_get_info(JEndpoint*, JConnectionType);
struct fid_eq* j_endpoint_get_event_queue(JEndpoint*, JConnectionType);
struct fid_cq* j_endpoint_get_completion_queue(JEndpoint*, JConnectionType, uint64_t);
struct fid_domain* j_endpoint_get_domain(JEndpoint*, JConnectionType);
struct fid_eq* j_endpoint_get_domain_eq(JEndpoint*, JConnectionType);

// setters
gboolean j_endpoint_set_connected(JEndpoint*, JConnectionType);


//uuid
JConData* j_con_data_new(void);
void j_con_data_free(JConData*);
void j_con_data_set_con_type(JConData*, JConnectionType);
JConnectionType j_con_data_get_con_type(JConData*);
gchar* j_con_data_get_uuid(JConData*);
JConData* j_con_data_retrieve (struct fi_eq_cm_entry*, ssize_t);
size_t j_con_data_get_size(void);

#endif
