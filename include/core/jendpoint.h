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

#include <jdomain_manager.h>

struct JEndpoint_;
typedef struct JEndpoint_ JEndpoint_;


gboolean j_endpoint_init_(JEndpoint_**, JFabric*, JDomainManager*, JConfiguration*, struct fi_info*, struct fi_info*, const gchar*);
void j_endpoint_fini_(JEndpoint_*, JDomainManager*, const gchar*);

gboolean j_endpoint_read_completion_queue(struct fid_cq*, int, const gchar*, const gchar*);
gboolean j_endpoint_read_event_queue(struct fid_eq*, uint32_t*, void*, size_t, int, struct fi_eq_err_entry*, const gchar*, const gchar*);

gboolean j_endpoint_shutdown_test_(JEndpoint_*, const gchar*);

// getters
gboolean j_endpoint_get_connection_status(JEndpoint_*, JConnectionType);
struct fid_ep* j_endpoint_get_endpoint(JEndpoint_*, JConnectionType);
struct fi_info* j_endpoint_get_info(JEndpoint_*, JConnectionType);
struct fid_eq* j_endpoint_get_event_queue(JEndpoint_*, JConnectionType);
struct fid_cq* j_endpoint_get_completion_queue(JEndpoint_*, JConnectionType, uint64_t);
struct fid_domain* j_endpoint_get_domain(JEndpoint_*, JConnectionType);
struct fid_eq* j_endpoint_get_domain_eq(JEndpoint_*, JConnectionType con_type);

#endif
