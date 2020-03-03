/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#ifndef JULEA_SERVER_H
#define JULEA_SERVER_H

#include <glib.h>
#include <gio/gio.h>

#include <jbackend.h>
#include <jmemory-chunk.h>
#include <jmessage.h>
#include <jstatistics.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h> //includes cqs and
#include <rdma/fi_cm.h> //connection management
#include <rdma/fi_errno.h> //translation error numbers

// structs for domain management
struct DomainCategory
{
	struct fi_info* info;
	GSList* domain_list;
  guint ref_count;
};
typedef struct DomainCategory DomainCategory;

struct RefCountedDomain
{
	struct fid_domain* domain;
	struct fid_eq* domain_eq;
  DomainCategory* category;
	guint ref_count;
};
typedef struct RefCountedDomain RefCountedDomain;

G_GNUC_INTERNAL JStatistics* jd_statistics;
G_GNUC_INTERNAL GMutex jd_statistics_mutex[1];

G_GNUC_INTERNAL JBackend* jd_object_backend;
G_GNUC_INTERNAL JBackend* jd_kv_backend;
G_GNUC_INTERNAL JBackend* jd_db_backend;

G_GNUC_INTERNAL gboolean j_thread_libfabric_ress_init(struct fi_info*, RefCountedDomain**, JEndpoint**);
G_GNUC_INTERNAL void j_thread_libfabric_ress_shutdown (struct fi_info*, RefCountedDomain*, JEndpoint*);

G_GNUC_INTERNAL void sig_handler (int);

/**
* domain management functions
*/
G_GNUC_INTERNAL void domain_ref (RefCountedDomain*);
G_GNUC_INTERNAL void domain_unref (RefCountedDomain*);
G_GNUC_INTERNAL void domain_manager_init (void);
G_GNUC_INTERNAL void domain_manager_fini (void);
G_GNUC_INTERNAL gboolean domain_request(struct fid_fabric*, struct fi_info* , JConfiguration*, RefCountedDomain**);
G_GNUC_INTERNAL gboolean domain_category_search(struct fi_info*, DomainCategory**);
G_GNUC_INTERNAL void domain_category_ref (DomainCategory*);
G_GNUC_INTERNAL void domain_category_unref (DomainCategory*);
G_GNUC_INTERNAL gboolean domain_new_internal (struct fid_fabric*, struct fi_info*, JConfiguration*, DomainCategory*, RefCountedDomain**);
G_GNUC_INTERNAL gboolean domain_category_new_internal (struct fid_fabric*, struct fi_info*, JConfiguration*, DomainCategory**, RefCountedDomain**);
G_GNUC_INTERNAL gboolean compare_domain_infos(struct fi_info*, struct fi_info*);

G_GNUC_INTERNAL gboolean jd_handle_message (JMessage*, JEndpoint*, JMemoryChunk*, guint64, JStatistics*);

#endif
