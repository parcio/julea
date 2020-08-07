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

#ifndef JULEA_DOMAIN_MANAGER_H
#define JULEA_DOMAIN_MANAGER_H

#include <glib.h>
#include <gio/gio.h>

#include <jstatistics.h>
#include <jconfiguration.h>

#include <rdma/fi_cm.h> //connection management
#include <rdma/fi_errno.h> //translation error numbers

// structs for domain management
struct DomainManager
{
	GPtrArray* cat_list;
	GMutex dm_mutex;
};
typedef struct DomainManager DomainManager;

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

/**
 * domain management functions
 */
G_GNUC_INTERNAL void domain_ref(RefCountedDomain*);
void domain_unref(RefCountedDomain*, DomainManager*, const gchar*);
DomainManager* domain_manager_init(void);
void domain_manager_fini(DomainManager*);
gboolean domain_request(struct fid_fabric*, struct fi_info*, JConfiguration*, RefCountedDomain**, DomainManager*, const gchar*);
G_GNUC_INTERNAL gboolean domain_category_search(struct fi_info*, DomainCategory**, DomainManager*);
G_GNUC_INTERNAL void domain_category_ref(DomainCategory*);
G_GNUC_INTERNAL void domain_category_unref(DomainCategory*, DomainManager*);
G_GNUC_INTERNAL gboolean domain_new_internal(struct fid_fabric*, struct fi_info*, JConfiguration*, DomainCategory*, RefCountedDomain**, const gchar*);
G_GNUC_INTERNAL gboolean domain_category_new_internal(struct fid_fabric*, struct fi_info*, JConfiguration*, DomainCategory**, RefCountedDomain**, DomainManager*, const gchar*);
G_GNUC_INTERNAL gboolean compare_domain_infos(struct fi_info*, struct fi_info*);

#endif
