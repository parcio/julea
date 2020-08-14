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
struct JDomainManager;
typedef struct JDomainManager JDomainManager;

struct JRefCountedDomain;
typedef struct JRefCountedDomain JRefCountedDomain;

struct DomainCategory;
typedef struct DomainCategory DomainCategory;

/**
 * domain management functions
 */
JDomainManager* j_domain_manager_init(void);
void j_domain_manager_fini(JDomainManager*);

gboolean j_domain_request(struct fid_fabric*, struct fi_info*, JConfiguration*, JRefCountedDomain**, JDomainManager*, const gchar*);
void j_domain_unref(JRefCountedDomain*, JDomainManager*, const gchar*);

struct fid_domain* j_get_domain(JRefCountedDomain*);
struct fid_eq* j_get_domain_eq(JRefCountedDomain*);

G_GNUC_INTERNAL void domain_ref(JRefCountedDomain*);
G_GNUC_INTERNAL gboolean domain_category_search(struct fi_info*, DomainCategory**, JDomainManager*);
G_GNUC_INTERNAL void domain_category_ref(DomainCategory*);
G_GNUC_INTERNAL void domain_category_unref(DomainCategory*, JDomainManager*);
G_GNUC_INTERNAL gboolean domain_new_internal(struct fid_fabric*, struct fi_info*, JConfiguration*, DomainCategory*, JRefCountedDomain**, const gchar*);
G_GNUC_INTERNAL gboolean domain_category_new_internal(struct fid_fabric*, struct fi_info*, JConfiguration*, DomainCategory**, JRefCountedDomain**, JDomainManager*, const gchar*);
G_GNUC_INTERNAL gboolean compare_domain_infos(struct fi_info*, struct fi_info*);

#endif
