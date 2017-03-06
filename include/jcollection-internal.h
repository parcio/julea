/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#ifndef H_COLLECTION_INTERNAL
#define H_COLLECTION_INTERNAL

#include <glib.h>

#include <bson.h>

#include <julea-internal.h>

#include <jcollection.h>

#include <jlist.h>

J_GNUC_INTERNAL JCollection* j_collection_new (gchar const*);
J_GNUC_INTERNAL JCollection* j_collection_new_from_bson (bson_t const*);

J_GNUC_INTERNAL bson_t* j_collection_serialize (JCollection*);
J_GNUC_INTERNAL void j_collection_deserialize (JCollection*, bson_t const*);

J_GNUC_INTERNAL bson_oid_t const* j_collection_get_id (JCollection*);

J_GNUC_INTERNAL gboolean j_collection_create_internal (JBatch*, JList*);
J_GNUC_INTERNAL gboolean j_collection_delete_internal (JBatch*, JList*);
J_GNUC_INTERNAL gboolean j_collection_get_internal (JBatch*, JList*);

#endif
