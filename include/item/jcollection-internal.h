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

/**
 * \file
 **/

#ifndef JULEA_ITEM_COLLECTION_INTERNAL_H
#define JULEA_ITEM_COLLECTION_INTERNAL_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

#include <julea.h>

#include <item/jcollection.h>

G_BEGIN_DECLS

G_GNUC_INTERNAL JCollection* j_collection_new(gchar const*);
G_GNUC_INTERNAL JCollection* j_collection_new_from_bson(bson_t const*);

G_GNUC_INTERNAL bson_t* j_collection_serialize(JCollection*);
G_GNUC_INTERNAL void j_collection_deserialize(JCollection*, bson_t const*);

G_GNUC_INTERNAL bson_oid_t const* j_collection_get_id(JCollection*);

G_GNUC_INTERNAL gboolean j_collection_get_exec(JList*, JSemantics*);

G_END_DECLS

#endif
