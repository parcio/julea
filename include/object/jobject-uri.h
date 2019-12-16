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

#ifndef JULEA_OBJECT_OBJECT_URI_H
#define JULEA_OBJECT_OBJECT_URI_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \addtogroup JObjectURI
 *
 * @{
 **/

enum JObjectURIScheme
{
	// object://index/namespace
	J_OBJECT_URI_SCHEME_NAMESPACE,
	// object://index/namespace/object
	J_OBJECT_URI_SCHEME_OBJECT,
	// dobject://namespace
	J_OBJECT_URI_SCHEME_DISTRIBUTED_NAMESPACE,
	// dobject://namespace/object
	J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT
};

typedef enum JObjectURIScheme JObjectURIScheme;

struct JObjectURI;

typedef struct JObjectURI JObjectURI;

G_END_DECLS

#include <object/jdistributed-object.h>
#include <object/jobject.h>

G_BEGIN_DECLS

JObjectURI* j_object_uri_new(gchar const*, JObjectURIScheme);
void j_object_uri_free(JObjectURI*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JObjectURI, j_object_uri_free)

guint32 j_object_uri_get_index(JObjectURI*);
gchar const* j_object_uri_get_namespace(JObjectURI*);
gchar const* j_object_uri_get_name(JObjectURI*);

JDistributedObject* j_object_uri_get_distributed_object(JObjectURI*);
JObject* j_object_uri_get_object(JObjectURI*);

/**
 * @}
 **/

G_END_DECLS

#endif
