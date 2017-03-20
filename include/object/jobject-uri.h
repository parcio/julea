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

#ifndef H_OBJECT_OBJECT_URI
#define H_OBJECT_OBJECT_URI

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

#include <glib.h>

#include <object/jdistributed-object.h>
#include <object/jobject.h>

JObjectURI* j_object_uri_new (gchar const*, JObjectURIScheme);
void j_object_uri_free (JObjectURI*);

guint32 j_object_uri_get_index (JObjectURI*);
gchar const* j_object_uri_get_namespace (JObjectURI*);
gchar const* j_object_uri_get_name (JObjectURI*);

JDistributedObject* j_object_uri_get_distributed_object (JObjectURI*);
JObject* j_object_uri_get_object (JObjectURI*);

/**
 * @}
 **/

#endif
