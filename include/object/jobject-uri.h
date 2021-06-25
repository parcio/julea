/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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
 * \defgroup JObjectURI Object URI
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

/**
 * Creates a new URI.
 *
 * \code
 * JObjectURI* uri;
 *
 * uri = j_object_uri_new("julea://foo/bar");
 * \endcode
 *
 * \param uri_ A URI string.
 * \param scheme A JObjectURIScheme.
 *
 * \return A new URI. Should be freed with j_object_uri_free().
 **/
JObjectURI* j_object_uri_new(gchar const* uri_, JObjectURIScheme scheme);

/**
 * Frees the memory allocated by a URI.
 *
 * \code
 * JObjectURI* uri;
 *
 * ...
 *
 * j_object_uri_free(uri);
 * \endcode
 *
 * \param uri A URI.
 **/
void j_object_uri_free(JObjectURI* uri);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JObjectURI, j_object_uri_free)

/**
 * Returns the index.
 *
 * \code
 * JObjectURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_object_uri_get_collection_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The index.
 **/
guint32 j_object_uri_get_index(JObjectURI* uri);

/**
 * Returns the namespace.
 *
 * \code
 * JObjectURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_object_uri_get_collection_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The namespace.
 **/
gchar const* j_object_uri_get_namespace(JObjectURI* uri);

/**
 * Returns the name.
 *
 * \code
 * JObjectURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_object_uri_get_item_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The name.
 **/
gchar const* j_object_uri_get_name(JObjectURI* uri);

/**
 * Returns the object.
 *
 * \code
 * JObjectURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_object_uri_get_object(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The object.
 **/
JDistributedObject* j_object_uri_get_distributed_object(JObjectURI* uri);

/**
 * Returns the object.
 *
 * \code
 * JObjectURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_object_uri_get_object(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The object.
 **/
JObject* j_object_uri_get_object(JObjectURI* uri);

/**
 * @}
 **/

G_END_DECLS

#endif
