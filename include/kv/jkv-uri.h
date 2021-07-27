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

#ifndef JULEA_KV_KV_URI_H
#define JULEA_KV_KV_URI_H

#if !defined(JULEA_KV_H) && !defined(JULEA_KV_COMPILATION)
#error "Only <julea-kv.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JKVURI KV URI
 *
 * @{
 **/

enum JKVURIScheme
{
	// kv://index/namespace
	J_KV_URI_SCHEME_NAMESPACE,
	// kv://index/namespace/key
	J_KV_URI_SCHEME_KV
};

typedef enum JKVURIScheme JKVURIScheme;

struct JKVURI;

typedef struct JKVURI JKVURI;

G_END_DECLS

#include <kv/jkv.h>

G_BEGIN_DECLS

/**
 * Creates a new URI.
 *
 * \code
 * JKVURI* uri;
 *
 * uri = j_kv_uri_new("julea://foo/bar");
 * \endcode
 *
 * \param uri_ A URI string.
 * \param scheme The JKVURIScheme to use.
 *
 * \return A new URI. Should be freed with j_kv_uri_free().
 **/
JKVURI* j_kv_uri_new(gchar const* uri_, JKVURIScheme scheme);

/**
 * Frees the memory allocated by a URI.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * j_kv_uri_free(uri);
 * \endcode
 *
 * \param uri A URI.
 **/
void j_kv_uri_free(JKVURI* uri);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JKVURI, j_kv_uri_free)

/**
 * Returns the index.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * g_print("%u\n", j_kv_uri_get_index(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The index.
 **/
guint32 j_kv_uri_get_index(JKVURI* uri);

/**
 * Returns the namespace.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_kv_uri_get_namespace(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The namespace.
 **/
gchar const* j_kv_uri_get_namespace(JKVURI* uri);

/**
 * Returns the name.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_kv_uri_get_item_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The name.
 **/
gchar const* j_kv_uri_get_name(JKVURI* uri);

/**
 * Returns the kv.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_kv_uri_get_object(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The kv.
 **/
JKV* j_kv_uri_get_kv(JKVURI* uri);

/**
 * @}
 **/

G_END_DECLS

#endif
