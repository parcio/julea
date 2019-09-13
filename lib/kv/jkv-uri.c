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

#include <julea-config.h>

#include <glib.h>

#include <kv/jkv-uri.h>

#include <kv/jkv.h>

#include <string.h>

/**
 * \defgroup JKVURI Object URI
 *
 * @{
 **/

/**
 * An object URI.
 **/
struct JKVURI
{
	JKVURIScheme scheme;

	/**
	 * The index.
	 */
	guint32 index;

	/**
	 * The namespace.
	 **/
	gchar* namespace;

	/**
	 * The name.
	 **/
	gchar* name;

	/**
	 * The kv.
	 **/
	JKV* kv;
};

/**
 * Parses a given URI.
 *
 * \private
 *
 * \code
 * JKVURI* uri;
 *
 * uri = g_slice_new(JKVURI);
 *
 * j_kv_uri_parse(uri, "julea://foo/bar");
 * \endcode
 *
 * \param uri  A URI.
 * \param uri_ A URI string.
 **/
static
gboolean
j_kv_uri_parse (JKVURI* uri, gchar const* uri_)
{
	J_TRACE_FUNCTION(NULL);

	g_auto(GStrv) parts = NULL;
	gchar const* illegal[2] = { "/", "/" };
	gchar const* scheme_prefix = NULL;
	guint parts_len;
	guint scheme_parts = 0;
	guint i;

	switch (uri->scheme)
	{
		case J_KV_URI_SCHEME_NAMESPACE:
			// kv://index/namespace
			scheme_parts = 2;
			scheme_prefix = "kv://";
			break;
		case J_KV_URI_SCHEME_KV:
			// kv://index/namespace/key
			scheme_parts = 3;
			scheme_prefix = "kv://";
			break;
		default:
			g_warn_if_reached();
			break;
	}

	if (!g_str_has_prefix(uri_, scheme_prefix))
	{
		goto error;
	}

	parts = g_strsplit(uri_ + strlen(scheme_prefix), "/", scheme_parts);
	parts_len = g_strv_length(parts);

	if (parts_len != scheme_parts)
	{
		goto error;
	}

	for (i = 0; i < parts_len; i++)
	{
		if (g_strcmp0(parts[i], "") == 0)
		{
			goto error;
		}
	}

	switch (uri->scheme)
	{
		case J_KV_URI_SCHEME_NAMESPACE:
		case J_KV_URI_SCHEME_KV:
			for (i = 0; i < G_N_ELEMENTS(illegal); i++)
			{
				if (strpbrk(parts[i], illegal[i]) != NULL)
				{
					goto error;
				}
			}
			break;
		default:
			g_warn_if_reached();
			break;
	}

	switch (uri->scheme)
	{
		case J_KV_URI_SCHEME_NAMESPACE:
		case J_KV_URI_SCHEME_KV:
			// FIXME check for errors
			uri->index = g_ascii_strtoull(parts[0], NULL, 10);
			uri->namespace = g_strdup(parts[1]);

			if (parts_len >= 3)
			{
				uri->name = g_strdup(parts[2]);
				// FIXME index
				uri->kv = j_kv_new_for_index(uri->index, uri->namespace, uri->name);
			}
			break;
		default:
			g_warn_if_reached();
			break;
	}

	return TRUE;

error:
	return FALSE;
}

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
 *
 * \return A new URI. Should be freed with j_kv_uri_free().
 **/
JKVURI*
j_kv_uri_new (gchar const* uri_, JKVURIScheme scheme)
{
	J_TRACE_FUNCTION(NULL);

	JKVURI* uri;

	uri = g_slice_new(JKVURI);

	uri->scheme = scheme;
	uri->index = 0;
	uri->namespace = NULL;
	uri->name = NULL;
	uri->kv = NULL;

	if (!j_kv_uri_parse(uri, uri_))
	{
		g_slice_free(JKVURI, uri);

		return NULL;
	}

	return uri;
}

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
void
j_kv_uri_free (JKVURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(uri != NULL);

	if (uri->kv != NULL)
	{
		j_kv_unref(uri->kv);
	}

	g_free(uri->namespace);
	g_free(uri->name);

	g_slice_free(JKVURI, uri);
}

/**
 * Returns the index.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_kv_uri_get_collection_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The index.
 **/
guint32
j_kv_uri_get_index (JKVURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, 0);

	return uri->index;
}

/**
 * Returns the namespace.
 *
 * \code
 * JKVURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_kv_uri_get_collection_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The namespace.
 **/
gchar const*
j_kv_uri_get_namespace (JKVURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->namespace;
}

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
gchar const*
j_kv_uri_get_name (JKVURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->name;
}

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
JKV*
j_kv_uri_get_kv (JKVURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->kv;
}

/**
 * @}
 **/
