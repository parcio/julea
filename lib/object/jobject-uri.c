/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2024 Michael Kuhn
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

#include <object/jobject-uri.h>

#include <object/jobject.h>

#include <string.h>

/**
 * \addtogroup JObjectURI
 *
 * @{
 **/

/**
 * An object URI.
 **/
struct JObjectURI
{
	JObjectURIScheme scheme;

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
	 * The distributed object.
	 **/
	JDistributedObject* distributed_object;

	/**
	 * The object.
	 **/
	JObject* object;
};

/**
 * Parses a given URI.
 *
 * \private
 *
 * \code
 * JObjectURI* uri;
 *
 * uri = g_new(JObjectURI, 1);
 *
 * j_object_uri_parse(uri, "julea://foo/bar");
 * \endcode
 *
 * \param uri  A URI.
 * \param uri_ A URI string.
 **/
static gboolean
j_object_uri_parse(JObjectURI* uri, gchar const* uri_)
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
		case J_OBJECT_URI_SCHEME_NAMESPACE:
			// object://index/namespace
			scheme_parts = 2;
			scheme_prefix = "object://";
			break;
		case J_OBJECT_URI_SCHEME_OBJECT:
			// object://index/namespace/object
			scheme_parts = 3;
			scheme_prefix = "object://";
			break;
		case J_OBJECT_URI_SCHEME_DISTRIBUTED_NAMESPACE:
			// dobject://namespace
			scheme_parts = 1;
			scheme_prefix = "dobject://";
			break;
		case J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT:
			// dobject://namespace/object
			scheme_parts = 2;
			scheme_prefix = "dobject://";
			break;
		default:
			g_warn_if_reached();
			break;
	}

	if (scheme_prefix == NULL || !g_str_has_prefix(uri_, scheme_prefix))
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
		case J_OBJECT_URI_SCHEME_NAMESPACE:
		case J_OBJECT_URI_SCHEME_OBJECT:
			for (i = 0; i < G_N_ELEMENTS(illegal); i++)
			{
				if (strpbrk(parts[i], illegal[i]) != NULL)
				{
					goto error;
				}
			}
			break;
		case J_OBJECT_URI_SCHEME_DISTRIBUTED_NAMESPACE:
		case J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT:
			for (i = 1; i < G_N_ELEMENTS(illegal); i++)
			{
				if (strpbrk(parts[i - 1], illegal[i]) != NULL)
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
		case J_OBJECT_URI_SCHEME_NAMESPACE:
		case J_OBJECT_URI_SCHEME_OBJECT:
			/// \todo check for errors
			uri->index = g_ascii_strtoull(parts[0], NULL, 10);
			uri->namespace = g_strdup(parts[1]);

			if (parts_len >= 3)
			{
				uri->name = g_strdup(parts[2]);
				/// \todo index
				uri->object = j_object_new_for_index(uri->index, uri->namespace, uri->name);
			}
			break;
		case J_OBJECT_URI_SCHEME_DISTRIBUTED_NAMESPACE:
		case J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT:
			uri->namespace = g_strdup(parts[0]);

			if (parts_len >= 2)
			{
				g_autoptr(JDistribution) distribution = NULL;

				/// \todo
				distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
				j_distribution_set(distribution, "start-index", 0);

				uri->name = g_strdup(parts[1]);
				uri->distributed_object = j_distributed_object_new(uri->namespace, uri->name, distribution);
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

JObjectURI*
j_object_uri_new(gchar const* uri_, JObjectURIScheme scheme)
{
	J_TRACE_FUNCTION(NULL);

	JObjectURI* uri;

	uri = g_new(JObjectURI, 1);

	uri->scheme = scheme;
	uri->index = 0;
	uri->namespace = NULL;
	uri->name = NULL;
	uri->distributed_object = NULL;
	uri->object = NULL;

	if (!j_object_uri_parse(uri, uri_))
	{
		g_free(uri);

		return NULL;
	}

	return uri;
}

void
j_object_uri_free(JObjectURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(uri != NULL);

	if (uri->distributed_object != NULL)
	{
		j_distributed_object_unref(uri->distributed_object);
	}

	if (uri->object != NULL)
	{
		j_object_unref(uri->object);
	}

	g_free(uri->namespace);
	g_free(uri->name);

	g_free(uri);
}

guint32
j_object_uri_get_index(JObjectURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, 0);

	return uri->index;
}

gchar const*
j_object_uri_get_namespace(JObjectURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->namespace;
}

gchar const*
j_object_uri_get_name(JObjectURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->name;
}

JDistributedObject*
j_object_uri_get_distributed_object(JObjectURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->distributed_object;
}

JObject*
j_object_uri_get_object(JObjectURI* uri)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(uri != NULL, NULL);

	return uri->object;
}

/**
 * @}
 **/
