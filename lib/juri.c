/*
 * Copyright (c) 2010-2013 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <juri.h>

#include <jcollection.h>
#include <jcommon.h>
#include <jitem.h>
#include <jbatch.h>
#include <jstore.h>

#include <string.h>

/**
 * \defgroup JURI URI
 *
 * @{
 **/

/**
 * A URI.
 **/
struct JURI
{
	/**
	 * The store name.
	 **/
	gchar* store_name;

	/**
	 * The collection name.
	 **/
	gchar* collection_name;

	/**
	 * The item name.
	 **/
	gchar* item_name;

	/**
	 * The store.
	 **/
	JStore* store;

	/**
	 * The collection.
	 **/
	JCollection* collection;

	/**
	 * The item.
	 **/
	JItem* item;
};

/**
 * Parses a given URI.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 *
 * uri = g_slice_new(JURI);
 *
 * j_uri_parse(uri, "julea://foo/bar");
 * \endcode
 *
 * \param uri  A URI.
 * \param uri_ A URI string.
 **/
static
gboolean
j_uri_parse (JURI* uri, gchar const* uri_)
{
	gchar** parts = NULL;
	guint parts_len;
	guint i;

	if (!g_str_has_prefix(uri_, "julea://"))
	{
		goto error;
	}

	parts = g_strsplit(uri_ + 8, "/", 0);
	parts_len = g_strv_length(parts);

	if (parts_len > 3)
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

	for (i = 0; i < MIN(parts_len, 1); i++)
	{
		if (strchr(parts[i], '.') != NULL)
		{
			goto error;
		}
	}

	if (parts_len >= 1)
	{
		uri->store_name = g_strdup(parts[0]);
	}

	if (parts_len >= 2)
	{
		uri->collection_name = g_strdup(parts[1]);
	}

	if (parts_len >= 3)
	{
		uri->item_name = g_strdup(parts[2]);
	}

	g_strfreev(parts);

	return TRUE;

error:
	g_strfreev(parts);

	return FALSE;
}

/**
 * Returns the URI error quark.
 *
 * \author Michael Kuhn
 *
 * \return The URI error quark.
 **/
GQuark
j_uri_error_quark (void)
{
	return g_quark_from_static_string("j-uri-error-quark");
}

/**
 * Creates a new URI.
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 *
 * uri = j_uri_new("julea://foo/bar");
 * \endcode
 *
 * \param uri_ A URI string.
 *
 * \return A new URI. Should be freed with j_uri_free().
 **/
JURI*
j_uri_new (gchar const* uri_)
{
	JURI* uri;

	uri = g_slice_new(JURI);

	uri->store_name = NULL;
	uri->collection_name = NULL;
	uri->item_name = NULL;

	uri->store = NULL;
	uri->collection = NULL;
	uri->item = NULL;

	if (!j_uri_parse(uri, uri_))
	{
		g_slice_free(JURI, uri);

		return NULL;
	}

	return uri;
}

/**
 * Frees the memory allocated by a URI.
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 *
 * ...
 *
 * j_uri_free(uri);
 * \endcode
 *
 * \param uri A URI.
 **/
void
j_uri_free (JURI* uri)
{
	g_return_if_fail(uri != NULL);

	if (uri->item != NULL)
	{
		j_item_unref(uri->item);
	}

	if (uri->collection != NULL)
	{
		j_collection_unref(uri->collection);
	}

	if (uri->store != NULL)
	{
		j_store_unref(uri->store);
	}

	g_free(uri->store_name);
	g_free(uri->collection_name);
	g_free(uri->item_name);

	g_slice_free(JURI, uri);
}

/**
 * Returns the store name.
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_uri_get_store_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The store name.
 **/
gchar const*
j_uri_get_store_name (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->store_name;
}

/**
 * Returns the collection name.
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_uri_get_collection_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The collection name.
 **/
gchar const*
j_uri_get_collection_name (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->collection_name;
}

/**
 * Returns the item name.
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 *
 * ...
 *
 * g_print("%s\n", j_uri_get_item_name(uri));
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The item name.
 **/
gchar const*
j_uri_get_item_name (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->item_name;
}

/**
 * Gets the store, collection and item.
 *
 * \author Michael Kuhn
 *
 * \code
 * JURI* uri;
 * GError* error = NULL;
 *
 * ...
 *
 * j_uri_get(uri, &error);
 * \endcode
 *
 * \param uri   A URI.
 * \param error An error.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_uri_get (JURI* uri, GError** error)
{
	JBatch* operation;
	gboolean ret = TRUE;

	g_return_val_if_fail(uri != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	operation = j_operation_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (uri->store_name != NULL)
	{
		if (uri->store != NULL)
		{
			j_store_unref(uri->store);
			uri->store = NULL;
		}

		j_get_store(&(uri->store), uri->store_name, operation);
		j_operation_execute(operation);

		if (uri->store == NULL)
		{
			ret = FALSE;
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_STORE_NOT_FOUND, "Store “%s” does not exist.", uri->store_name);

			goto end;
		}
	}

	if (uri->collection_name != NULL)
	{
		if (uri->collection != NULL)
		{
			j_collection_unref(uri->collection);
			uri->collection = NULL;
		}

		j_store_get_collection(uri->store, &(uri->collection), uri->collection_name, operation);
		j_operation_execute(operation);

		if (uri->collection == NULL)
		{
			ret = FALSE;
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_COLLECTION_NOT_FOUND, "Collection “%s” does not exist.", uri->collection_name);

			goto end;
		}
	}

	if (uri->item_name != NULL)
	{
		if (uri->item != NULL)
		{
			j_item_unref(uri->item);
			uri->item = NULL;
		}

		j_collection_get_item(uri->collection, &(uri->item), uri->item_name, J_ITEM_STATUS_NONE, operation);
		j_operation_execute(operation);

		if (uri->item == NULL)
		{
			ret = FALSE;
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_ITEM_NOT_FOUND, "Item “%s” does not exist.", uri->item_name);

			goto end;
		}
	}

end:
	j_operation_unref(operation);

	return ret;
}

/**
 * Returns the store.
 *
 * \author Michael Kuhn
 *
 * \code
 * JStore* store;
 * JURI* uri;
 *
 * ...
 *
 * store = j_uri_get_store(uri);
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The store.
 **/
JStore*
j_uri_get_store (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->store;
}

/**
 * Returns the collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* collection;
 * JURI* uri;
 *
 * ...
 *
 * collection = j_uri_get_collection(uri);
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The collection.
 **/
JCollection*
j_uri_get_collection (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->collection;
}

/**
 * Returns the item.
 *
 * \author Michael Kuhn
 *
 * \code
 * JItem* item;
 * JURI* uri;
 *
 * ...
 *
 * item = j_uri_get_item(uri);
 * \endcode
 *
 * \param uri A URI.
 *
 * \return The item.
 **/
JItem*
j_uri_get_item (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->item;
}

/**
 * @}
 **/
