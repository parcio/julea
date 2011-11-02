/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <glib.h>

#include <juri.h>

#include <jcollection.h>
#include <jcommon.h>
#include <jitem.h>
#include <joperation.h>
#include <jstore.h>

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
	gchar* store_name;
	gchar* collection_name;
	gchar* item_name;

	JStore* store;
	JCollection* collection;
	JItem* item;
};

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

	if (parts_len < 1 || parts_len > 3)
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
 * \endcode
 *
 * \param uri_ A URI.
 *
 * \return A new URI. Should be freed with j_uri_free().
 **/
JURI*
j_uri_new (gchar const* uri_)
{
	JURI* uri;

	uri = g_slice_new(JURI);

	if (!j_uri_parse(uri, uri_))
	{
		g_slice_free(JURI, uri);

		return NULL;
	}

	uri->store = NULL;
	uri->collection = NULL;
	uri->item = NULL;

	return uri;
}

/**
 * Frees the memory allocated by a URI.
 *
 * \author Michael Kuhn
 *
 * \code
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

gchar const*
j_uri_get_store_name (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->store_name;
}

gchar const*
j_uri_get_collection_name (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->collection_name;
}

gchar const*
j_uri_get_item_name (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->item_name;
}

gboolean
j_uri_get (JURI* uri, GError** error)
{
	JOperation* operation;

	g_return_val_if_fail(uri != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	operation = j_operation_new();

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
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_STORE_NOT_FOUND, "Store “%s” does not exist.", uri->store_name);

			return FALSE;
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
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_COLLECTION_NOT_FOUND, "Collection “%s” does not exist.", uri->collection_name);

			return FALSE;
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
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_ITEM_NOT_FOUND, "Item “%s” does not exist.", uri->item_name);

			return FALSE;
		}
	}

	return TRUE;
}

JStore*
j_uri_get_store (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->store;
}

JCollection*
j_uri_get_collection (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->collection;
}

JItem*
j_uri_get_item (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->item;
}

/**
 * @}
 **/
