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

#include <item/juri.h>

#include <item/jcollection.h>
#include <item/jitem.h>

#include <julea.h>

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
	 * The collection name.
	 **/
	gchar* collection_name;

	/**
	 * The item name.
	 **/
	gchar* item_name;

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
	g_auto(GStrv) parts = NULL;
	gchar const* illegal[2] = { "/", "/" };
	guint parts_len;
	guint i;

	if (!g_str_has_prefix(uri_, "julea://"))
	{
		goto error;
	}

	parts = g_strsplit(uri_ + 8, "/", 0);
	parts_len = g_strv_length(parts);

	if (parts_len > 2)
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

	for (i = 0; i < parts_len; i++)
	{
		if (strpbrk(parts[i], illegal[i]) != NULL)
		{
			goto error;
		}
	}

	if (parts_len >= 1)
	{
		uri->collection_name = g_strdup(parts[0]);
	}

	if (parts_len >= 2)
	{
		uri->item_name = g_strdup(parts[1]);
	}

	return TRUE;

error:
	return FALSE;
}

static
gboolean
j_uri_only_last_component_not_found (JURI* uri)
{
	gboolean ret = FALSE;

	g_return_val_if_fail(uri != NULL, FALSE);

	if (uri->collection == NULL && uri->collection_name != NULL)
	{
		ret = (uri->item_name == NULL);
	}
	else if (uri->item == NULL && uri->item_name != NULL)
	{
		ret = TRUE;
	}

	return ret;
}

/**
 * Returns the URI error quark.
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

	uri->collection_name = NULL;
	uri->item_name = NULL;

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

	g_free(uri->collection_name);
	g_free(uri->item_name);

	g_slice_free(JURI, uri);
}

/**
 * Returns the collection name.
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
 * Gets the collection and item.
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
	g_autoptr(JBatch) batch = NULL;
	gboolean ret = TRUE;

	g_return_val_if_fail(uri != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	if (uri->collection != NULL)
	{
		j_collection_unref(uri->collection);
		uri->collection = NULL;
	}

	if (uri->item != NULL)
	{
		j_item_unref(uri->item);
		uri->item = NULL;
	}

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (uri->collection_name != NULL)
	{
		j_collection_get(&(uri->collection), uri->collection_name, batch);

		if (!j_batch_execute(batch))
		{
			ret = FALSE;
			goto end;
		}

		if (uri->collection == NULL)
		{
			ret = FALSE;
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_COLLECTION_NOT_FOUND, "Collection “%s” does not exist.", uri->collection_name);

			goto end;
		}
	}

	if (uri->item_name != NULL)
	{
		j_item_get(uri->collection, &(uri->item), uri->item_name, batch);

		if (!j_batch_execute(batch))
		{
			ret = FALSE;
			goto end;
		}

		if (uri->item == NULL)
		{
			ret = FALSE;
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_ITEM_NOT_FOUND, "Item “%s” does not exist.", uri->item_name);

			goto end;
		}
	}

end:
	return ret;
}

/**
 * Creates the collection and item.
 *
 * \code
 * JURI* uri;
 * GError* error = NULL;
 *
 * ...
 *
 * j_uri_create(uri, FALSE, &error);
 * \endcode
 *
 * \param uri          A URI.
 * \param with_parents Whether to create the parent objects, too.
 * \param error        An error.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_uri_create (JURI* uri, gboolean with_parents, GError** error)
{
	g_autoptr(JBatch) batch = NULL;
	gboolean ret = TRUE;

	g_return_val_if_fail(uri != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	if (j_uri_get(uri, error))
	{
		if (uri->item != NULL)
		{
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_ITEM_EXISTS, "Item “%s” already exists.", j_item_get_name(uri->item));
		}
		else if (uri->collection != NULL)
		{
			g_set_error(error, J_URI_ERROR, J_URI_ERROR_COLLECTION_EXISTS, "Collection “%s” already exists.", j_collection_get_name(uri->collection));
		}

		ret = FALSE;
		goto end;
	}
	else
	{
		if (!with_parents && !j_uri_only_last_component_not_found(uri))
		{
			ret = FALSE;
			goto end;
		}

		g_clear_error(error);
	}

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (uri->collection == NULL && uri->collection_name != NULL)
	{
		uri->collection = j_collection_create(uri->collection_name, batch);
		ret = j_batch_execute(batch);
	}

	if (!ret)
	{
		goto end;
	}

	if (uri->item == NULL && uri->item_name != NULL)
	{
		uri->item = j_item_create(uri->collection, uri->item_name, NULL, batch);
		ret = j_batch_execute(batch);
	}

end:
	return ret;
}

/**
 * Deletes the collection and item.
 *
 * \code
 * JURI* uri;
 * GError* error = NULL;
 *
 * ...
 *
 * j_uri_delete(uri, FALSE, &error);
 * \endcode
 *
 * \param uri          A URI.
 * \param error        An error.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_uri_delete (JURI* uri, GError** error)
{
	g_autoptr(JBatch) batch = NULL;
	gboolean ret = TRUE;

	g_return_val_if_fail(uri != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	if (!j_uri_get(uri, error))
	{
		ret = FALSE;
		goto end;
	}

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (uri->item != NULL)
	{
		j_item_delete(uri->item, batch);
		ret = j_batch_execute(batch);
		/* Delete only the last component. */
		goto end;
	}

	if (uri->collection != NULL)
	{
		j_collection_delete(uri->collection, batch);
		ret = j_batch_execute(batch);
	}

end:
	return ret;
}

/**
 * Returns the collection.
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
