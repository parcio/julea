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

#ifndef JULEA_ITEM_URI_H
#define JULEA_ITEM_URI_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JURI
 *
 * @{
 **/

/**
 * A URI error domain.
 **/
#define J_URI_ERROR j_uri_error_quark()

/**
 * A URI error code.
 **/
typedef enum
{
	J_URI_ERROR_STORE_NOT_FOUND,
	J_URI_ERROR_COLLECTION_NOT_FOUND,
	J_URI_ERROR_ITEM_NOT_FOUND,
	J_URI_ERROR_STORE_EXISTS,
	J_URI_ERROR_COLLECTION_EXISTS,
	J_URI_ERROR_ITEM_EXISTS
} JURIError;

struct JURI;

/**
 * A URI.
 **/
typedef struct JURI JURI;

G_END_DECLS

#include <item/jcollection.h>
#include <item/jitem.h>

G_BEGIN_DECLS

/**
 * Returns the URI error quark.
 *
 * \return The URI error quark.
 **/
GQuark j_uri_error_quark(void);

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
JURI* j_uri_new(gchar const* uri_);

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
void j_uri_free(JURI* uri);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JURI, j_uri_free)

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
gchar const* j_uri_get_collection_name(JURI* uri);

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
gchar const* j_uri_get_item_name(JURI* uri);

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
gboolean j_uri_get(JURI* uri, GError** error);

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
gboolean j_uri_create(JURI* uri, gboolean with_parents, GError** error);

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
gboolean j_uri_delete(JURI* uri, GError** error);

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
JCollection* j_uri_get_collection(JURI* uri);

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
JItem* j_uri_get_item(JURI* uri);

/**
 * @}
 **/

G_END_DECLS

#endif
