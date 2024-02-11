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

#ifndef JULEA_ITEM_COLLECTION_H
#define JULEA_ITEM_COLLECTION_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

struct JCollection;

typedef struct JCollection JCollection;

G_END_DECLS

#include <item/jitem.h>

G_BEGIN_DECLS

/**
 * \defgroup JCollection Collection
 *
 * Data structures and functions for managing collections.
 *
 * @{
 **/

/**
 * Increases a collection's reference count.
 *
 * \code
 * JCollection* c;
 *
 * j_collection_ref(c);
 * \endcode
 *
 * \param collection A collection.
 *
 * \return \p collection.
 **/
JCollection* j_collection_ref(JCollection* collection);

/**
 * Decreases a collection's reference count.
 * When the reference count reaches zero, frees the memory allocated for the collection.
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 **/
void j_collection_unref(JCollection* collection);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JCollection, j_collection_unref)

/**
 * Returns a collection's name.
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return A collection name.
 **/
gchar const* j_collection_get_name(JCollection* collection);

/**
 * Creates a collection.
 *
 * \code
 * \endcode
 *
 * \param name       A name for the new collection.
 * \param batch      A batch.
 **/
JCollection* j_collection_create(gchar const* name, JBatch* batch);

/**
 * Gets a collection.
 *
 * \code
 * \endcode
 *
 * \param collection A pointer to a collection.
 * \param name       A name.
 * \param batch      A batch.
 **/
void j_collection_get(JCollection** collection, gchar const* name, JBatch* batch);

/**
 * Deletes a collection.
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param batch      A batch.
 **/
void j_collection_delete(JCollection* collection, JBatch* batch);

/**
 * @}
 **/

G_END_DECLS

#endif
