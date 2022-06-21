/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
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

#ifndef JULEA_ITEM_COLLECTION_ITERATOR_H
#define JULEA_ITEM_COLLECTION_ITERATOR_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JCollectionIterator Collection Iterator
 *
 * Data structures and functions for iterating over collections.
 *
 * @{
 **/

struct JCollectionIterator;

typedef struct JCollectionIterator JCollectionIterator;

G_END_DECLS

#include <item/jcollection.h>

G_BEGIN_DECLS

/**
 * Creates a new JCollectionIterator.
 *
 * \return A new JCollectionIterator.
 **/
JCollectionIterator* j_collection_iterator_new(void);

/**
 * Frees the memory allocated by the JCollectionIterator.
 *
 * \param iterator A JCollectionIterator.
 **/
void j_collection_iterator_free(JCollectionIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JCollectionIterator, j_collection_iterator_free)

/**
 * Checks whether another collection is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return TRUE on success, FALSE if the end of the store is reached.
 **/
gboolean j_collection_iterator_next(JCollectionIterator* iterator);

/**
 * Returns the current collection.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection* j_collection_iterator_get(JCollectionIterator* iterator);

/**
 * @}
 **/

G_END_DECLS

#endif
