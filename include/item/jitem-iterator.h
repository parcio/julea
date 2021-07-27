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

#ifndef JULEA_ITEM_ITEM_ITERATOR_H
#define JULEA_ITEM_ITEM_ITERATOR_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JItemIterator Item Iterator
 *
 * Data structures and functions for iterating over items.
 *
 * @{
 **/

struct JItemIterator;

typedef struct JItemIterator JItemIterator;
G_END_DECLS

#include <item/jcollection.h>
#include <item/jitem.h>

G_BEGIN_DECLS

/**
 * Creates a new JItemIterator.
 *
 * \param collection A JCollection.
 *
 * \return A new JItemIterator.
 **/
JItemIterator* j_item_iterator_new(JCollection* collection);

/**
 * Frees the memory allocated by the JItemIterator.
 *
 * \param iterator A JItemIterator.
 **/
void j_item_iterator_free(JItemIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JItemIterator, j_item_iterator_free)

/**
 * Checks whether another item is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A collection iterator.
 *
 * \return TRUE on success, FALSE if the end of the collection is reached.
 **/
gboolean j_item_iterator_next(JItemIterator* iterator);

/**
 * Returns the current item.
 *
 * \code
 * \endcode
 *
 * \param iterator A collection iterator.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem* j_item_iterator_get(JItemIterator* iterator);

/**
 * @}
 **/

G_END_DECLS

#endif
