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

#ifndef JULEA_LIST_ITERATOR_H
#define JULEA_LIST_ITERATOR_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JListIterator List Iterator
 *
 * @{
 **/

struct JListIterator;

typedef struct JListIterator JListIterator;

G_END_DECLS

#include <core/jlist.h>

G_BEGIN_DECLS

/**
 * Creates a new list iterator.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 *
 * \return A new list iterator.
 **/
JListIterator* j_list_iterator_new(JList* list);

/**
 * Frees the memory allocated by a list iterator.
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 **/
void j_list_iterator_free(JListIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JListIterator, j_list_iterator_free)

/**
 * Checks whether another list element is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 *
 * \return TRUE on success, FALSE if the end of the list is reached.
 **/
gboolean j_list_iterator_next(JListIterator* iterator);

/**
 * Returns the current list element.
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 *
 * \return A list element.
 **/
gpointer j_list_iterator_get(JListIterator* iterator);

/**
 * @}
 **/

G_END_DECLS

#endif
