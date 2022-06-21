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

#ifndef JULEA_LIST_H
#define JULEA_LIST_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JList List
 *
 * @{
 **/

struct JList;

typedef struct JList JList;

typedef void (*JListFreeFunc)(gpointer);

/**
 * Creates a new list.
 *
 * \code
 * \endcode
 *
 * \param free_func A function to free the element data, or NULL.
 *
 * \return A new list.
 **/
JList* j_list_new(JListFreeFunc free_func);

/**
 * Increases the list's reference count.
 *
 * \param list A list.
 *
 * \return The list.
 **/
JList* j_list_ref(JList* list);

/**
 * Decreases the list's reference count.
 * When the reference count reaches zero, frees the memory allocated for the list.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 **/
void j_list_unref(JList* list);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JList, j_list_unref)

/**
 * Returns the list's length.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 *
 * \return The list's length.
 **/
guint j_list_length(JList* list);

/**
 * Appends a new list element to a list.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 * \param data A list element.
 **/
void j_list_append(JList* list, gpointer data);

/**
 * Prepends a new list element to a list.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 * \param data A list element.
 **/
void j_list_prepend(JList* list, gpointer data);

/**
 * Returns the first list element.
 *
 * \param list  A list.
 *
 * \return A list element, or NULL.
 **/
gpointer j_list_get_first(JList* list);

/**
 * Returns the last list element.
 *
 * \param list  A list.
 *
 * \return A list element, or NULL.
 **/
gpointer j_list_get_last(JList* list);

/**
 * Deletes all list elements.
 *
 * \param list A list.
 **/
void j_list_delete_all(JList* list);

/**
 * @}
 **/

G_END_DECLS

#endif
