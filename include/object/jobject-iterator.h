/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2021 Michael Kuhn
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

#ifndef JULEA_OBJECT_OBJECT_ITERATOR_H
#define JULEA_OBJECT_OBJECT_ITERATOR_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JObjectIterator Object Iterator
 *
 * Data structures and functions for iterating over objects.
 *
 * @{
 **/

struct JObjectIterator;

typedef struct JObjectIterator JObjectIterator;

/**
 * Creates a new JObjectIterator.
 *
 * \param namespace The namespace to iterate over.
 * \param prefix Prefix of names to iterate over. Set to NULL to iterate over all objects in the namespace.
 *
 * \return A new JObjectIterator.
 **/
JObjectIterator* j_object_iterator_new(gchar const* namespace, gchar const* prefix);

/**
 * Creates a new JObjectIterator on a specific object server.
 *
 * \param index Server to query.
 * \param namespace JKV namespace to iterate over.
 * \param prefix Prefix of names to iterate over. Set to NULL to iterate over all objects in the namespace.
 *
 * \return A new JObjectIterator.
 **/
JObjectIterator* j_object_iterator_new_for_index(guint32 index, gchar const* namespace, gchar const* prefix);

/**
 * Frees the memory allocated by the JObjectIterator.
 *
 * \param iterator A JObjectIterator.
 **/
void j_object_iterator_free(JObjectIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JObjectIterator, j_object_iterator_free)

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
gboolean j_object_iterator_next(JObjectIterator* iterator);

/**
 * Returns the current collection.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return A new collection. Should be freed with j_object_unref().
 **/
gchar const* j_object_iterator_get(JObjectIterator* iterator);

/**
 * @}
 **/

G_END_DECLS

#endif
