/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2022 Michael Kuhn
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

#ifndef JULEA_KV_KV_ITERATOR_H
#define JULEA_KV_KV_ITERATOR_H

#if !defined(JULEA_KV_H) && !defined(JULEA_KV_COMPILATION)
#error "Only <julea-kv.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JKVIterator KV Iterator
 *
 * Data structures and functions for iterating over key-value pairs.
 *
 * @{
 **/

struct JKVIterator;

typedef struct JKVIterator JKVIterator;

G_END_DECLS

#include <kv/jkv.h>

G_BEGIN_DECLS

/**
 * Creates a new JKVIterator.
 *
 * \param namespace JKV namespace to iterate over.
 * \param prefix Prefix of keys to iterate over. Set to NULL to iterate over all KVs.
 *
 * \return A new JKVIterator.
 **/
JKVIterator* j_kv_iterator_new(gchar const* namespace, gchar const* prefix);

/**
 * Creates a new JKVIterator on a specific KV server.
 *
 * \param index Server to query.
 * \param namespace JKV namespace to iterate over.
 * \param prefix Prefix of keys to iterate over. Set to NULL to iterate over all KVs.
 *
 * \return A new JKVIterator.
 **/
JKVIterator* j_kv_iterator_new_for_index(guint32 index, gchar const* namespace, gchar const* prefix);

/**
 * Frees the memory allocated by the JKVIterator.
 *
 * \param iterator A JKVIterator.
 **/
void j_kv_iterator_free(JKVIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JKVIterator, j_kv_iterator_free)

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
gboolean j_kv_iterator_next(JKVIterator* iterator);

/**
 * Returns the current collection.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 * \param value A pointer to be set to the current value.
 * \param len Will be set to the length of the current value.
 *
 * \return The current key.
 **/
gchar const* j_kv_iterator_get(JKVIterator* iterator, gconstpointer* value, guint32* len);

/**
 * @}
 **/

G_END_DECLS

#endif
