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

#ifndef JULEA_KV_KV_H
#define JULEA_KV_KV_H

#if !defined(JULEA_KV_H) && !defined(JULEA_KV_COMPILATION)
#error "Only <julea-kv.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

/**
 * \defgroup JKV KV
 *
 * Data structures and functions for managing key-value pairs.
 *
 * @{
 **/

struct JKV;

typedef struct JKV JKV;

/**
 * A callback for j_kv_get_callback.
 *
 * The callback will receive a pointer to the data (must be freed by the caller), the data's length and a pointer to optional user-provided data.
 */
typedef void (*JKVGetFunc)(gpointer, guint32, gpointer);

/**
 * Creates a new key-value pair.
 *
 * \code
 * JKV* i;
 *
 * i = j_kv_new("JULEA", "KEY");
 * \endcode
 *
 * \param namespace    Namespace to create the key-value pair in.
 * \param key          A key-value pair key.
 *
 * \return A new key-value pair. Should be freed with j_kv_unref().
 **/
JKV* j_kv_new(gchar const* namespace, gchar const* key);

/**
 * Creates a new key-value pair on a specific KV-server.
 *
 * \code
 * JKV* i;
 *
 * i = j_kv_new_for_index(0, "JULEA", "KEY");
 * \endcode
 *
 * \param index        Index of the KV-server where the key-value pair should be stored.
 * \param namespace    Namespace to create the key-value pair in.
 * \param key          A key-value pair key.
 *
 * \return A new key-value pair. Should be freed with j_kv_unref().
 **/
JKV* j_kv_new_for_index(guint32 index, gchar const* namespace, gchar const* key);

/**
 * Increases a key-value pair's reference count.
 *
 * \code
 * JKV* i;
 *
 * j_kv_ref(i);
 * \endcode
 *
 * \param kv A key-value pair.
 *
 * \return key-value pair.
 **/
JKV* j_kv_ref(JKV* kv);

/**
 * Decreases a key-value pair's reference count.
 * When the reference count reaches zero, frees the memory allocated for the key-value pair.
 *
 * \code
 * \endcode
 *
 * \param kv A key-value pair.
 **/
void j_kv_unref(JKV* kv);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JKV, j_kv_unref)

/**
 * Creates a key-value pair.
 *
 * \code
 * \endcode
 *
 * \param kv    A KV.
 * \param value A value.
 * \param value_len Length of value buffer.
 * \param value_destroy A function to correctly free the stored data.
 * \param batch A batch.
 **/
void j_kv_put(JKV* kv, gpointer value, guint32 value_len, GDestroyNotify value_destroy, JBatch* batch);

/**
 * Deletes a key-value pair.
 *
 * \code
 * \endcode
 *
 * \param kv     A JKV.
 * \param batch  A batch.
 **/
void j_kv_delete(JKV* kv, JBatch* batch);

/**
 * Get a key-value pair.
 *
 * \code
 * \endcode
 *
 * \param kv    A KV.
 * \param value A pointer for the returned value buffer.
 * \param value_len A pointer to the length of the returned value buffer.
 * \param batch A batch.
 **/
void j_kv_get(JKV* kv, gpointer* value, guint32* value_len, JBatch* batch);

/**
 * Get a key-value pair and execute a callback function once the pair is received.
 *
 * \code
 * \endcode
 *
 * \param kv        A key-value pair.
 * \param func      The callback function. Will be called once the KV is received.
 * \param data      User defined data which will be passed to the callback function.
 * \param batch     A batch.
 **/
void j_kv_get_callback(JKV* kv, JKVGetFunc func, gpointer data, JBatch* batch);

/**
 * @}
 **/

G_END_DECLS

#endif
