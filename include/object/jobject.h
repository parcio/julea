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

#ifndef JULEA_OBJECT_OBJECT_H
#define JULEA_OBJECT_OBJECT_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

/**
 * \defgroup JObject Object
 *
 * Data structures and functions for managing objects.
 *
 * @{
 **/

struct JObject;

typedef struct JObject JObject;

/**
 * Creates a new object.
 *
 * \code
 * JObject* i;
 *
 * i = j_object_new("JULEA", "JULEA");
 * \endcode
 *
 * \param namespace    A namespace.
 * \param name         An object name.
 *
 * \return A new object. Should be freed with j_object_unref().
 **/
JObject* j_object_new(gchar const* namespace, gchar const* name);

/**
 * Creates a new object.
 *
 * \code
 * JObject* i;
 *
 * i = j_object_new_for_index(index, "JULEA", "JULEA");
 * \endcode
 *
 * \param index        An object server index. Must be less than the return value of j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT).
 * \param namespace    A namespace.
 * \param name         An object name.
 *
 * \return A new object. Should be freed with j_object_unref().
 **/
JObject* j_object_new_for_index(guint32 index, gchar const* namespace, gchar const* name);

/**
 * Increases an object's reference count.
 *
 * \code
 * JObject* i;
 *
 * j_object_ref(i);
 * \endcode
 *
 * \param object An object.
 *
 * \return \p object.
 **/
JObject* j_object_ref(JObject* object);

/**
 * Decreases an object's reference count.
 * When the reference count reaches zero, frees the memory allocated for the object.
 *
 * \code
 * \endcode
 *
 * \param object An object.
 **/
void j_object_unref(JObject* object);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JObject, j_object_unref)

/**
 * Creates an object.
 *
 * \code
 * \endcode
 *
 * \param object       A JObject pointer to fill.
 * \param batch        A batch.
 *
 **/
void j_object_create(JObject* object, JBatch* batch);

/**
 * Deletes an object.
 *
 * \code
 * \endcode
 *
 * \param object     An object.
 * \param batch      A batch.
 **/
void j_object_delete(JObject* object, JBatch* batch);

/**
 * Reads an object.
 *
 * \code
 * \endcode
 *
 * \param object     An object.
 * \param data       A buffer to hold the read data.
 * \param length     Number of bytes to read.
 * \param offset     An offset within \p object.
 * \param bytes_read Number of bytes read.
 * \param batch      A batch.
 **/
void j_object_read(JObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch);

/**
 * Writes an object.
 *
 * \note
 * j_object_write() modifies bytes_written even if j_batch_execute() is not called.
 *
 * \code
 * \endcode
 *
 * \param object        An object.
 * \param data          A buffer holding the data to write.
 * \param length        Number of bytes to write.
 * \param offset        An offset within \p object.
 * \param bytes_written Number of bytes written.
 * \param batch         A batch.
 **/
void j_object_write(JObject* object, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch);

/**
 * Get the status of an object.
 *
 * \code
 * \endcode
 *
 * \param object            An object.
 * \param modification_time The modification time of object.
 * \param size              The size of object.
 * \param batch             A batch.
 **/
void j_object_status(JObject* object, gint64* modification_time, guint64* size, JBatch* batch);

/**
 * Sync an object.
 *
 * \code
 * \endcode
 *
 * \param object    An object.
 * \param batch     A batch.
 **/
void j_object_sync(JObject* object, JBatch* batch);

/**
 * @}
 **/

G_END_DECLS

#endif
