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

#ifndef JULEA_ITEM_ITEM_H
#define JULEA_ITEM_ITEM_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

/**
 * \defgroup JItem Item
 *
 * Data structures and functions for managing items.
 *
 * @{
 **/

struct JItem;

typedef struct JItem JItem;

G_END_DECLS

#include <item/jcollection.h>

G_BEGIN_DECLS

/**
 * Increases an item's reference count.
 *
 * \code
 * JItem* i;
 *
 * j_item_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return \p item.
 **/
JItem* j_item_ref(JItem* item);

/**
 * Decreases an item's reference count.
 * When the reference count reaches zero, frees the memory allocated for the item.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 **/
void j_item_unref(JItem* item);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JItem, j_item_unref)

/**
 * Returns an item's name.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return The name.
 **/
gchar const* j_item_get_name(JItem* item);

/**
 * Returns an item's credentials.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A collection.
 **/
JCredentials* j_item_get_credentials(JItem* item);

/**
 * Creates an item in a collection.
 *
 * \code
 * \endcode
 *
 * \param collection   A collection.
 * \param name         A name.
 * \param distribution A distribution.
 * \param batch        A batch.
 *
 * \return A new item. Should be freed with \ref j_item_unref().
 **/
JItem* j_item_create(JCollection* collection, gchar const* name, JDistribution* distribution, JBatch* batch);

/**
 * Deletes an item from a collection.
 *
 * \code
 * \endcode
 *
 * \param item       An item.
 * \param batch      A batch.
 **/
void j_item_delete(JItem* item, JBatch* batch);

/**
 * Gets an item from a collection.
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param item       A pointer to an item.
 * \param name       A name.
 * \param batch      A batch.
 **/
void j_item_get(JCollection* collection, JItem** item, gchar const* name, JBatch* batch);

/**
 * Reads an item.
 *
 * \code
 * \endcode
 *
 * \param item       An item.
 * \param data       A buffer to hold the read data.
 * \param length     Number of bytes to read.
 * \param offset     An offset within \p item.
 * \param bytes_read Number of bytes read.
 * \param batch      A batch.
 **/
void j_item_read(JItem* item, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch);

/**
 * Writes an item.
 *
 * \note
 * j_item_write() modifies bytes_written even if j_batch_execute() is not called.
 *
 * \code
 * \endcode
 *
 * \param item          An item.
 * \param data          A buffer holding the data to write.
 * \param length        Number of bytes to write.
 * \param offset        An offset within \p item.
 * \param bytes_written Number of bytes written.
 * \param batch         A batch.
 **/
void j_item_write(JItem* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch);

/**
 * Get the status of an item.
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param batch     A batch.
 **/
void j_item_get_status(JItem* item, JBatch* batch);

/**
 * Returns an item's size.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A size.
 **/
guint64 j_item_get_size(JItem* item);

/**
 * Returns an item's modification time.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A modification time.
 **/
gint64 j_item_get_modification_time(JItem* item);

/**
 * @}
 **/

G_END_DECLS

#endif
