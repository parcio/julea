/*
 * Copyright (c) 2010-2011 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <glib.h>

#include <string.h>

#include <bson.h>
#include <mongo.h>

#include "jitem.h"
#include "jitem-internal.h"

#include "jcommon.h"
#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection-internal.h"
#include "jdistribution.h"
#include "jitem-status.h"
#include "jlist.h"
#include "jlist-iterator.h"
#include "jmessage.h"
#include "joperation.h"
#include "joperation-internal.h"
#include "jsemantics.h"
#include "jtrace.h"

/**
 * \defgroup JItem Item
 *
 * Data structures and functions for managing items.
 *
 * @{
 **/

/**
 * A JItem.
 **/
struct JItem
{
	/**
	 * The ID.
	 **/
	bson_oid_t id;

	/**
	 * The name.
	 **/
	gchar* name;

	/**
	 * The status.
	 **/
	JItemStatus* status;

	/**
	 * The parent collection.
	 **/
	JCollection* collection;

	/**
	 * The semantics.
	 **/
	JSemantics* semantics;

	/**
	 * The reference count.
	 **/
	guint ref_count;
};

/**
 * Creates a new item.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 * JItem* i;
 *
 * i = j_item_new(c, "JULEA");
 * \endcode
 *
 * \param collection A collection.
 * \param name       An item name.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_new (JCollection* collection, const gchar* name)
{
	JItem* item;

	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	item = g_slice_new(JItem);
	bson_oid_gen(&(item->id));
	item->name = g_strdup(name);
	item->status = NULL;
	item->collection = j_collection_ref(collection);
	item->semantics = NULL;
	item->ref_count = 1;

	j_trace_leave(j_trace(), G_STRFUNC);

	return item;
}

/**
 * Increases an item's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JItem* i;
 *
 * j_item_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JItem*
j_item_ref (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	item->ref_count++;

	j_trace_leave(j_trace(), G_STRFUNC);

	return item;
}

/**
 * Decreases an item's reference count.
 * When the reference count reaches zero, frees the memory allocated for the item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 **/
void
j_item_unref (JItem* item)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	item->ref_count--;

	if (item->ref_count == 0)
	{
		if (item->collection != NULL)
		{
			j_collection_unref(item->collection);
		}

		if (item->semantics != NULL)
		{
			j_semantics_unref(item->semantics);
		}

		if (item->status != NULL)
		{
			j_item_status_unref(item->status);
		}

		g_free(item->name);

		g_slice_free(JItem, item);
	}

	j_trace_leave(j_trace(), G_STRFUNC);
}

/**
 * Returns an item's name.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return An item name.
 **/
const gchar*
j_item_get_name (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return item->name;
}

JItemStatus*
j_item_get_status (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return item->status;
}

void
j_item_set_status (JItem* item, JItemStatus* status)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(status != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (item->status != NULL)
	{
		j_item_status_unref(item->status);
	}

	item->status = status;

	j_trace_leave(j_trace(), G_STRFUNC);
}

/**
 * Returns an item's semantics.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A semantics object.
 **/
JSemantics*
j_item_get_semantics (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (item->semantics != NULL)
	{
		return item->semantics;
	}

	j_trace_leave(j_trace(), G_STRFUNC);

	return j_collection_get_semantics(item->collection);
}

/**
 * Sets an item's semantics.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param semantics A semantics object.
 **/
void
j_item_set_semantics (JItem* item, JSemantics* semantics)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(semantics != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (item->semantics != NULL)
	{
		j_semantics_unref(item->semantics);
	}

	item->semantics = j_semantics_ref(semantics);

	j_trace_leave(j_trace(), G_STRFUNC);
}

/**
 * Creates an item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param operation An operation.
 **/
void
j_item_create (JItem* item, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(item != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_ITEM_CREATE;
	part->u.item_create.item = j_item_ref(item);

	j_operation_add(operation, part);
}

/**
 * Gets an item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param operation An operation.
 **/
void
j_item_get (JItem* item, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(item != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_ITEM_GET;
	part->u.item_get.item = j_item_ref(item);

	j_operation_add(operation, part);
}

/**
 * Reads an item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item       An item.
 * \param data       A buffer to hold the read data.
 * \param length     Number of bytes to read.
 * \param offset     An offset within #item.
 * \param bytes_read Number of bytes read.
 * \param operation  An operation.
 **/
void
j_item_read (JItem* item, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_read != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_ITEM_READ;
	part->u.item_read.item = j_item_ref(item);
	part->u.item_read.data = data;
	part->u.item_read.length = length;
	part->u.item_read.offset = offset;
	part->u.item_read.bytes_read = bytes_read;

	j_operation_add(operation, part);
}

/**
 * Writes an item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item        An item.
 * \param data        A buffer holding the data to write.
 * \param length      Number of bytes to write.
 * \param offset      An offset within #item.
 * \param bytes_write Number of bytes written.
 * \param operation   An operation.
 **/
void
j_item_write (JItem* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_written != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_ITEM_WRITE;
	part->u.item_write.item = j_item_ref(item);
	part->u.item_write.data = data;
	part->u.item_write.length = length;
	part->u.item_write.offset = offset;
	part->u.item_write.bytes_written = bytes_written;

	j_operation_add(operation, part);
}

/* Internal */

/**
 * Creates a new item from a BSON object.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param b          A BSON object.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_new_from_bson (JCollection* collection, bson* b)
{
	JItem* item;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(b != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	item = g_slice_new(JItem);
	item->name = NULL;
	item->status = NULL;
	item->collection = j_collection_ref(collection);
	item->semantics = NULL;
	item->ref_count = 1;

	j_item_deserialize(item, b);

	j_trace_leave(j_trace(), G_STRFUNC);

	return item;
}

/**
 * Serializes an item.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
bson*
j_item_serialize (JItem* item)
{
	bson* b;

	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	b = g_slice_new(bson);
	bson_init(b);
	bson_append_oid(b, "_id", &(item->id));
	bson_append_oid(b, "Collection", j_collection_get_id(item->collection));
	bson_append_string(b, "Name", item->name);
	bson_finish(b);

	j_trace_leave(j_trace(), G_STRFUNC);

	return b;
}

/**
 * Deserializes an item.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param b    A BSON object.
 **/
void
j_item_deserialize (JItem* item, bson* b)
{
	bson_iterator iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	bson_iterator_init(&iterator, b->data);

	while (bson_iterator_next(&iterator))
	{
		const gchar* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "_id") == 0)
		{
			item->id = *bson_iterator_oid(&iterator);
		}
		else if (g_strcmp0(key, "Name") == 0)
		{
			g_free(item->name);
			item->name = g_strdup(bson_iterator_string(&iterator));
		}
	}

	j_trace_leave(j_trace(), G_STRFUNC);
}

/**
 * Returns an item's ID.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return An ID.
 **/
bson_oid_t const*
j_item_get_id (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return &(item->id);
}

void
j_item_create_internal (JList* parts)
{
	JCollection* collection;
	JListIterator* it;
	JSemantics* semantics;
	bson** obj;
	bson index;
	mongo* connection;
	guint i;
	guint length;

	g_return_if_fail(parts != NULL);

	/*
	IsInitialized(true);
	*/

	i = 0;
	length = j_list_length(parts);
	obj = g_new(bson*, length);
	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JItem* item = part->u.item_create.item;
		bson* b;

		collection = item->collection;
		b = j_item_serialize(item);

		obj[i] = b;
		i++;
	}

	j_list_iterator_free(it);

	connection = j_connection_get_connection(j_store_get_connection(j_collection_get_store(collection)));

	bson_init(&index);
	bson_append_int(&index, "Collection", 1);
	bson_append_int(&index, "Name", 1);
	bson_finish(&index);

	mongo_create_index(connection, j_collection_collection_items(collection), &index, MONGO_INDEX_UNIQUE, NULL);
	mongo_insert_batch(connection, j_collection_collection_items(collection), obj, length);

	bson_destroy(&index);

	for (i = 0; i < length; i++)
	{
		bson_destroy(obj[i]);
		g_slice_free(bson, obj[i]);
	}

	g_free(obj);

	semantics = j_collection_get_semantics(collection);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_STRICT)
	{
		mongo_simple_int_command(connection, "admin", "fsync", 1, NULL);
	}
}

void
j_item_get_internal (JList* parts)
{
	JCollection* collection;
	bson empty;
	bson b;
	mongo* connection;
	mongo_cursor* iterator;
	guint length;
	guint n = 0;

	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	bson_init(&b);
	length = j_list_length(parts);

	if (length == 1)
	{
		JOperationPart* part = j_list_get(parts, 0);
		JItem* item = part->u.item_get.item;

		collection = item->collection;

		bson_append_oid(&b, "Collection", j_collection_get_id(item->collection));
		bson_append_string(&b, "Name", item->name);
		n = 1;
	}
	else if (length > 1)
	{
		bson names_bson;
		JListIterator* it;

		bson_init(&names_bson);
		it = j_list_iterator_new(parts);

		while (j_list_iterator_next(it))
		{
			JOperationPart* part = j_list_iterator_get(it);
			JItem* item = part->u.item_get.item;

			collection = item->collection;

			bson_append_string(&names_bson, "Name", item->name);
		}

		j_list_iterator_free(it);
		bson_finish(&names_bson);

		bson_append_oid(&b, "Collection", j_collection_get_id(collection));
		bson_append_bson(&b, "$or", &names_bson);
		bson_destroy(&names_bson);
	}

	bson_finish(&b);
	bson_empty(&empty);

	connection = j_connection_get_connection(j_store_get_connection(j_collection_get_store(collection)));
	iterator = mongo_find(connection, j_collection_collection_items(collection), &b, NULL, n, 0, 0);

	while (mongo_cursor_next(iterator))
	{
		//j_list_append(items, j_item_new_from_bson(collection, &(iterator->current)));
	}

	mongo_cursor_destroy(iterator);

	bson_destroy(&b);
}

void
j_item_read_internal (JList* parts)
{
	JDistribution* distribution;
	JListIterator* iterator;
	gchar* d;
	guint64 new_length;
	guint64 new_offset;
	guint index;

	g_return_if_fail(parts != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	iterator = j_list_iterator_new(parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);
		JItem* item = part->u.item_read.item;
		gpointer data = part->u.item_read.data;
		guint64 length = part->u.item_read.length;
		guint64 offset = part->u.item_read.offset;
		guint64* bytes_read = part->u.item_read.bytes_read;

		*bytes_read = 0;

		if (length == 0)
		{
			continue;
		}

		j_trace_file_begin(j_trace(), item->name, J_TRACE_FILE_READ);

		distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, length, offset);
		d = data;

		while (j_distribution_distribute(distribution, &index, &new_length, &new_offset))
		{
			JMessage* message;
			gchar const* store;
			gchar const* collection;
			gsize store_len;
			gsize collection_len;
			gsize item_len;

			store = j_store_get_name(j_collection_get_store(item->collection));
			collection = j_collection_get_name(item->collection);

			store_len = strlen(store) + 1;
			collection_len = strlen(collection) + 1;
			item_len = strlen(item->name) + 1;

			message = j_message_new(store_len + collection_len + item_len + sizeof(guint64) + sizeof(guint64), J_MESSAGE_OPERATION_READ, 1);
			j_message_append_n(message, store, store_len);
			j_message_append_n(message, collection, collection_len);
			j_message_append_n(message, item->name, item_len);
			j_message_append_8(message, &new_length);
			j_message_append_8(message, &new_offset);

			j_connection_send(j_store_get_connection(j_collection_get_store(item->collection)), index, message, NULL, 0);
			j_connection_receive(j_store_get_connection(j_collection_get_store(item->collection)), index, message, d, new_length);

			j_message_free(message);

			d += new_length;
			*bytes_read += new_length;
		}

		j_distribution_free(distribution);

		j_trace_file_end(j_trace(), item->name, J_TRACE_FILE_READ, length, offset);
	}

	j_list_iterator_free(iterator);

	j_trace_leave(j_trace(), G_STRFUNC);
}

void
j_item_write_internal (JList* parts)
{
	JDistribution* distribution;
	JListIterator* iterator;
	guint64 new_length;
	guint64 new_offset;
	guint index;
	gchar const* d;

	g_return_if_fail(parts != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	iterator = j_list_iterator_new(parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);
		JItem* item = part->u.item_write.item;
		gconstpointer data = part->u.item_write.data;
		guint64 length = part->u.item_write.length;
		guint64 offset = part->u.item_write.offset;
		guint64* bytes_written = part->u.item_write.bytes_written;

		*bytes_written = 0;

		if (length == 0)
		{
			continue;
		}

		j_trace_file_begin(j_trace(), item->name, J_TRACE_FILE_WRITE);

		distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, length, offset);
		d = data;

		while (j_distribution_distribute(distribution, &index, &new_length, &new_offset))
		{
			JMessage* message;
			gchar const* store;
			gchar const* collection;
			gsize store_len;
			gsize collection_len;
			gsize item_len;

			store = j_store_get_name(j_collection_get_store(item->collection));
			collection = j_collection_get_name(item->collection);

			store_len = strlen(store) + 1;
			collection_len = strlen(collection) + 1;
			item_len = strlen(item->name) + 1;

			message = j_message_new(store_len + collection_len + item_len + sizeof(guint64) + sizeof(guint64), J_MESSAGE_OPERATION_WRITE, 1);
			j_message_append_n(message, store, store_len);
			j_message_append_n(message, collection, collection_len);
			j_message_append_n(message, item->name, item_len);
			j_message_append_8(message, &new_length);
			j_message_append_8(message, &new_offset);

			j_connection_send(j_store_get_connection(j_collection_get_store(item->collection)), index, message, d, new_length);

			j_message_free(message);

			d += new_length;
			*bytes_written += new_length;
		}

		j_distribution_free(distribution);

		j_trace_file_end(j_trace(), item->name, J_TRACE_FILE_WRITE, length, offset);
	}

	j_list_iterator_free(iterator);

	j_trace_leave(j_trace(), G_STRFUNC);
}


/*
	void _Item::IsInitialized (bool check)
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Item not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Item already initialized.");
			}
		}
	}
*/

/**
 * @}
 **/
