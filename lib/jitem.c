/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <jitem.h>
#include <jitem-internal.h>

#include <jbackground-operation-internal.h>
#include <jcollection.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
#include <jconnection-internal.h>
#include <jdistribution.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jmessage.h>
#include <joperation.h>
#include <joperation-internal.h>
#include <joperation-part-internal.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

/**
 * \defgroup JItem Item
 *
 * Data structures and functions for managing items.
 *
 * @{
 **/

/**
 * Data for background operations.
 */
struct JItemBackgroundData
{
	/**
	 * The connection.
	 */
	JConnection* connection;

	/**
	 * The message to send.
	 */
	JMessage* message;

	/**
	 * The data server's index.
	 */
	guint index;

	/**
	 * The union for read and write parts.
	 */
	union
	{
		/**
		 * The read part.
		 */
		struct
		{
			/**
			 * The list of buffers to fill.
			 * Contains #JItemReadData elements.
			 */
			JList* buffer_list;
		}
		read;

		/**
		 * The write part.
		 */
		struct
		{
			/**
			 * The create message.
			 */
			JMessage* create_message;

			/**
			 * The sync message.
			 */
			JMessage* sync_message;
		}
		write;
	}
	u;
};

typedef struct JItemBackgroundData JItemBackgroundData;

/**
 * Data for buffers.
 */
struct JItemReadData
{
	/**
	 * The data.
	 */
	gpointer data;

	/**
	 * The data length.
	 */
	guint64* nbytes;
};

typedef struct JItemReadData JItemReadData;

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
	struct
	{
		guint flags;

		guint64 size;

		gint64 modification_time;
	}
	status;

	/**
	 * The parent collection.
	 **/
	JCollection* collection;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Executes read operations in a background operation.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static
gpointer
j_item_read_background_operation (gpointer data)
{
	JItemBackgroundData* background_data = data;
	JListIterator* iterator;
	JMessage* reply;
	guint operations_done;
	guint32 operation_count;
	guint64 nbytes;

	j_connection_send(background_data->connection, background_data->index, background_data->message);

	reply = j_message_new_reply(background_data->message);
	iterator = j_list_iterator_new(background_data->u.read.buffer_list);

	operations_done = 0;
	operation_count = j_message_operation_count(background_data->message);

	while (operations_done < operation_count)
	{
		guint32 reply_operation_count;

		j_connection_receive(background_data->connection, background_data->index, reply);

		reply_operation_count = j_message_operation_count(reply);

		for (guint j = 0; j < reply_operation_count && j_list_iterator_next(iterator); j++)
		{
			JItemReadData* buffer = j_list_iterator_get(iterator);

			nbytes = j_message_get_8(reply);
			*(buffer->nbytes) = nbytes;

			if (nbytes > 0)
			{
				j_connection_receive_data(background_data->connection, background_data->index, buffer->data, nbytes);
			}
		}

		operations_done += reply_operation_count;
	}

	j_list_iterator_free(iterator);
	j_message_unref(reply);

	/*
	if (nbytes < new_length)
	{
		break;
	}
	*/

	return data;
}

/**
 * Executes write operations in a background operation.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static
gpointer
j_item_write_background_operation (gpointer data)
{
	JItemBackgroundData* background_data = data;

	if (background_data->u.write.create_message != NULL)
	{
		/* FIXME reply? */
		j_connection_send(background_data->connection, background_data->index, background_data->u.write.create_message);
	}

	/* FIXME reply? */
	j_connection_send(background_data->connection, background_data->index, background_data->message);

	if (background_data->u.write.sync_message != NULL)
	{
		JMessage* reply;

		j_connection_send(background_data->connection, background_data->index, background_data->u.write.sync_message);

		reply = j_message_new_reply(background_data->u.write.sync_message);
		j_connection_receive(background_data->connection, background_data->index, reply);

		j_message_unref(reply);
	}

	return data;
}

/**
 * Creates a new item.
 *
 * \author Michael Kuhn
 *
 * \code
 * JItem* i;
 *
 * i = j_item_new("JULEA");
 * \endcode
 *
 * \param name       An item name.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_new (gchar const* name)
{
	JItem* item;

	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	item = g_slice_new(JItem);
	bson_oid_gen(&(item->id));
	item->name = g_strdup(name);
	item->status.flags = J_ITEM_STATUS_SIZE | J_ITEM_STATUS_MODIFICATION_TIME;
	item->status.size = 0;
	item->status.modification_time = g_get_real_time();
	item->collection = NULL;
	item->ref_count = 1;

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	g_atomic_int_inc(&(item->ref_count));

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
		if (item->collection != NULL)
		{
			j_collection_unref(item->collection);
		}

		g_free(item->name);

		g_slice_free(JItem, item);
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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
gchar const*
j_item_get_name (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return item->name;
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_operation_part_new(J_OPERATION_ITEM_READ);
	part->key = item;
	part->u.item_read.item = j_item_ref(item);
	part->u.item_read.data = data;
	part->u.item_read.length = length;
	part->u.item_read.offset = offset;
	part->u.item_read.bytes_read = bytes_read;

	j_operation_add(operation, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * Writes an item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item          An item.
 * \param data          A buffer holding the data to write.
 * \param length        Number of bytes to write.
 * \param offset        An offset within #item.
 * \param bytes_written Number of bytes written.
 * \param operation     An operation.
 **/
void
j_item_write (JItem* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_written != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_operation_part_new(J_OPERATION_ITEM_WRITE);
	part->key = item;
	part->u.item_write.item = j_item_ref(item);
	part->u.item_write.data = data;
	part->u.item_write.length = length;
	part->u.item_write.offset = offset;
	part->u.item_write.bytes_written = bytes_written;

	j_operation_add(operation, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * Get the status of an item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param flags     Status flags.
 * \param operation An operation.
 **/
void
j_item_get_status (JItem* item, JItemStatusFlags flags, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_operation_part_new(J_OPERATION_ITEM_GET_STATUS);
	part->key = NULL;
	part->u.item_get_status.item = j_item_ref(item);
	part->u.item_get_status.flags = flags;

	j_operation_add(operation, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * Returns an item's size.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A size.
 **/
guint64
j_item_get_size (JItem* item)
{
	g_return_val_if_fail(item != NULL, 0);
	g_return_val_if_fail((item->status.flags & J_ITEM_STATUS_SIZE) == J_ITEM_STATUS_SIZE, 0);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return item->status.size;
}

/**
 * Sets an item's size.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param size A size.
 **/
void
j_item_set_size (JItem* item, guint64 size)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	item->status.flags |= J_ITEM_STATUS_SIZE;
	item->status.size = size;
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * Returns an item's modification time.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A modification time.
 **/
gint64
j_item_get_modification_time (JItem* item)
{
	g_return_val_if_fail(item != NULL, 0);
	g_return_val_if_fail((item->status.flags & J_ITEM_STATUS_MODIFICATION_TIME) == J_ITEM_STATUS_MODIFICATION_TIME, 0);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return item->status.modification_time;
}

/**
 * Sets an item's modification time.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param size A modification time.
 **/
void
j_item_set_modification_time (JItem* item, gint64 modification_time)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	item->status.flags |= J_ITEM_STATUS_MODIFICATION_TIME;
	item->status.modification_time = modification_time;
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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
j_item_new_from_bson (JCollection* collection, bson const* b)
{
	JItem* item;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(b != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	item = g_slice_new(JItem);
	item->name = NULL;
	item->status.flags = J_ITEM_STATUS_NONE;
	item->status.size = 0;
	item->status.modification_time = 0;
	item->collection = j_collection_ref(collection);
	item->ref_count = 1;

	j_item_deserialize(item, b);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	b = g_slice_new(bson);
	bson_init(b);
	bson_append_oid(b, "_id", &(item->id));
	bson_append_oid(b, "Collection", j_collection_get_id(item->collection));
	bson_append_string(b, "Name", item->name);
	bson_append_long(b, "Size", item->status.size);
	bson_append_long(b, "ModificationTime", item->status.modification_time);
	bson_finish(b);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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
j_item_deserialize (JItem* item, bson const* b)
{
	bson_iterator iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	bson_iterator_init(&iterator, b);

	while (bson_iterator_next(&iterator))
	{
		gchar const* key;

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
		else if (g_strcmp0(key, "Size") == 0)
		{
			item->status.size = bson_iterator_long(&iterator);
			item->status.flags |= J_ITEM_STATUS_SIZE;
		}
		else if (g_strcmp0(key, "ModificationTime") == 0)
		{
			item->status.modification_time = bson_iterator_long(&iterator);
			item->status.flags |= J_ITEM_STATUS_MODIFICATION_TIME;
		}
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return &(item->id);
}

void
j_item_set_collection (JItem* item, JCollection* collection)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(item->collection == NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	item->collection = j_collection_ref(collection);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

gboolean
j_item_read_internal (JOperation* operation, JList* parts)
{
	JBackgroundOperation** background_operations;
	JConnection* connection;
	JList** buffer_list;
	JListIterator* iterator;
	JMessage** messages;
	guint n;
	gchar const* item_name;
	gchar const* collection_name;
	gchar const* store_name;
	gsize item_len;
	gsize collection_len;
	gsize store_len;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	n = j_configuration_get_data_server_count(j_configuration());
	background_operations = g_new(JBackgroundOperation*, n);
	messages = g_new(JMessage*, n);
	buffer_list = g_new(JList*, n);

	for (guint i = 0; i < n; i++)
	{
		background_operations[i] = NULL;
		messages[i] = NULL;
		buffer_list[i] = j_list_new(NULL);
	}

	{
		JItem* item;
		JOperationPart* part;

		part = j_list_get_first(parts);
		g_assert(part != NULL);
		item = part->u.item_read.item;

		connection = j_store_get_connection(j_collection_get_store(item->collection));

		item_name = item->name;
		collection_name = j_collection_get_name(item->collection);
		store_name = j_store_get_name(j_collection_get_store(item->collection));

		item_len = strlen(item_name) + 1;
		collection_len = strlen(collection_name) + 1;
		store_len = strlen(store_name) + 1;
	}

	iterator = j_list_iterator_new(parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);
		gpointer data = part->u.item_read.data;
		guint64 length = part->u.item_read.length;
		guint64 offset = part->u.item_read.offset;
		guint64* bytes_read = part->u.item_read.bytes_read;

		JDistribution* distribution;
		JItemReadData* buffer;
		gchar* d;
		guint64 new_length;
		guint64 new_offset;
		guint index;

		*bytes_read = 0;

		if (length == 0)
		{
			continue;
		}

		j_trace_file_begin(j_trace_get_thread_default(), item_name, J_TRACE_FILE_READ);

		distribution = j_distribution_new(j_configuration(), J_DISTRIBUTION_ROUND_ROBIN, length, offset);
		d = data;

		while (j_distribution_distribute(distribution, &index, &new_length, &new_offset))
		{
			if (messages[index] == NULL)
			{
				messages[index] = j_message_new(J_MESSAGE_OPERATION_READ, store_len + collection_len + item_len);
				j_message_append_n(messages[index], store_name, store_len);
				j_message_append_n(messages[index], collection_name, collection_len);
				j_message_append_n(messages[index], item_name, item_len);
			}

			j_message_add_operation(messages[index], sizeof(guint64) + sizeof(guint64));
			j_message_append_8(messages[index], &new_length);
			j_message_append_8(messages[index], &new_offset);

			buffer = g_slice_new(JItemReadData);
			buffer->data = d;
			buffer->nbytes = bytes_read;

			j_list_append(buffer_list[index], buffer);

			d += new_length;
		}

		j_distribution_free(distribution);

		j_trace_file_end(j_trace_get_thread_default(), item_name, J_TRACE_FILE_READ, length, offset);
	}

	j_list_iterator_free(iterator);

	for (guint i = 0; i < n; i++)
	{
		JItemBackgroundData* background_data;

		if (messages[i] == NULL)
		{
			continue;
		}

		background_data = g_slice_new(JItemBackgroundData);
		background_data->connection = connection;
		background_data->message = messages[i];
		background_data->index = i;
		background_data->u.read.buffer_list = buffer_list[i];

		background_operations[i] = j_background_operation_new(j_item_read_background_operation, background_data);
	}

	for (guint i = 0; i < n; i++)
	{
		if (background_operations[i] != NULL)
		{
			JItemBackgroundData* background_data;

			background_data = j_background_operation_wait(background_operations[i]);
			j_background_operation_unref(background_operations[i]);

			g_slice_free(JItemBackgroundData, background_data);
		}

		if (messages[i] != NULL)
		{
			j_message_unref(messages[i]);
		}

		j_list_unref(buffer_list[i]);
	}

	g_free(background_operations);
	g_free(messages);
	g_free(buffer_list);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return TRUE;
}

gboolean
j_item_write_internal (JOperation* operation, JList* parts)
{
	JBackgroundOperation** background_operations;
	JConnection* connection;
	JListIterator* iterator;
	JMessage** messages;
	guint n;
	gchar const* item_name;
	gchar const* collection_name;
	gchar const* store_name;
	gsize item_len;
	gsize collection_len;
	gsize store_len;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	n = j_configuration_get_data_server_count(j_configuration());
	background_operations = g_new(JBackgroundOperation*, n);
	messages = g_new(JMessage*, n);

	for (guint i = 0; i < n; i++)
	{
		background_operations[i] = NULL;
		messages[i] = NULL;
	}

	{
		JItem* item;
		JOperationPart* part;

		part = j_list_get_first(parts);
		g_assert(part != NULL);
		item = part->u.item_write.item;

		connection = j_store_get_connection(j_collection_get_store(item->collection));

		item_name = item->name;
		collection_name = j_collection_get_name(item->collection);
		store_name = j_store_get_name(j_collection_get_store(item->collection));

		store_len = strlen(store_name) + 1;
		collection_len = strlen(collection_name) + 1;
		item_len = strlen(item->name) + 1;
	}

	iterator = j_list_iterator_new(parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);
		gconstpointer data = part->u.item_write.data;
		guint64 length = part->u.item_write.length;
		guint64 offset = part->u.item_write.offset;
		guint64* bytes_written = part->u.item_write.bytes_written;

		JDistribution* distribution;
		gchar const* d;
		guint64 new_length;
		guint64 new_offset;
		guint index;

		*bytes_written = 0;

		if (length == 0)
		{
			continue;
		}

		j_trace_file_begin(j_trace_get_thread_default(), item_name, J_TRACE_FILE_WRITE);

		distribution = j_distribution_new(j_configuration(), J_DISTRIBUTION_ROUND_ROBIN, length, offset);
		d = data;

		while (j_distribution_distribute(distribution, &index, &new_length, &new_offset))
		{
			if (messages[index] == NULL)
			{
				/* FIXME */
				messages[index] = j_message_new(J_MESSAGE_OPERATION_WRITE, store_len + collection_len + item_len);
				j_message_append_n(messages[index], store_name, store_len);
				j_message_append_n(messages[index], collection_name, collection_len);
				j_message_append_n(messages[index], item_name, item_len);
			}

			j_message_add_operation(messages[index], sizeof(guint64) + sizeof(guint64));
			j_message_append_8(messages[index], &new_length);
			j_message_append_8(messages[index], &new_offset);
			j_message_add_send(messages[index], d, new_length);

			d += new_length;
			/* FIXME */
			*bytes_written += new_length;
		}

		j_distribution_free(distribution);

		j_trace_file_end(j_trace_get_thread_default(), item_name, J_TRACE_FILE_WRITE, length, offset);
	}

	j_list_iterator_free(iterator);

	for (guint i = 0; i < n; i++)
	{
		JItemBackgroundData* background_data;
		JMessage* create_message = NULL;
		JMessage* sync_message = NULL;

		if (messages[i] == NULL)
		{
			continue;
		}

		/* FIXME temporary workaround */
		create_message = j_message_new(J_MESSAGE_OPERATION_CREATE, store_len + collection_len);
		j_message_append_n(create_message, store_name, store_len);
		j_message_append_n(create_message, collection_name, collection_len);
		j_message_add_operation(create_message, item_len);
		j_message_append_n(create_message, item_name, item_len);

		if (j_semantics_get(j_operation_get_semantics(operation), J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_IMMEDIATE)
		{
			sync_message = j_message_new(J_MESSAGE_OPERATION_SYNC, store_len + collection_len + item_len);
			j_message_add_operation(sync_message, 0);
			j_message_append_n(sync_message, store_name, store_len);
			j_message_append_n(sync_message, collection_name, collection_len);
			j_message_append_n(sync_message, item_name, item_len);
		}

		background_data = g_slice_new(JItemBackgroundData);
		background_data->connection = connection;
		background_data->message = messages[i];
		background_data->index = i;
		background_data->u.write.sync_message = sync_message;

		background_operations[i] = j_background_operation_new(j_item_write_background_operation, background_data);
	}

	for (guint i = 0; i < n; i++)
	{
		if (background_operations[i] != NULL)
		{
			JItemBackgroundData* background_data;

			background_data = j_background_operation_wait(background_operations[i]);
			j_background_operation_unref(background_operations[i]);

			if (background_data->u.write.create_message != NULL)
			{
				j_message_unref(background_data->u.write.create_message);
			}

			if (background_data->u.write.sync_message != NULL)
			{
				j_message_unref(background_data->u.write.sync_message);
			}

			g_slice_free(JItemBackgroundData, background_data);
		}

		if (messages[i] != NULL)
		{
			j_message_unref(messages[i]);
		}
	}

	g_free(background_operations);
	g_free(messages);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return TRUE;
}

gboolean
j_item_get_status_internal (JOperation* operation, JList* parts)
{
	JListIterator* iterator;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	iterator = j_list_iterator_new(parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);
		JItem* item = part->u.item_get_status.item;
		JItemStatusFlags flags = part->u.item_get_status.flags;
		bson b;
		bson fields;
		mongo* connection;
		mongo_cursor* cursor;

		if (flags == J_ITEM_STATUS_NONE)
		{
			continue;
		}

		bson_init(&fields);

		if (flags & J_ITEM_STATUS_SIZE)
		{
			bson_append_int(&fields, "Size", 1);
		}

		if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
		{
			bson_append_int(&fields, "ModificationTime", 1);
		}

		bson_finish(&fields);

		bson_init(&b);
		bson_append_oid(&b, "_id", &(item->id));
		bson_finish(&b);

		connection = j_connection_get_connection(j_store_get_connection(j_collection_get_store(item->collection)));
		cursor = mongo_find(connection, j_collection_collection_items(item->collection), &b, &fields, 1, 0, 0);

		bson_destroy(&fields);
		bson_destroy(&b);

		while (mongo_cursor_next(cursor) == MONGO_OK)
		{
			j_item_deserialize(item, mongo_cursor_bson(cursor));
		}

		mongo_cursor_destroy(cursor);
	}

	j_list_iterator_free(iterator);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return TRUE;
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
