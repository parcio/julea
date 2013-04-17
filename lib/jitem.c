/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <julea-config.h>

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
#include <jconnection-pool-internal.h>
#include <jcredentials-internal.h>
#include <jdistribution.h>
#include <jhelper-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jlock-internal.h>
#include <jmessage.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-internal.h>
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
		 * The read status part.
		 */
		struct
		{
			/**
			 * The list of items.
			 * Contains #JItemReadStatusData elements.
			 */
			JList* buffer_list;
		}
		read_status;

		/**
		 * The write part.
		 */
		struct
		{
			/**
			 * The create message.
			 */
			JMessage* create_message;
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
 * Data for buffers.
 */
struct JItemReadStatusData
{
	/**
	 * The item.
	 */
	JItem* item;

	/**
	 * The item status flags.
	 */
	JItemStatusFlags flags;

	guint64* sizes;
};

typedef struct JItemReadStatusData JItemReadStatusData;

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

	JCredentials* credentials;

	/**
	 * The status.
	 **/
	struct
	{
		guint64 age;

		/**
		 * The size.
		 * Stored in bytes.
		 */
		guint64 size;

		/**
		 * The time of the last modification.
		 * Stored in microseconds since the Epoch.
		 */
		gint64 modification_time;

		gboolean* created;
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
	operation_count = j_message_get_count(background_data->message);

	while (operations_done < operation_count)
	{
		guint32 reply_operation_count;

		j_connection_receive(background_data->connection, background_data->index, reply);

		reply_operation_count = j_message_get_count(reply);

		for (guint j = 0; j < reply_operation_count && j_list_iterator_next(iterator); j++)
		{
			JItemReadData* buffer = j_list_iterator_get(iterator);

			nbytes = j_message_get_8(reply);
			// FIXME thread-safety
			*(buffer->nbytes) += nbytes;

			if (nbytes > 0)
			{
				j_connection_receive_data(background_data->connection, background_data->index, buffer->data, nbytes);
			}

			g_slice_free(JItemReadData, buffer);
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
	JMessage* reply;

	if (background_data->u.write.create_message != NULL)
	{
		/* FIXME reply? */
		j_connection_send(background_data->connection, background_data->index, background_data->u.write.create_message);
	}

	j_connection_send(background_data->connection, background_data->index, background_data->message);

	if (j_message_get_type_modifier(background_data->message) & J_MESSAGE_SAFETY_NETWORK)
	{
		reply = j_message_new_reply(background_data->message);
		j_connection_receive(background_data->connection, background_data->index, reply);

		/* FIXME do something with reply */
		j_message_unref(reply);
	}

	return data;
}

/**
 * Executes get status operations in a background operation.
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
j_item_get_status_background_operation (gpointer data)
{
	JItemBackgroundData* background_data = data;
	JListIterator* iterator;
	JMessage* reply;

	/* FIXME reply? */
	j_connection_send(background_data->connection, background_data->index, background_data->message);

	reply = j_message_new_reply(background_data->message);
	iterator = j_list_iterator_new(background_data->u.read_status.buffer_list);

	j_connection_receive(background_data->connection, background_data->index, reply);

	while (j_list_iterator_next(iterator))
	{
		JItemReadStatusData* buffer = j_list_iterator_get(iterator);

		if (buffer->flags & J_ITEM_STATUS_MODIFICATION_TIME)
		{
			gint64 modification_time;

			modification_time = j_message_get_8(reply);
			// FIXME thread-safety
			j_item_set_modification_time(buffer->item, modification_time);
		}

		if (buffer->flags & J_ITEM_STATUS_SIZE)
		{
			guint64 size;

			size = j_message_get_8(reply);
			// FIXME thread-safety
			//j_item_add_size(buffer->item, size);
			buffer->sizes[background_data->index] = size;
		}
	}

	j_list_iterator_free(iterator);
	j_message_unref(reply);

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
 * \param name An item name.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_new (gchar const* name)
{
	JItem* item;

	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	item = g_slice_new(JItem);
	bson_oid_gen(&(item->id));
	item->name = g_strdup(name);
	item->credentials = j_credentials_new();
	item->status.age = g_get_real_time();
	item->status.size = 0;
	item->status.modification_time = g_get_real_time();
	item->status.created = NULL;
	item->collection = NULL;
	item->ref_count = 1;

	j_trace_leave(G_STRFUNC);

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

	j_trace_enter(G_STRFUNC);

	g_atomic_int_inc(&(item->ref_count));

	j_trace_leave(G_STRFUNC);

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

	j_trace_enter(G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
		if (item->collection != NULL)
		{
			j_collection_unref(item->collection);
		}

		g_free(item->status.created);
		g_free(item->name);

		g_slice_free(JItem, item);
	}

	j_trace_leave(G_STRFUNC);
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
 * \return The name.
 **/
gchar const*
j_item_get_name (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

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
 * \param batch      A batch.
 **/
void
j_item_read (JItem* item, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_read != NULL);

	j_trace_enter(G_STRFUNC);

	operation = j_operation_new(J_OPERATION_ITEM_READ);
	operation->key = item;
	operation->u.item_read.item = j_item_ref(item);
	operation->u.item_read.data = data;
	operation->u.item_read.length = length;
	operation->u.item_read.offset = offset;
	operation->u.item_read.bytes_read = bytes_read;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
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
 * \param batch         A batch.
 **/
void
j_item_write (JItem* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_written != NULL);

	j_trace_enter(G_STRFUNC);

	operation = j_operation_new(J_OPERATION_ITEM_WRITE);
	operation->key = item;
	operation->u.item_write.item = j_item_ref(item);
	operation->u.item_write.data = data;
	operation->u.item_write.length = length;
	operation->u.item_write.offset = offset;
	operation->u.item_write.bytes_written = bytes_written;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
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
 * \param batch     A batch.
 **/
void
j_item_get_status (JItem* item, JItemStatusFlags flags, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC);

	operation = j_operation_new(J_OPERATION_ITEM_GET_STATUS);
	operation->key = item->collection;
	operation->u.item_get_status.item = j_item_ref(item);
	operation->u.item_get_status.flags = flags;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
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

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return item->status.size;
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

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return item->status.modification_time;
}

/**
 * Returns the item's optimal access size.
 *
 * \author Michael Kuhn
 *
 * \code
 * JItem* item;
 * guint64 optimal_size;
 *
 * ...
 * optimal_size = j_item_get_optimal_access_size(item);
 * j_item_write(item, buffer, optimal_size, 0, &dummy, batch);
 * ...
 * \endcode
 *
 * \param item An item.
 *
 * \return An access size.
 */
guint64
j_item_get_optimal_access_size (JItem* item)
{
	g_return_val_if_fail(item != NULL, 0);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return J_KIB(512);
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

	j_trace_enter(G_STRFUNC);

	item = g_slice_new(JItem);
	item->name = NULL;
	item->credentials = j_credentials_new();
	item->status.age = 0;
	item->status.size = 0;
	item->status.modification_time = 0;
	item->status.created = NULL;
	item->collection = j_collection_ref(collection);
	item->ref_count = 1;

	j_item_deserialize(item, b);

	j_trace_leave(G_STRFUNC);

	return item;
}

/**
 * Returns an item's collection.
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
 * \return A collection.
 **/
JCollection*
j_item_get_collection (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return item->collection;
}

/**
 * Returns an item's credentials.
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
 * \return A collection.
 **/
JCredentials*
j_item_get_credentials (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return item->credentials;
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
 * \param item      An item.
 * \param semantics A semantics object.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
bson*
j_item_serialize (JItem* item, JSemantics* semantics)
{
	bson* b;
	bson* b_cred;

	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	b_cred = j_credentials_serialize(item->credentials);

	b = g_slice_new(bson);
	bson_init(b);

	bson_append_oid(b, "_id", &(item->id));
	bson_append_oid(b, "Collection", j_collection_get_id(item->collection));
	bson_append_string(b, "Name", item->name);

	bson_append_start_object(b, "Status");

	if (j_semantics_get(semantics, J_SEMANTICS_CONCURRENCY) == J_SEMANTICS_CONCURRENCY_NONE)
	{
		bson_append_long(b, "Size", item->status.size);
		bson_append_long(b, "ModificationTime", item->status.modification_time);
	}

	bson_append_finish_object(b);

	bson_append_bson(b, "Credentials", b_cred);

	bson_finish(b);

	bson_destroy(b_cred);
	g_slice_free(bson, b_cred);

	j_trace_leave(G_STRFUNC);

	return b;
}

static
void
j_item_deserialize_status (JItem* item, bson const* b)
{
	bson_iterator iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC);

	bson_iterator_init(&iterator, b);

	while (bson_iterator_next(&iterator))
	{
		gchar const* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "Size") == 0)
		{
			item->status.size = bson_iterator_long(&iterator);
			item->status.age = g_get_real_time();
		}
		else if (g_strcmp0(key, "ModificationTime") == 0)
		{
			item->status.modification_time = bson_iterator_long(&iterator);
			item->status.age = g_get_real_time();
		}
	}

	j_trace_leave(G_STRFUNC);
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

	j_trace_enter(G_STRFUNC);

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
		else if (g_strcmp0(key, "Credentials") == 0)
		{
			bson b_cred[1];

			bson_iterator_subobject(&iterator, b_cred);
			j_credentials_deserialize(item->credentials, b_cred);
		}
		else if (g_strcmp0(key, "Status") == 0)
		{
			bson b_status[1];

			bson_iterator_subobject(&iterator, b_status);
			j_item_deserialize_status(item, b_status);
		}
	}

	j_trace_leave(G_STRFUNC);
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

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return &(item->id);
}

void
j_item_set_collection (JItem* item, JCollection* collection)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(item->collection == NULL);

	j_trace_enter(G_STRFUNC);

	item->collection = j_collection_ref(collection);

	j_trace_leave(G_STRFUNC);
}

/**
 * Sets an item's modification time.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item              An item.
 * \param modification_time A modification time.
 **/
void
j_item_set_modification_time (JItem* item, gint64 modification_time)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC);
	item->status.age = g_get_real_time();
	item->status.modification_time = MAX(item->status.modification_time, modification_time);
	j_trace_leave(G_STRFUNC);
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

	j_trace_enter(G_STRFUNC);
	item->status.age = g_get_real_time();
	item->status.size = size;
	j_trace_leave(G_STRFUNC);
}

gboolean
j_item_read_internal (JBatch* batch, JList* operations)
{
	JBackgroundOperation** background_operations;
	JConnection* connection;
	JItem* item;
	JList** buffer_list;
	JListIterator* iterator;
	JLock* lock = NULL;
	JMessage** messages;
	JSemantics* semantics;
	guint m;
	guint n;
	gchar const* item_name;
	gchar const* collection_name;
	gchar const* store_name;
	gsize item_len;
	gsize collection_len;
	gsize store_len;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	semantics = j_batch_get_semantics(batch);
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
		JOperation* operation;

		operation = j_list_get_first(operations);
		g_assert(operation != NULL);
		item = operation->u.item_read.item;

		item_name = item->name;
		collection_name = j_collection_get_name(item->collection);
		store_name = j_store_get_name(j_collection_get_store(item->collection));

		item_len = strlen(item_name) + 1;
		collection_len = strlen(collection_name) + 1;
		store_len = strlen(store_name) + 1;
	}

	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new(item);
	}

	iterator = j_list_iterator_new(operations);

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);
		gpointer data = operation->u.item_read.data;
		guint64 length = operation->u.item_read.length;
		guint64 offset = operation->u.item_read.offset;
		guint64* bytes_read = operation->u.item_read.bytes_read;

		JDistribution* distribution;
		JItemReadData* buffer;
		gchar* d;
		guint64 new_length;
		guint64 new_offset;
		guint64 block_id;
		guint index;

		*bytes_read = 0;

		if (length == 0)
		{
			continue;
		}

		j_trace_file_begin(item_name, J_TRACE_FILE_READ);

		distribution = j_distribution_new(j_configuration(), J_DISTRIBUTION_ROUND_ROBIN, length, offset);
		d = data;

		while (j_distribution_distribute(distribution, &index, &new_length, &new_offset, &block_id))
		{
			if (messages[index] == NULL)
			{
				messages[index] = j_message_new(J_MESSAGE_READ, store_len + collection_len + item_len);
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

			if (lock != NULL)
			{
				j_lock_add(lock, block_id);
			}

			d += new_length;
		}

		j_distribution_free(distribution);

		j_trace_file_end(item_name, J_TRACE_FILE_READ, length, offset);
	}

	j_list_iterator_free(iterator);

	if (lock != NULL)
	{
		/* FIXME busy wait */
		while (!j_lock_acquire(lock));
	}

	connection = j_connection_pool_pop();

	// FIXME ugly
	m = 0;

	for (guint i = 0; i < n; i++)
	{
		if (messages[i] != NULL)
		{
			m++;
		}
	}

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

		// FIXME ugly
		if (m > 1)
		{
			background_operations[i] = j_background_operation_new(j_item_read_background_operation, background_data);
		}
		else
		{
			j_item_read_background_operation(background_data);
		}
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

	j_connection_pool_push(connection);

	if (lock != NULL)
	{
		j_lock_free(lock);
	}

	g_free(background_operations);
	g_free(messages);
	g_free(buffer_list);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

gboolean
j_item_write_internal (JBatch* batch, JList* operations)
{
	JBackgroundOperation** background_operations;
	JConnection* connection;
	JItem* item;
	JListIterator* iterator;
	JLock* lock = NULL;
	JMessage** messages;
	JSemantics* semantics;
	guint m;
	guint n;
	gchar const* item_name;
	gchar const* collection_name;
	gchar const* store_name;
	gsize item_len;
	gsize collection_len;
	gsize store_len;
	guint64 max_offset = 0;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	semantics = j_batch_get_semantics(batch);
	n = j_configuration_get_data_server_count(j_configuration());
	background_operations = g_new(JBackgroundOperation*, n);
	messages = g_new(JMessage*, n);

	for (guint i = 0; i < n; i++)
	{
		background_operations[i] = NULL;
		messages[i] = NULL;
	}

	{
		JOperation* operation;

		operation = j_list_get_first(operations);
		g_assert(operation != NULL);
		item = operation->u.item_write.item;

		item_name = item->name;
		collection_name = j_collection_get_name(item->collection);
		store_name = j_store_get_name(j_collection_get_store(item->collection));

		store_len = strlen(store_name) + 1;
		collection_len = strlen(collection_name) + 1;
		item_len = strlen(item->name) + 1;
	}

	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new(item);
	}

	iterator = j_list_iterator_new(operations);

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);
		gconstpointer data = operation->u.item_write.data;
		guint64 length = operation->u.item_write.length;
		guint64 offset = operation->u.item_write.offset;
		guint64* bytes_written = operation->u.item_write.bytes_written;

		JDistribution* distribution;
		gchar const* d;
		guint64 new_length;
		guint64 new_offset;
		guint64 block_id;
		guint index;

		*bytes_written = 0;

		if (length == 0)
		{
			continue;
		}

		max_offset = MAX(max_offset, offset + length);

		j_trace_file_begin(item_name, J_TRACE_FILE_WRITE);

		distribution = j_distribution_new(j_configuration(), J_DISTRIBUTION_ROUND_ROBIN, length, offset);
		d = data;

		while (j_distribution_distribute(distribution, &index, &new_length, &new_offset, &block_id))
		{
			if (messages[index] == NULL)
			{
				/* FIXME */
				messages[index] = j_message_new(J_MESSAGE_WRITE, store_len + collection_len + item_len);
				j_message_set_safety(messages[index], semantics);
				j_message_append_n(messages[index], store_name, store_len);
				j_message_append_n(messages[index], collection_name, collection_len);
				j_message_append_n(messages[index], item_name, item_len);
			}

			j_message_add_operation(messages[index], sizeof(guint64) + sizeof(guint64));
			j_message_append_8(messages[index], &new_length);
			j_message_append_8(messages[index], &new_offset);
			j_message_add_send(messages[index], d, new_length);

			if (lock != NULL)
			{
				j_lock_add(lock, block_id);
			}

			d += new_length;
			/* FIXME */
			*bytes_written += new_length;
		}

		j_distribution_free(distribution);

		j_trace_file_end(item_name, J_TRACE_FILE_WRITE, length, offset);
	}

	j_list_iterator_free(iterator);

	if (lock != NULL)
	{
		/* FIXME busy wait */
		while (!j_lock_acquire(lock));
	}

	connection = j_connection_pool_pop();

	m = 0;

	for (guint i = 0; i < n; i++)
	{
		if (messages[i] != NULL)
		{
			m++;
		}
	}

	for (guint i = 0; i < n; i++)
	{
		JItemBackgroundData* background_data;
		JMessage* create_message = NULL;

		if (messages[i] == NULL)
		{
			continue;
		}

		if (item->status.created == NULL)
		{
			item->status.created = g_new(gboolean, n);

			for (guint j = 0; j < n; j++)
			{
				item->status.created[j] = FALSE;
			}
		}

		if (!item->status.created[i])
		{
			/* FIXME better solution? */
			create_message = j_message_new(J_MESSAGE_CREATE, store_len + collection_len);
			j_message_append_n(create_message, store_name, store_len);
			j_message_append_n(create_message, collection_name, collection_len);
			j_message_add_operation(create_message, item_len);
			j_message_append_n(create_message, item_name, item_len);

			item->status.created[i] = TRUE;
		}

		background_data = g_slice_new(JItemBackgroundData);
		background_data->connection = connection;
		background_data->message = messages[i];
		background_data->index = i;
		background_data->u.write.create_message = create_message;

		if (m > 1)
		{
			background_operations[i] = j_background_operation_new(j_item_write_background_operation, background_data);
		}
		else
		{
			j_item_write_background_operation(background_data);

			if (create_message != NULL)
			{
				j_message_unref(create_message);
			}

			g_slice_free(JItemBackgroundData, background_data);
		}
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

			g_slice_free(JItemBackgroundData, background_data);
		}

		if (messages[i] != NULL)
		{
			j_message_unref(messages[i]);
		}
	}

	if (j_semantics_get(semantics, J_SEMANTICS_CONCURRENCY) == J_SEMANTICS_CONCURRENCY_NONE && FALSE)
	{
		bson cond[1];
		bson op[1];
		mongo_write_concern write_concern[1];
		gint ret;

		j_helper_set_write_concern(write_concern, semantics);

		bson_init(cond);
		bson_append_oid(cond, "_id", &(item->id));
		bson_append_int(cond, "Status.ModificationTime", item->status.modification_time);
		bson_finish(cond);

		j_item_set_modification_time(item, g_get_real_time());

		bson_init(op);

		bson_append_start_object(op, "$set");

		if (max_offset > item->status.size)
		{
			j_item_set_size(item, max_offset);
			bson_append_long(op, "Status.Size", item->status.size);
		}

		bson_append_long(op, "Status.ModificationTime", item->status.modification_time);
		bson_append_finish_object(op);

		bson_finish(op);

		ret = mongo_update(j_connection_get_connection(connection), j_collection_collection_items(item->collection), cond, op, MONGO_UPDATE_BASIC, write_concern);
		g_assert(ret == MONGO_OK);

		if (ret != MONGO_OK)
		{

		}

		bson_destroy(cond);
		bson_destroy(op);

		mongo_write_concern_destroy(write_concern);
	}

	j_connection_pool_push(connection);

	if (lock != NULL)
	{
		j_lock_free(lock);
	}

	g_free(background_operations);
	g_free(messages);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

gboolean
j_item_get_status_internal (JBatch* batch, JList* operations)
{
	gboolean ret = TRUE;
	JBackgroundOperation** background_operations;
	JConnection* connection = NULL;
	JList* buffer_list;
	JListIterator* iterator;
	JMessage** messages;
	JSemantics* semantics;
	gint semantics_concurrency;
	gint semantics_consistency;
	guint n;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	n = j_configuration_get_data_server_count(j_configuration());
	background_operations = g_new(JBackgroundOperation*, n);
	messages = g_new(JMessage*, n);
	buffer_list = j_list_new(NULL);

	for (guint i = 0; i < n; i++)
	{
		background_operations[i] = NULL;
		messages[i] = NULL;
	}

	iterator = j_list_iterator_new(operations);
	semantics = j_batch_get_semantics(batch);
	semantics_concurrency = j_semantics_get(semantics, J_SEMANTICS_CONCURRENCY);
	semantics_consistency = j_semantics_get(semantics, J_SEMANTICS_CONSISTENCY);

	connection = j_connection_pool_pop();

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);
		JItem* item = operation->u.item_get_status.item;
		JItemStatusFlags flags = operation->u.item_get_status.flags;
		bson b;
		bson fields;
		mongo_cursor* cursor;

		if (flags == J_ITEM_STATUS_NONE)
		{
			continue;
		}

		if (semantics_consistency != J_SEMANTICS_CONSISTENCY_IMMEDIATE)
		{
			if (item->status.age >= (guint64)g_get_real_time() - G_USEC_PER_SEC)
			{
				continue;
			}
		}

		if (semantics_concurrency == J_SEMANTICS_CONCURRENCY_NONE)
		{
			bson_init(&fields);

			if (flags & J_ITEM_STATUS_SIZE)
			{
				bson_append_int(&fields, "Status.Size", 1);
			}

			if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
			{
				bson_append_int(&fields, "Status.ModificationTime", 1);
			}

			bson_finish(&fields);

			bson_init(&b);
			bson_append_oid(&b, "_id", &(item->id));
			bson_finish(&b);

			cursor = mongo_find(j_connection_get_connection(connection), j_collection_collection_items(item->collection), &b, &fields, 1, 0, 0);

			bson_destroy(&fields);
			bson_destroy(&b);

			// FIXME ret
			while (mongo_cursor_next(cursor) == MONGO_OK)
			{
				j_item_deserialize(item, mongo_cursor_bson(cursor));
			}

			mongo_cursor_destroy(cursor);
		}
		else
		{
			JItemReadStatusData* buffer;

			for (guint i = 0; i < n; i++)
			{
				gchar const* item_name;
				gsize item_len;

				item_name = item->name;
				item_len = strlen(item->name) + 1;

				if (messages[i] == NULL)
				{
					gchar const* collection_name;
					gchar const* store_name;
					gsize collection_len;
					gsize store_len;

					collection_name = j_collection_get_name(item->collection);
					store_name = j_store_get_name(j_collection_get_store(item->collection));

					store_len = strlen(store_name) + 1;
					collection_len = strlen(collection_name) + 1;

					messages[i] = j_message_new(J_MESSAGE_STATUS, store_len + collection_len);
					j_message_append_n(messages[i], store_name, store_len);
					j_message_append_n(messages[i], collection_name, collection_len);
				}

				j_message_add_operation(messages[i], item_len + sizeof(guint32));
				j_message_append_n(messages[i], item_name, item_len);
				j_message_append_4(messages[i], &flags);
			}

			buffer = g_slice_new(JItemReadStatusData);
			buffer->item = item;
			buffer->flags = flags;
			buffer->sizes = g_slice_alloc0(n * sizeof(guint64));

			j_list_append(buffer_list, buffer);
		}
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
		background_data->u.read_status.buffer_list = buffer_list;

		background_operations[i] = j_background_operation_new(j_item_get_status_background_operation, background_data);
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

	}

	j_connection_pool_push(connection);

	iterator = j_list_iterator_new(buffer_list);

	while (j_list_iterator_next(iterator))
	{
		JItemReadStatusData* buffer = j_list_iterator_get(iterator);
		guint64 size = 0;

		for (guint i = 0; i < n; i++)
		{
			size += buffer->sizes[i];
		}

		j_item_set_size(buffer->item, size);

		g_slice_free1(n * sizeof(guint64), buffer->sizes);
		g_slice_free(JItemReadStatusData, buffer);
	}

	j_list_iterator_free(iterator);

	j_list_unref(buffer_list);

	g_free(background_operations);
	g_free(messages);

	j_trace_leave(G_STRFUNC);

	return ret;
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
