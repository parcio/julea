/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <bson.h>

#include <object/jobject.h>

#include <jbackground-operation-internal.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <jcommon-internal.h>
#include <jconnection-pool-internal.h>
#include <jdistribution.h>
#include <jdistribution-internal.h>
#include <jhelper-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jlock-internal.h>
#include <jmessage-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

/**
 * \defgroup JObject Object
 *
 * Data structures and functions for managing objects.
 *
 * @{
 **/

/**
 * Data for background operations.
 */
struct JObjectBackgroundData
{
	/**
	 * The message to send.
	 */
	JMessage* message;

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
			 * Contains #JObjectReadData elements.
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
			 * Contains #JObjectReadStatusData elements.
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
	};
};

typedef struct JObjectBackgroundData JObjectBackgroundData;

struct JObjectOperation
{
	union
	{
		struct
		{
			JObject* object;
			gint64* modification_time;
			guint64* size;
		}
		get_status;

		struct
		{
			JObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		}
		read;

		struct
		{
			JObject* object;
			gconstpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		}
		write;
	};
};

typedef struct JObjectOperation JObjectOperation;

/**
 * Data for buffers.
 */
struct JObjectReadData
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

typedef struct JObjectReadData JObjectReadData;

/**
 * Data for buffers.
 */
struct JObjectReadStatusData
{
	/**
	 * The item.
	 */
	JObject* item;

	guint64* sizes;
};

typedef struct JObjectReadStatusData JObjectReadStatusData;

/**
 * A JObject.
 **/
struct JObject
{
	/**
	 * The namespace.
	 **/
	gchar* namespace;

	/**
	 * The name.
	 **/
	gchar* name;

	/**
	 * The data server index.
	 */
	guint32 index;

	JDistribution* distribution;

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
	 * The reference count.
	 **/
	gint ref_count;
};

static
void
j_object_create_free (gpointer data)
{
	JObject* object = data;

	j_object_unref(object);
}

static
void
j_object_delete_free (gpointer data)
{
	JObject* object = data;

	j_object_unref(object);
}

static
void
j_object_get_status_free (gpointer data)
{
	JObjectOperation* operation = data;

	j_object_unref(operation->get_status.object);

	g_slice_free(JObjectOperation, operation);
}

static
void
j_object_read_free (gpointer data)
{
	JObjectOperation* operation = data;

	j_object_unref(operation->read.object);

	g_slice_free(JObjectOperation, operation);
}

static
void
j_object_write_free (gpointer data)
{
	JObjectOperation* operation = data;

	j_object_unref(operation->write.object);

	g_slice_free(JObjectOperation, operation);
}

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
j_object_read_background_operation (gpointer data)
{
	JObjectBackgroundData* background_data = data;
	JListIterator* iterator;
	JMessage* reply;
	GSocketConnection* connection;
	guint operations_done;
	guint32 operation_count;
	guint64 nbytes;

	connection = j_connection_pool_pop_data(background_data->index);

	j_message_send(background_data->message, connection);

	reply = j_message_new_reply(background_data->message);
	iterator = j_list_iterator_new(background_data->read.buffer_list);

	operations_done = 0;
	operation_count = j_message_get_count(background_data->message);

	while (operations_done < operation_count)
	{
		guint32 reply_operation_count;

		j_message_receive(reply, connection);

		reply_operation_count = j_message_get_count(reply);

		for (guint j = 0; j < reply_operation_count && j_list_iterator_next(iterator); j++)
		{
			JObjectReadData* buffer = j_list_iterator_get(iterator);

			nbytes = j_message_get_8(reply);
			// FIXME thread-safety
			*(buffer->nbytes) += nbytes;

			if (nbytes > 0)
			{
				GInputStream* input;

				input = g_io_stream_get_input_stream(G_IO_STREAM(connection));
				g_input_stream_read_all(input, buffer->data, nbytes, NULL, NULL, NULL);
			}

			g_slice_free(JObjectReadData, buffer);
		}

		operations_done += reply_operation_count;
	}

	j_list_iterator_free(iterator);
	j_message_unref(reply);

	j_message_unref(background_data->message);
	j_list_unref(background_data->read.buffer_list);

	j_connection_pool_push_data(background_data->index, connection);

	g_slice_free(JObjectBackgroundData, background_data);

	/*
	if (nbytes < new_length)
	{
		break;
	}
	*/

	return NULL;
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
j_object_write_background_operation (gpointer data)
{
	JObjectBackgroundData* background_data = data;
	JMessage* reply;
	GSocketConnection* connection;

	connection = j_connection_pool_pop_data(background_data->index);

	if (background_data->write.create_message != NULL)
	{
		j_message_send(background_data->write.create_message, connection);

		/* This will always be true, see j_object_write_exec(). */
		if (j_message_get_type_modifier(background_data->write.create_message) & J_MESSAGE_SAFETY_NETWORK)
		{
			reply = j_message_new_reply(background_data->write.create_message);
			j_message_receive(reply, connection);
			j_message_unref(reply);
		}

		j_message_unref(background_data->write.create_message);
	}

	j_message_send(background_data->message, connection);

	if (j_message_get_type_modifier(background_data->message) & J_MESSAGE_SAFETY_NETWORK)
	{
		/* guint64 nbytes; */

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, connection);

		/* FIXME do something with nbytes */
		/* nbytes = j_message_get_8(reply); */

		j_message_unref(reply);
	}

	j_connection_pool_push_data(background_data->index, connection);

	j_message_unref(background_data->message);

	g_slice_free(JObjectBackgroundData, background_data);

	return NULL;
}

static
gboolean
j_object_create_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage* message;
	GSocketConnection* data_connection;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend == NULL)
	{
		data_connection = j_connection_pool_pop_data(index);
		message = j_message_new(J_MESSAGE_DATA_CREATE, namespace_len);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JObject* object = j_list_iterator_get(it);

		if (data_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_data_create(data_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_data_close(data_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->name, name_len);
		}
	}

	if (data_backend == NULL)
	{
		j_message_send(message, data_connection);
		j_message_unref(message);
		j_connection_pool_push_data(index, data_connection);
	}

	j_list_iterator_free(it);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_object_delete_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage* message;
	GSocketConnection* data_connection;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend == NULL)
	{
		data_connection = j_connection_pool_pop_data(index);
		message = j_message_new(J_MESSAGE_DATA_DELETE, namespace_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JObject* object = j_list_iterator_get(it);

		if (data_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_data_open(data_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_data_delete(data_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->name, name_len);
		}
	}

	if (data_backend == NULL)
	{
		j_message_send(message, data_connection);

		if (j_message_get_type_modifier(message) & J_MESSAGE_SAFETY_NETWORK)
		{
			JMessage* reply;

			reply = j_message_new_reply(message);
			j_message_receive(reply, data_connection);

			/* FIXME do something with reply */
			j_message_unref(reply);
		}

		j_message_unref(message);
		j_connection_pool_push_data(index, data_connection);
	}

	j_list_iterator_free(it);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_object_read_exec (JList* operations, JSemantics* semantics)
{
	JObject* item;
	JList** buffer_list;
	JListIterator* iterator;
	JLock* lock = NULL;
	JMessage** messages;
	guint n;
	gchar const* item_name;
	gchar* path;
	gpointer* background_data;
	gsize path_len;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	n = j_configuration_get_data_server_count(j_configuration());
	background_data = g_new(gpointer, n);
	messages = g_new(JMessage*, n);
	buffer_list = g_new(JList*, n);

	for (guint i = 0; i < n; i++)
	{
		messages[i] = NULL;
		buffer_list[i] = j_list_new(NULL);
	}

	{
		JObjectOperation* operation;

		operation = j_list_get_first(operations);
		g_assert(operation != NULL);
		item = operation->read.object;

		item_name = item->name;

		path = g_build_path("/", item_name, NULL);
		path_len = strlen(path) + 1;
	}

	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}

	iterator = j_list_iterator_new(operations);

	while (j_list_iterator_next(iterator))
	{
		JObjectOperation* operation = j_list_iterator_get(iterator);
		gpointer data = operation->read.data;
		guint64 length = operation->read.length;
		guint64 offset = operation->read.offset;
		guint64* bytes_read = operation->read.bytes_read;

		JObjectReadData* buffer;
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

		j_distribution_reset(item->distribution, length, offset);
		d = data;

		while (j_distribution_distribute(item->distribution, &index, &new_length, &new_offset, &block_id))
		{
			if (messages[index] == NULL)
			{
				messages[index] = j_message_new(J_MESSAGE_DATA_READ, 5 + path_len);
				j_message_append_n(messages[index], "item", 5);
				j_message_append_n(messages[index], path, path_len);
			}

			j_message_add_operation(messages[index], sizeof(guint64) + sizeof(guint64));
			j_message_append_8(messages[index], &new_length);
			j_message_append_8(messages[index], &new_offset);

			buffer = g_slice_new(JObjectReadData);
			buffer->data = d;
			buffer->nbytes = bytes_read;

			j_list_append(buffer_list[index], buffer);

			if (lock != NULL)
			{
				j_lock_add(lock, block_id);
			}

			d += new_length;
		}

		j_trace_file_end(item_name, J_TRACE_FILE_READ, length, offset);
	}

	j_list_iterator_free(iterator);

	g_free(path);

	if (lock != NULL)
	{
		/* FIXME busy wait */
		while (!j_lock_acquire(lock));
	}

	for (guint i = 0; i < n; i++)
	{
		JObjectBackgroundData* data;

		if (messages[i] == NULL)
		{
			background_data[i] = NULL;
			continue;
		}

		data = g_slice_new(JObjectBackgroundData);
		data->message = messages[i];
		data->index = i;
		data->read.buffer_list = buffer_list[i];

		background_data[i] = data;
	}

	j_helper_execute_parallel(j_object_read_background_operation, background_data, n);

	if (lock != NULL)
	{
		j_lock_free(lock);
	}

	g_free(background_data);
	g_free(messages);
	g_free(buffer_list);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
j_object_write_exec (JList* operations, JSemantics* semantics)
{
	JObject* item;
	JListIterator* iterator;
	JLock* lock = NULL;
	JMessage** messages;
	guint n;
	gchar const* item_name;
	guint64 max_offset = 0;
	gchar* path;
	gpointer* background_data;
	gsize path_len;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	n = j_configuration_get_data_server_count(j_configuration());
	background_data = g_new(gpointer, n);
	messages = g_new(JMessage*, n);

	for (guint i = 0; i < n; i++)
	{
		messages[i] = NULL;
	}

	{
		JObjectOperation* operation;

		operation = j_list_get_first(operations);
		g_assert(operation != NULL);
		item = operation->write.object;

		item_name = item->name;

		path = g_build_path("/", item_name, NULL);
		path_len = strlen(path) + 1;
	}

	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}

	iterator = j_list_iterator_new(operations);

	while (j_list_iterator_next(iterator))
	{
		JObjectOperation* operation = j_list_iterator_get(iterator);
		gconstpointer data = operation->write.data;
		guint64 length = operation->write.length;
		guint64 offset = operation->write.offset;
		guint64* bytes_written = operation->write.bytes_written;

		gchar const* d;
		guint64 new_length;
		guint64 new_offset;
		guint64 block_id;
		guint index;

		if (length == 0)
		{
			continue;
		}

		max_offset = MAX(max_offset, offset + length);

		j_trace_file_begin(item_name, J_TRACE_FILE_WRITE);

		j_distribution_reset(item->distribution, length, offset);
		d = data;

		while (j_distribution_distribute(item->distribution, &index, &new_length, &new_offset, &block_id))
		{
			if (messages[index] == NULL)
			{
				/* FIXME */
				messages[index] = j_message_new(J_MESSAGE_DATA_WRITE, 5 + path_len);
				j_message_set_safety(messages[index], semantics);
				j_message_append_n(messages[index], "item", 5);
				j_message_append_n(messages[index], path, path_len);
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
			if (bytes_written != NULL)
			{
				*bytes_written += new_length;
			}
		}

		j_trace_file_end(item_name, J_TRACE_FILE_WRITE, length, offset);
	}

	j_list_iterator_free(iterator);

	if (lock != NULL)
	{
		/* FIXME busy wait */
		while (!j_lock_acquire(lock));
	}

	for (guint i = 0; i < n; i++)
	{
		JObjectBackgroundData* data;
		JMessage* create_message = NULL;

		if (messages[i] == NULL)
		{
			background_data[i] = NULL;
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
			create_message = j_message_new(J_MESSAGE_DATA_CREATE, 5);
			/**
			 * Force safe semantics to make the server send a reply.
			 * Otherwise, nasty races can occur when using unsafe semantics:
			 * - The client creates the item and sends its first write.
			 * - The client sends another operation using another connection from the pool.
			 * - The second operation is executed first and fails because the item does not exist.
			 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
			 **/
			j_message_force_safety(create_message, J_SEMANTICS_SAFETY_NETWORK);
			j_message_append_n(create_message, "item", 5);
			j_message_add_operation(create_message, path_len);
			j_message_append_n(create_message, path, path_len);

			item->status.created[i] = TRUE;
		}

		data = g_slice_new(JObjectBackgroundData);
		data->message = messages[i];
		data->index = i;
		data->write.create_message = create_message;

		background_data[i] = data;
	}

	j_helper_execute_parallel(j_object_write_background_operation, background_data, n);

	if (lock != NULL)
	{
		j_lock_free(lock);
	}

	g_free(path);

	g_free(background_data);
	g_free(messages);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
j_object_get_status_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage* message;
	GSocketConnection* data_connection;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JObjectOperation* operation = j_list_get_first(operations);
		JObject* object = operation->get_status.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend == NULL)
	{
		data_connection = j_connection_pool_pop_data(index);
		message = j_message_new(J_MESSAGE_DATA_STATUS, namespace_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JObjectOperation* operation = j_list_iterator_get(it);
		JObject* object = operation->get_status.object;
		gint64* modification_time = operation->get_status.modification_time;
		guint64* size = operation->get_status.size;

		if (data_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_data_open(data_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_data_status(data_backend, object_handle, modification_time, size) && ret;
			ret = j_backend_data_close(data_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->name, name_len);
		}
	}

	j_list_iterator_free(it);

	if (data_backend == NULL)
	{
		JMessage* reply;

		j_message_send(message, data_connection);

		reply = j_message_new_reply(message);
		j_message_receive(reply, data_connection);

		it = j_list_iterator_new(operations);

		while (j_list_iterator_next(it))
		{
			JObjectOperation* operation = j_list_iterator_get(it);
			gint64* modification_time = operation->get_status.modification_time;
			guint64* size = operation->get_status.size;
			gint64 modification_time_;
			guint64 size_;

			modification_time_ = j_message_get_8(reply);
			size_ = j_message_get_8(reply);

			*modification_time = modification_time_;
			*size = size_;
		}

		j_list_iterator_free(it);

		j_message_unref(reply);
		j_message_unref(message);

		j_connection_pool_push_data(index, data_connection);
	}

	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Creates a new item.
 *
 * \author Michael Kuhn
 *
 * \code
 * JObject* i;
 *
 * i = j_object_new("JULEA");
 * \endcode
 *
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_object_unref().
 **/
static
JObject*
j_object_new (guint32 index, gchar const* namespace, gchar const* name)
{
	JObject* object = NULL;

	JConfiguration* configuration = j_configuration();

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_data_server_count(configuration), NULL);

	j_trace_enter(G_STRFUNC, NULL);

	object = g_slice_new(JObject);
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->index = index;
	object->status.age = g_get_real_time();
	object->status.size = 0;
	object->status.modification_time = g_get_real_time();
	object->status.created = NULL;
	object->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return object;
}

/**
 * Increases an item's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JObject* i;
 *
 * j_object_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JObject*
j_object_ref (JObject* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

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
j_object_unref (JObject* item)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
		j_distribution_unref(item->distribution);

		g_free(item->status.created);
		g_free(item->name);

		g_slice_free(JObject, item);
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
j_object_get_name (JObject* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);
	j_trace_leave(G_STRFUNC);

	return item->name;
}

/**
 * Creates an object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param name         A name.
 * \param distribution A distribution.
 * \param batch        A batch.
 *
 * \return A new item. Should be freed with j_object_unref().
 **/
JObject*
j_object_create (guint32 index, gchar const* namespace, gchar const* name, JBatch* batch)
{
	JObject* object;
	JOperation* operation;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if ((object = j_object_new(index, namespace, name)) == NULL)
	{
		goto end;
	}

	operation = j_operation_new();
	// FIXME key = index + namespace
	operation->key = object;
	operation->data = j_object_ref(object);
	operation->exec_func = j_object_create_exec;
	operation->free_func = j_object_create_free;

	j_batch_add(batch, operation);

end:
	j_trace_leave(G_STRFUNC);

	return object;
}

/**
 * Deletes an object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item       An item.
 * \param batch      A batch.
 **/
void
j_object_delete (guint32 index, gchar const* namespace, gchar const* name, JBatch* batch)
{
	JObject* object;
	JOperation* operation;

	g_return_if_fail(namespace != NULL);
	g_return_if_fail(name != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if ((object = j_object_new(index, namespace, name)) == NULL)
	{
		goto end;
	}

	operation = j_operation_new();
	operation->key = object;
	operation->data = j_object_ref(object);
	operation->exec_func = j_object_delete_exec;
	operation->free_func = j_object_delete_free;

	j_batch_add(batch, operation);

end:
	j_trace_leave(G_STRFUNC);
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
j_object_read (JObject* item, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	JObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_read != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JObjectOperation);
	iop->read.object = j_object_ref(item);
	iop->read.data = data;
	iop->read.length = length;
	iop->read.offset = offset;
	iop->read.bytes_read = bytes_read;

	operation = j_operation_new();
	operation->key = item;
	operation->data = iop;
	operation->exec_func = j_object_read_exec;
	operation->free_func = j_object_read_free;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * Writes an item.
 *
 * \note
 * j_object_write() modifies bytes_written even if j_batch_execute() is not called.
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
j_object_write (JObject* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	JObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_written != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JObjectOperation);
	iop->write.object = j_object_ref(item);
	iop->write.data = data;
	iop->write.length = length;
	iop->write.offset = offset;
	iop->write.bytes_written = bytes_written;

	operation = j_operation_new();
	operation->key = item;
	operation->data = iop;
	operation->exec_func = j_object_write_exec;
	operation->free_func = j_object_write_free;

	*bytes_written = 0;

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
 * \param batch     A batch.
 **/
void
j_object_get_status (guint32 index, gchar const* namespace, gchar const* name, gint64* modification_time, guint64* size, JBatch* batch)
{
	JObject* object;
	JObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(namespace != NULL);
	g_return_if_fail(name != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if ((object = j_object_new(index, namespace, name)) == NULL)
	{
		goto end;
	}

	iop = g_slice_new(JObjectOperation);
	iop->get_status.object = j_object_ref(object);
	iop->get_status.modification_time = modification_time;
	iop->get_status.size = size;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_object_get_status_exec;
	operation->free_func = j_object_get_status_free;

	j_batch_add(batch, operation);

end:
	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
