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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <object/jdistributed-object.h>

#include <object/jobject-internal.h>

#include <julea.h>

/**
 * \addtogroup JDistributedObject
 *
 * @{
 **/

/**
 * Data for background operations.
 */
struct JDistributedObjectBackgroundData
{
	guint32 index;
	JMessage* message;
	JList* operations;
	JSemantics* semantics;
	gboolean ret;

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
			 * Contains #JDistributedObjectReadBuffer elements.
			 */
			JList* buffers;
		} read;

		/**
		 * The write part.
		 */
		struct
		{
			JList* bytes_written;
		} write;
	};
};

typedef struct JDistributedObjectBackgroundData JDistributedObjectBackgroundData;

struct JDistributedObjectReadBuffer
{
	gchar* data;
	guint64* bytes_read;
};

typedef struct JDistributedObjectReadBuffer JDistributedObjectReadBuffer;

struct JDistributedObjectOperation
{
	union
	{
		struct
		{
			JDistributedObject* object;
			gint64* modification_time;
			guint64* size;
		} status;

		struct
		{
			JDistributedObject* object;
		} sync;

		struct
		{
			JDistributedObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		} read;

		struct
		{
			JDistributedObject* object;
			gconstpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		} write;
	};
};

typedef struct JDistributedObjectOperation JDistributedObjectOperation;

/**
 * A JDistributedObject.
 **/
struct JDistributedObject
{
	/**
	 * The namespace.
	 **/
	gchar* namespace;

	/**
	 * The name.
	 **/
	gchar* name;

	JDistribution* distribution;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

static void
j_distributed_object_create_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObject* object = data;

	j_distributed_object_unref(object);
}

static void
j_distributed_object_delete_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObject* object = data;

	j_distributed_object_unref(object);
}

static void
j_distributed_object_status_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->status.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

static void
j_distributed_object_sync_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->sync.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

static void
j_distributed_object_read_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->read.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

static void
j_distributed_object_write_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->write.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

/**
 * Executes create operations in a background operation.
 *
 * \private
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static gpointer
j_distributed_object_create_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectBackgroundData* background_data = data;

	JSemanticsSafety safety;

	gpointer object_connection;

	safety = j_semantics_get(background_data->semantics, J_SEMANTICS_SAFETY);
	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, background_data->index);

	j_message_send(background_data->message, object_connection);

	if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
	{
		g_autoptr(JMessage) reply = NULL;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, object_connection);

		/// \todo do something with reply
	}

	j_message_unref(background_data->message);
	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, background_data->index, object_connection);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

/**
 * Executes create operations in a background operation.
 *
 * \private
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static gpointer
j_distributed_object_delete_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectBackgroundData* background_data = data;

	JSemanticsSafety safety;

	gpointer object_connection;

	safety = j_semantics_get(background_data->semantics, J_SEMANTICS_SAFETY);
	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, background_data->index);

	j_message_send(background_data->message, object_connection);

	if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
	{
		g_autoptr(JMessage) reply = NULL;
		guint32 operation_count;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, object_connection);

		operation_count = j_message_get_count(reply);

		for (guint i = 0; i < operation_count; i++)
		{
			guint32 status;

			status = j_message_get_4(reply);
			background_data->ret = (status == 1) && background_data->ret;
		}
	}

	j_message_unref(background_data->message);
	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, background_data->index, object_connection);

	return data;
}

/**
 * Executes write operations in a background operation.
 *
 * \private
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static gpointer
j_distributed_object_read_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectBackgroundData* background_data = data;

	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) reply = NULL;
	gpointer object_connection;
	guint32 operations_done;
	guint32 operation_count;

	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, background_data->index);
	j_message_send(background_data->message, object_connection);

	reply = j_message_new_reply(background_data->message);

	operations_done = 0;
	operation_count = j_message_get_count(background_data->message);

	it = j_list_iterator_new(background_data->read.buffers);

	/**
	 * This extra loop is necessary because the server might send multiple
	 * replies per message. The same reply object can be used to receive
	 * multiple times.
	 */
	while (operations_done < operation_count)
	{
		guint32 reply_operation_count;

		j_message_receive(reply, object_connection);

		reply_operation_count = j_message_get_count(reply);

		for (guint i = 0; i < reply_operation_count && j_list_iterator_next(it); i++)
		{
			JDistributedObjectReadBuffer* buffer = j_list_iterator_get(it);
			gchar* read_data = buffer->data;
			guint64* bytes_read = buffer->bytes_read;

			guint64 nbytes;

			nbytes = j_message_get_8(reply);
			j_helper_atomic_add(bytes_read, nbytes);

			if (nbytes > 0)
			{
				GInputStream* input;

				input = g_io_stream_get_input_stream(G_IO_STREAM(object_connection));
				g_input_stream_read_all(input, read_data, nbytes, NULL, NULL, NULL);
			}

			g_slice_free(JDistributedObjectReadBuffer, buffer);
		}

		operations_done += reply_operation_count;
	}

	j_message_unref(background_data->message);

	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, background_data->index, object_connection);

	j_list_unref(background_data->read.buffers);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

/**
 * Executes write operations in a background operation.
 *
 * \private
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static gpointer
j_distributed_object_write_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectBackgroundData* background_data = data;

	JSemanticsSafety safety;

	gpointer object_connection;

	safety = j_semantics_get(background_data->semantics, J_SEMANTICS_SAFETY);
	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, background_data->index);
	j_message_send(background_data->message, object_connection);

	if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
	{
		g_autoptr(JListIterator) it = NULL;
		g_autoptr(JMessage) reply = NULL;
		guint64 nbytes;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, object_connection);

		it = j_list_iterator_new(background_data->write.bytes_written);

		while (j_list_iterator_next(it))
		{
			guint64* bytes_written = j_list_iterator_get(it);

			nbytes = j_message_get_8(reply);
			j_helper_atomic_add(bytes_written, nbytes);
		}
	}

	j_message_unref(background_data->message);

	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, background_data->index, object_connection);

	j_list_unref(background_data->write.bytes_written);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

/**
 * Executes status operations in a background operation.
 *
 * \private
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static gpointer
j_distributed_object_status_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectBackgroundData* background_data = data;

	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) reply = NULL;
	gpointer object_connection;

	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, background_data->index);
	j_message_send(background_data->message, object_connection);

	reply = j_message_new_reply(background_data->message);
	j_message_receive(reply, object_connection);

	it = j_list_iterator_new(background_data->operations);

	while (j_list_iterator_next(it))
	{
		JDistributedObjectOperation* operation = j_list_iterator_get(it);
		gint64* modification_time = operation->status.modification_time;
		guint64* size = operation->status.size;
		gint64 modification_time_;
		guint64 size_;

		modification_time_ = j_message_get_8(reply);
		size_ = j_message_get_8(reply);

		if (modification_time != NULL)
		{
			/// \todo max?
			*modification_time = modification_time_;
		}

		if (size != NULL)
		{
			j_helper_atomic_add(size, size_);
		}
	}

	j_message_unref(background_data->message);

	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, background_data->index, object_connection);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

/**
 * Executes sync operations in a background operation.
 *
 * \private
 *
 * \param data Background data.
 *
 * \return #data.
 **/
static gpointer
j_distributed_object_sync_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectBackgroundData* background_data = data;

	JSemanticsSafety safety;
	gpointer object_connection;

	safety = j_semantics_get(background_data->semantics, J_SEMANTICS_SAFETY);
	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, background_data->index);
	j_message_send(background_data->message, object_connection);

	if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
	{
		g_autoptr(JMessage) reply = NULL;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, object_connection);

		/// \todo do something with reply
	}

	j_message_unref(background_data->message);
	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, background_data->index, object_connection);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

static gboolean
j_distributed_object_create_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	/// \todo check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autofree JMessage** messages = NULL;
	gchar const* namespace = NULL;
	gsize namespace_len = 0;
	guint32 server_count = 0;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JDistributedObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);
		messages = g_new(JMessage*, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			/**
			 * Force safe semantics to make the server send a reply.
			 * Otherwise, nasty races can occur when using unsafe semantics:
			 * - The client creates the object and sends its first write.
			 * - The client sends another operation using another connection from the pool.
			 * - The second operation is executed first and fails because the object does not exist.
			 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
			 **/
			messages[i] = j_message_new(J_MESSAGE_OBJECT_CREATE, namespace_len);
			j_message_set_semantics(messages[i], semantics);
			j_message_append_n(messages[i], namespace, namespace_len);
		}
	}

	while (j_list_iterator_next(it))
	{
		JDistributedObject* object = j_list_iterator_get(it);

		if (object_backend == NULL)
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			/// \todo use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
		else
		{
			gpointer object_handle;

			ret = j_backend_object_create(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_close(object_backend, object_handle) && ret;
		}
	}

	if (object_backend == NULL)
	{
		g_autofree gpointer* background_data = NULL;

		background_data = g_new(gpointer, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = NULL;
			data->semantics = semantics;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_create_background_operation, background_data, server_count);
	}

	return ret;
}

static gboolean
j_distributed_object_delete_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autofree JMessage** messages = NULL;
	gchar const* namespace = NULL;
	gsize namespace_len = 0;
	guint32 server_count = 0;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JDistributedObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);
		messages = g_new(JMessage*, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = j_message_new(J_MESSAGE_OBJECT_DELETE, namespace_len);
			j_message_set_semantics(messages[i], semantics);
			j_message_append_n(messages[i], namespace, namespace_len);
		}
	}

	while (j_list_iterator_next(it))
	{
		JDistributedObject* object = j_list_iterator_get(it);

		if (object_backend == NULL)
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			/// \todo use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
		else
		{
			gboolean lret = FALSE;
			gpointer object_handle;

			if (j_backend_object_open(object_backend, object->namespace, object->name, &object_handle)
			    && j_backend_object_delete(object_backend, object_handle))
			{
				lret = TRUE;
			}

			ret = lret && ret;
		}
	}

	if (object_backend == NULL)
	{
		g_autofree gpointer* background_data = NULL;

		background_data = g_new(gpointer, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = NULL;
			data->semantics = semantics;
			data->ret = TRUE;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_delete_background_operation, background_data, server_count);

		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data = background_data[i];

			ret = data->ret && ret;

			g_slice_free(JDistributedObjectBackgroundData, data);
		}
	}

	return ret;
}

static gboolean
j_distributed_object_read_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	/// \todo check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autofree JList** br_lists = NULL;
	g_autoptr(JListIterator) it = NULL;
	g_autofree JMessage** messages = NULL;
	JDistributedObject* object = NULL;
	gpointer object_handle;
	gsize name_len = 0;
	gsize namespace_len = 0;
	guint32 server_count = 0;

	/// \todo
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);
		g_assert(operation != NULL);

		object = operation->read.object;
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);
		messages = g_new(JMessage*, server_count);
		br_lists = g_new(JList*, server_count);

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = NULL;
			br_lists[i] = NULL;
		}
	}
	else
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("object", path);
	}
	*/

	while (j_list_iterator_next(it))
	{
		JDistributedObjectOperation* operation = j_list_iterator_get(it);
		gpointer data = operation->read.data;
		guint64 length = operation->read.length;
		guint64 offset = operation->read.offset;
		guint64* bytes_read = operation->read.bytes_read;

		j_trace_file_begin(object->name, J_TRACE_FILE_READ);

		if (object_backend == NULL)
		{
			gchar* new_data;
			guint32 index;
			guint64 block_id;
			guint64 new_length;
			guint64 new_offset;

			j_distribution_reset(object->distribution, length, offset);
			new_data = data;

			while (j_distribution_distribute(object->distribution, &index, &new_length, &new_offset, &block_id))
			{
				JDistributedObjectReadBuffer* buffer;

				if (messages[index] == NULL && br_lists[index] == NULL)
				{
					messages[index] = j_message_new(J_MESSAGE_OBJECT_READ, namespace_len + name_len);
					j_message_set_semantics(messages[index], semantics);
					j_message_append_n(messages[index], object->namespace, namespace_len);
					j_message_append_n(messages[index], object->name, name_len);

					br_lists[index] = j_list_new(NULL);
				}

				j_message_add_operation(messages[index], sizeof(guint64) + sizeof(guint64));
				j_message_append_8(messages[index], &new_length);
				j_message_append_8(messages[index], &new_offset);

				buffer = g_slice_new(JDistributedObjectReadBuffer);
				buffer->data = new_data;
				buffer->bytes_read = bytes_read;

				j_list_append(br_lists[index], buffer);

				/*
				if (lock != NULL)
				{
					j_lock_add(lock, block_id);
				}
				*/

				new_data += new_length;
			}
		}
		else
		{
			guint64 nbytes = 0;

			ret = j_backend_object_read(object_backend, object_handle, data, length, offset, &nbytes) && ret;
			j_helper_atomic_add(bytes_read, nbytes);
		}

		j_trace_file_end(object->name, J_TRACE_FILE_READ, length, offset);
	}

	if (object_backend == NULL)
	{
		g_autofree gpointer* background_data = NULL;

		background_data = g_new(gpointer, server_count);

		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			if (messages[i] == NULL)
			{
				background_data[i] = NULL;
				continue;
			}

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = NULL;
			data->semantics = semantics;
			data->read.buffers = br_lists[i];

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_read_background_operation, background_data, server_count);
	}
	else
	{
		ret = j_backend_object_close(object_backend, object_handle) && ret;
	}

	/*
	if (lock != NULL)
	{
		/// \todo busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	return ret;
}

static gboolean
j_distributed_object_write_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	/// \todo check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autofree JList** bw_lists = NULL;
	g_autoptr(JListIterator) it = NULL;
	g_autofree JMessage** messages = NULL;
	JDistributedObject* object = NULL;
	gpointer object_handle;
	gsize name_len = 0;
	gsize namespace_len = 0;
	guint32 server_count = 0;

	/// \todo
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);
		g_assert(operation != NULL);

		object = operation->write.object;
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);
		messages = g_new(JMessage*, server_count);
		bw_lists = g_new(JList*, server_count);

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = NULL;
			bw_lists[i] = NULL;
		}
	}
	else
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("object", path);
	}
	*/

	while (j_list_iterator_next(it))
	{
		JDistributedObjectOperation* operation = j_list_iterator_get(it);
		gconstpointer data = operation->write.data;
		guint64 length = operation->write.length;
		guint64 offset = operation->write.offset;
		guint64* bytes_written = operation->write.bytes_written;

		j_trace_file_begin(object->name, J_TRACE_FILE_WRITE);

		if (object_backend == NULL)
		{
			gchar const* new_data;
			guint32 index;
			guint64 block_id;
			guint64 new_length;
			guint64 new_offset;

			j_distribution_reset(object->distribution, length, offset);
			new_data = data;

			while (j_distribution_distribute(object->distribution, &index, &new_length, &new_offset, &block_id))
			{
				if (messages[index] == NULL && bw_lists[index] == NULL)
				{
					messages[index] = j_message_new(J_MESSAGE_OBJECT_WRITE, namespace_len + name_len);
					j_message_set_semantics(messages[index], semantics);
					j_message_append_n(messages[index], object->namespace, namespace_len);
					j_message_append_n(messages[index], object->name, name_len);

					bw_lists[index] = j_list_new(NULL);
				}

				j_message_add_operation(messages[index], sizeof(guint64) + sizeof(guint64));
				j_message_append_8(messages[index], &new_length);
				j_message_append_8(messages[index], &new_offset);
				j_message_add_send(messages[index], new_data, new_length);

				j_list_append(bw_lists[index], bytes_written);

				/*
				if (lock != NULL)
				{
					j_lock_add(lock, block_id);
				}
				*/

				new_data += new_length;

				// Fake bytes_written here instead of doing another loop further down
				if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_NONE)
				{
					j_helper_atomic_add(bytes_written, new_length);
				}
			}
		}
		else
		{
			guint64 nbytes = 0;

			ret = j_backend_object_write(object_backend, object_handle, data, length, offset, &nbytes) && ret;
			j_helper_atomic_add(bytes_written, nbytes);
		}

		j_trace_file_end(object->name, J_TRACE_FILE_WRITE, length, offset);
	}

	if (object_backend == NULL)
	{
		g_autofree gpointer* background_data = NULL;

		background_data = g_new(gpointer, server_count);

		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			if (messages[i] == NULL)
			{
				background_data[i] = NULL;
				continue;
			}

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = NULL;
			data->semantics = semantics;
			data->write.bytes_written = bw_lists[i];

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_write_background_operation, background_data, server_count);
	}
	else
	{
		ret = j_backend_object_close(object_backend, object_handle) && ret;
	}

	/*
	if (lock != NULL)
	{
		/// \todo busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	return ret;
}

static gboolean
j_distributed_object_status_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	/// \todo check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autofree JMessage** messages = NULL;
	gchar const* namespace = NULL;
	gsize namespace_len = 0;
	guint32 server_count = 0;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);
		JDistributedObject* object = operation->status.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);
		messages = g_new(JMessage*, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = j_message_new(J_MESSAGE_OBJECT_STATUS, namespace_len);
			j_message_set_semantics(messages[i], semantics);
			j_message_append_n(messages[i], namespace, namespace_len);
		}
	}

	while (j_list_iterator_next(it))
	{
		JDistributedObjectOperation* operation = j_list_iterator_get(it);
		JDistributedObject* object = operation->status.object;
		gint64* modification_time = operation->status.modification_time;
		guint64* size = operation->status.size;

		if (modification_time != NULL)
		{
			*modification_time = 0;
		}

		if (size != NULL)
		{
			*size = 0;
		}

		if (object_backend == NULL)
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			/// \todo use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
		else
		{
			gpointer object_handle;

			ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_status(object_backend, object_handle, modification_time, size) && ret;
			ret = j_backend_object_close(object_backend, object_handle) && ret;
		}
	}

	if (object_backend == NULL)
	{
		g_autofree gpointer* background_data = NULL;

		background_data = g_new(gpointer, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = operations;
			data->semantics = semantics;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_status_background_operation, background_data, server_count);
	}

	return ret;
}

static gboolean
j_distributed_object_sync_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	/// \todo check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autofree JMessage** messages = NULL;
	gchar const* namespace = NULL;
	gsize namespace_len = 0;
	guint32 server_count = 0;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);
		JDistributedObject* object = operation->sync.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);
		messages = g_new(JMessage*, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = j_message_new(J_MESSAGE_OBJECT_SYNC, namespace_len);
			j_message_set_semantics(messages[i], semantics);
			j_message_append_n(messages[i], namespace, namespace_len);
		}
	}

	while (j_list_iterator_next(it))
	{
		JDistributedObjectOperation* operation = j_list_iterator_get(it);
		JDistributedObject* object = operation->sync.object;

		if (object_backend == NULL)
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			/// \todo use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
		else
		{
			gpointer object_handle;

			ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_sync(object_backend, object_handle) && ret;
			ret = j_backend_object_close(object_backend, object_handle) && ret;
		}
	}

	if (object_backend == NULL)
	{
		g_autofree gpointer* background_data = NULL;

		background_data = g_new(gpointer, server_count);

		/// \todo use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = operations;
			data->semantics = semantics;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_sync_background_operation, background_data, server_count);
	}

	return ret;
}

JDistributedObject*
j_distributed_object_new(gchar const* namespace, gchar const* name, JDistribution* distribution)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObject* object = NULL;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(distribution != NULL, NULL);

	object = g_slice_new(JDistributedObject);
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->distribution = j_distribution_ref(distribution);
	object->ref_count = 1;

	return object;
}

JDistributedObject*
j_distributed_object_ref(JDistributedObject* object)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(object != NULL, NULL);

	g_atomic_int_inc(&(object->ref_count));

	return object;
}

void
j_distributed_object_unref(JDistributedObject* object)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(object != NULL);

	if (g_atomic_int_dec_and_test(&(object->ref_count)))
	{
		g_free(object->name);
		g_free(object->namespace);

		j_distribution_unref(object->distribution);

		g_slice_free(JDistributedObject, object);
	}
}

void
j_distributed_object_create(JDistributedObject* object, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;

	g_return_if_fail(object != NULL);

	operation = j_operation_new();
	/// \todo key = index + namespace
	operation->key = object;
	operation->data = j_distributed_object_ref(object);
	operation->exec_func = j_distributed_object_create_exec;
	operation->free_func = j_distributed_object_create_free;

	j_batch_add(batch, operation);
}

void
j_distributed_object_delete(JDistributedObject* object, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;

	g_return_if_fail(object != NULL);

	operation = j_operation_new();
	operation->key = object;
	operation->data = j_distributed_object_ref(object);
	operation->exec_func = j_distributed_object_delete_exec;
	operation->free_func = j_distributed_object_delete_free;

	j_batch_add(batch, operation);
}

void
j_distributed_object_read(JDistributedObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* iop;
	JOperation* operation;
	guint64 max_operation_size;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);
	g_return_if_fail(bytes_read != NULL);

	max_operation_size = j_configuration_get_max_operation_size(j_configuration());

	// Chunk operation if necessary
	while (length > 0)
	{
		guint64 chunk_size;

		chunk_size = MIN(length, max_operation_size);

		iop = g_slice_new(JDistributedObjectOperation);
		iop->read.object = j_distributed_object_ref(object);
		iop->read.data = data;
		iop->read.length = chunk_size;
		iop->read.offset = offset;
		iop->read.bytes_read = bytes_read;

		operation = j_operation_new();
		operation->key = object;
		operation->data = iop;
		operation->exec_func = j_distributed_object_read_exec;
		operation->free_func = j_distributed_object_read_free;

		j_batch_add(batch, operation);

		data = (gchar*)data + chunk_size;
		length -= chunk_size;
		offset += chunk_size;
	}

	*bytes_read = 0;
}

void
j_distributed_object_write(JDistributedObject* object, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* iop;
	JOperation* operation;
	guint64 max_operation_size;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);
	g_return_if_fail(bytes_written != NULL);

	max_operation_size = j_configuration_get_max_operation_size(j_configuration());

	// Chunk operation if necessary
	while (length > 0)
	{
		guint64 chunk_size;

		chunk_size = MIN(length, max_operation_size);

		iop = g_slice_new(JDistributedObjectOperation);
		iop->write.object = j_distributed_object_ref(object);
		iop->write.data = data;
		iop->write.length = chunk_size;
		iop->write.offset = offset;
		iop->write.bytes_written = bytes_written;

		operation = j_operation_new();
		operation->key = object;
		operation->data = iop;
		operation->exec_func = j_distributed_object_write_exec;
		operation->free_func = j_distributed_object_write_free;

		j_batch_add(batch, operation);

		data = (gchar const*)data + chunk_size;
		length -= chunk_size;
		offset += chunk_size;
	}

	*bytes_written = 0;
}

void
j_distributed_object_status(JDistributedObject* object, gint64* modification_time, guint64* size, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);

	iop = g_slice_new(JDistributedObjectOperation);
	iop->status.object = j_distributed_object_ref(object);
	iop->status.modification_time = modification_time;
	iop->status.size = size;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_distributed_object_status_exec;
	operation->free_func = j_distributed_object_status_free;

	j_batch_add(batch, operation);
}

void
j_distributed_object_sync(JDistributedObject* object, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JDistributedObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);

	iop = g_slice_new(JDistributedObjectOperation);
	iop->sync.object = j_distributed_object_ref(object);

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_distributed_object_sync_exec;
	operation->free_func = j_distributed_object_sync_free;

	j_batch_add(batch, operation);
}

/**
 * @}
 **/
