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

#include <object/jdistributed-object.h>

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
 * \defgroup JDistributedObject Distributed Object
 *
 * Data structures and functions for managing objects.
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
			JList* bytes_written;
		}
		write;
	};
};

typedef struct JDistributedObjectBackgroundData JDistributedObjectBackgroundData;

struct JDistributedObjectOperation
{
	union
	{
		struct
		{
			JDistributedObject* object;
			gint64* modification_time;
			guint64* size;
		}
		status;

		struct
		{
			JDistributedObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		}
		read;

		struct
		{
			JDistributedObject* object;
			gconstpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		}
		write;
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

static
void
j_distributed_object_create_free (gpointer data)
{
	JDistributedObject* object = data;

	j_distributed_object_unref(object);
}

static
void
j_distributed_object_delete_free (gpointer data)
{
	JDistributedObject* object = data;

	j_distributed_object_unref(object);
}

static
void
j_distributed_object_status_free (gpointer data)
{
	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->status.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

static
void
j_distributed_object_read_free (gpointer data)
{
	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->read.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

static
void
j_distributed_object_write_free (gpointer data)
{
	JDistributedObjectOperation* operation = data;

	j_distributed_object_unref(operation->write.object);

	g_slice_free(JDistributedObjectOperation, operation);
}

/**
 * Executes create operations in a background operation.
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
j_distributed_object_create_background_operation (gpointer data)
{
	JDistributedObjectBackgroundData* background_data = data;

	GSocketConnection* data_connection;

	data_connection = j_connection_pool_pop_data(background_data->index);

	j_message_send(background_data->message, data_connection);

	if (j_message_get_type_modifier(background_data->message) & J_MESSAGE_SAFETY_NETWORK)
	{
		JMessage* reply;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, data_connection);

		/* FIXME do something with reply */
		j_message_unref(reply);
	}

	j_message_unref(background_data->message);
	j_connection_pool_push_data(background_data->index, data_connection);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

/**
 * Executes create operations in a background operation.
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
j_distributed_object_delete_background_operation (gpointer data)
{
	JDistributedObjectBackgroundData* background_data = data;

	GSocketConnection* data_connection;

	data_connection = j_connection_pool_pop_data(background_data->index);

	j_message_send(background_data->message, data_connection);

	if (j_message_get_type_modifier(background_data->message) & J_MESSAGE_SAFETY_NETWORK)
	{
		JMessage* reply;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, data_connection);

		/* FIXME do something with reply */
		j_message_unref(reply);
	}

	j_message_unref(background_data->message);
	j_connection_pool_push_data(background_data->index, data_connection);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

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
j_distributed_object_write_background_operation (gpointer data)
{
	JDistributedObjectBackgroundData* background_data = data;

	JListIterator* it;
	GSocketConnection* data_connection;

	data_connection = j_connection_pool_pop_data(background_data->index);
	j_message_send(background_data->message, data_connection);

	if (j_message_get_type_modifier(background_data->message) & J_MESSAGE_SAFETY_NETWORK)
	{
		JMessage* reply;
		guint64 nbytes;

		reply = j_message_new_reply(background_data->message);
		j_message_receive(reply, data_connection);

		it = j_list_iterator_new(background_data->write.bytes_written);

		while (j_list_iterator_next(it))
		{
			guint64* bytes_written = j_list_iterator_get(it);

			nbytes = j_message_get_8(reply);
			j_helper_atomic_add(bytes_written, nbytes);
		}

		j_list_iterator_free(it);

		j_message_unref(reply);
	}

	j_message_unref(background_data->message);

	j_connection_pool_push_data(background_data->index, data_connection);

	j_list_unref(background_data->write.bytes_written);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

/**
 * Executes status operations in a background operation.
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
j_distributed_object_status_background_operation (gpointer data)
{
	JDistributedObjectBackgroundData* background_data = data;

	JListIterator* it;
	JMessage* reply;
	GSocketConnection* data_connection;

	data_connection = j_connection_pool_pop_data(background_data->index);
	j_message_send(background_data->message, data_connection);

	reply = j_message_new_reply(background_data->message);
	j_message_receive(reply, data_connection);

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

		// FIXME
		*modification_time = modification_time_;
		j_helper_atomic_add(size, size_);
	}

	j_list_iterator_free(it);

	j_message_unref(reply);
	j_message_unref(background_data->message);

	j_connection_pool_push_data(background_data->index, data_connection);

	g_slice_free(JDistributedObjectBackgroundData, background_data);

	return NULL;
}

static
gboolean
j_distributed_object_create_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage** messages;
	gchar const* namespace;
	gsize namespace_len;
	guint32 server_count;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JDistributedObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend == NULL)
	{
		server_count = j_configuration_get_data_server_count(j_configuration());
		messages = g_new(JMessage*, server_count);

		// FIXME use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			/**
			 * Force safe semantics to make the server send a reply.
			 * Otherwise, nasty races can occur when using unsafe semantics:
			 * - The client creates the item and sends its first write.
			 * - The client sends another operation using another connection from the pool.
			 * - The second operation is executed first and fails because the item does not exist.
			 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
			 **/
			messages[i] = j_message_new(J_MESSAGE_DATA_CREATE, namespace_len);
			j_message_set_safety(messages[i], semantics);
			//j_message_force_safety(messages[i], J_SEMANTICS_SAFETY_NETWORK);
			j_message_append_n(messages[i], namespace, namespace_len);
		}
	}

	while (j_list_iterator_next(it))
	{
		JDistributedObject* object = j_list_iterator_get(it);

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

			// FIXME use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
	}

	if (data_backend == NULL)
	{
		gpointer* background_data;

		background_data = g_new(gpointer, server_count);

		// FIXME use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = NULL;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_create_background_operation, background_data, server_count);

		g_free(background_data);
		g_free(messages);
	}

	j_list_iterator_free(it);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_distributed_object_delete_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage** messages;
	gchar const* namespace;
	gsize namespace_len;
	guint32 server_count;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JDistributedObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend == NULL)
	{
		server_count = j_configuration_get_data_server_count(j_configuration());
		messages = g_new(JMessage*, server_count);

		// FIXME use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = j_message_new(J_MESSAGE_DATA_DELETE, namespace_len);
			j_message_set_safety(messages[i], semantics);
			j_message_append_n(messages[i], namespace, namespace_len);
		}
	}

	while (j_list_iterator_next(it))
	{
		JDistributedObject* object = j_list_iterator_get(it);

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

			// FIXME use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
	}

	if (data_backend == NULL)
	{
		gpointer* background_data;

		background_data = g_new(gpointer, server_count);

		// FIXME use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = NULL;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_delete_background_operation, background_data, server_count);

		g_free(background_data);
		g_free(messages);
	}

	j_list_iterator_free(it);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_distributed_object_read_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage* message;
	JDistributedObject* object;
	GSocketConnection* data_connection;
	gpointer object_handle;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);

		object = operation->status.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend != NULL)
	{
		ret = j_backend_data_open(data_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		data_connection = j_connection_pool_pop_data(0);
		message = j_message_new(J_MESSAGE_DATA_READ, namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
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

		if (data_backend != NULL)
		{
			ret = j_backend_data_read(data_backend, object_handle, data, length, offset, bytes_read) && ret;
		}
		else
		{
			j_message_add_operation(message, sizeof(guint64) + sizeof(guint64));
			j_message_append_8(message, &length);
			j_message_append_8(message, &offset);
		}

		j_trace_file_end(object->name, J_TRACE_FILE_READ, length, offset);
	}

	j_list_iterator_free(it);

	if (data_backend != NULL)
	{
		ret = j_backend_data_close(data_backend, object_handle) && ret;
	}
	else
	{
		JMessage* reply;
		guint32 operations_done;
		guint32 operation_count;

		j_message_send(message, data_connection);

		reply = j_message_new_reply(message);

		operations_done = 0;
		operation_count = j_message_get_count(message);

		it = j_list_iterator_new(operations);

		/**
		 * This extra loop is necessary because the server might send multiple
		 * replies per message. The same reply object can be used to receive
		 * multiple times.
		 */
		while (operations_done < operation_count)
		{
			guint32 reply_operation_count;

			j_message_receive(reply, data_connection);

			reply_operation_count = j_message_get_count(reply);

			for (guint i = 0; i < reply_operation_count && j_list_iterator_next(it); i++)
			{
				JDistributedObjectOperation* operation = j_list_iterator_get(it);
				gpointer data = operation->read.data;
				guint64* bytes_read = operation->read.bytes_read;

				guint64 nbytes;

				nbytes = j_message_get_8(reply);
				j_helper_atomic_add(bytes_read, nbytes);

				if (nbytes > 0)
				{
					GInputStream* input;

					input = g_io_stream_get_input_stream(G_IO_STREAM(data_connection));
					g_input_stream_read_all(input, data, nbytes, NULL, NULL, NULL);
				}
			}

			operations_done += reply_operation_count;
		}

		j_list_iterator_free(it);

		j_message_unref(reply);
		j_message_unref(message);

		j_connection_pool_push_data(0, data_connection);
	}

	/*
	if (lock != NULL)
	{
		// FIXME busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_distributed_object_write_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JList** bw_lists;
	JListIterator* it;
	JMessage** messages;
	JDistributedObject* object;
	gpointer object_handle;
	gsize name_len;
	gsize namespace_len;
	guint32 server_count;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);
		g_assert(operation != NULL);

		object = operation->status.object;
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend != NULL)
	{
		ret = j_backend_data_open(data_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		server_count = j_configuration_get_data_server_count(j_configuration());
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

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}
	*/

	while (j_list_iterator_next(it))
	{
		JDistributedObjectOperation* operation = j_list_iterator_get(it);
		gconstpointer data = operation->write.data;
		guint64 length = operation->write.length;
		guint64 offset = operation->write.offset;
		guint64* bytes_written = operation->write.bytes_written;

		if (length == 0)
		{
			continue;
		}

		j_trace_file_begin(object->name, J_TRACE_FILE_WRITE);

		if (data_backend != NULL)
		{
			ret = j_backend_data_write(data_backend, object_handle, data, length, offset, bytes_written) && ret;
		}
		else
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
					messages[index] = j_message_new(J_MESSAGE_DATA_WRITE, namespace_len + name_len);
					j_message_set_safety(messages[index], semantics);
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

				if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_NONE)
				{
					j_helper_atomic_add(bytes_written, new_length);
				}
			}
		}

		j_trace_file_end(object->name, J_TRACE_FILE_WRITE, length, offset);
	}

	j_list_iterator_free(it);

	if (data_backend != NULL)
	{
		ret = j_backend_data_close(data_backend, object_handle) && ret;
	}
	else
	{
		gpointer* background_data;

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
			data->operations = operations;
			data->write.bytes_written = bw_lists[i];

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_write_background_operation, background_data, server_count);

		g_free(background_data);
		g_free(messages);
		g_free(bw_lists);
	}

	/*
	if (lock != NULL)
	{
		// FIXME busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_distributed_object_status_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* data_backend;
	JListIterator* it;
	JMessage** messages;
	gchar const* namespace;
	gsize namespace_len;
	guint32 server_count;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JDistributedObjectOperation* operation = j_list_get_first(operations);
		JDistributedObject* object = operation->status.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
	}

	it = j_list_iterator_new(operations);
	data_backend = j_data_backend();

	if (data_backend == NULL)
	{
		server_count = j_configuration_get_data_server_count(j_configuration());
		messages = g_new(JMessage*, server_count);

		// FIXME use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			messages[i] = j_message_new(J_MESSAGE_DATA_STATUS, namespace_len);
			j_message_set_safety(messages[i], semantics);
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

			// FIXME use actual distribution
			for (guint i = 0; i < server_count; i++)
			{
				j_message_add_operation(messages[i], name_len);
				j_message_append_n(messages[i], object->name, name_len);
			}
		}
	}

	j_list_iterator_free(it);

	if (data_backend == NULL)
	{
		gpointer* background_data;

		background_data = g_new(gpointer, server_count);

		// FIXME use actual distribution
		for (guint i = 0; i < server_count; i++)
		{
			JDistributedObjectBackgroundData* data;

			data = g_slice_new(JDistributedObjectBackgroundData);
			data->index = i;
			data->message = messages[i];
			data->operations = operations;

			background_data[i] = data;
		}

		j_helper_execute_parallel(j_distributed_object_status_background_operation, background_data, server_count);

		g_free(background_data);
		g_free(messages);
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
 * JDistributedObject* i;
 *
 * i = j_distributed_object_new("JULEA");
 * \endcode
 *
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_distributed_object_unref().
 **/
JDistributedObject*
j_distributed_object_new (gchar const* namespace, gchar const* name, JDistribution* distribution)
{
	JDistributedObject* object = NULL;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(distribution != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	object = g_slice_new(JDistributedObject);
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->distribution = j_distribution_ref(distribution);
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
 * JDistributedObject* i;
 *
 * j_distributed_object_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JDistributedObject*
j_distributed_object_ref (JDistributedObject* item)
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
j_distributed_object_unref (JDistributedObject* item)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
		g_free(item->name);
		g_free(item->namespace);

		g_slice_free(JDistributedObject, item);
	}

	j_trace_leave(G_STRFUNC);
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
 * \return A new item. Should be freed with j_distributed_object_unref().
 **/
void
j_distributed_object_create (JDistributedObject* object, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	operation = j_operation_new();
	// FIXME key = index + namespace
	operation->key = object;
	operation->data = j_distributed_object_ref(object);
	operation->exec_func = j_distributed_object_create_exec;
	operation->free_func = j_distributed_object_create_free;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
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
j_distributed_object_delete (JDistributedObject* object, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	operation = j_operation_new();
	operation->key = object;
	operation->data = j_distributed_object_ref(object);
	operation->exec_func = j_distributed_object_delete_exec;
	operation->free_func = j_distributed_object_delete_free;

	j_batch_add(batch, operation);

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
 * \param object     An object.
 * \param data       A buffer to hold the read data.
 * \param length     Number of bytes to read.
 * \param offset     An offset within #object.
 * \param bytes_read Number of bytes read.
 * \param batch      A batch.
 **/
void
j_distributed_object_read (JDistributedObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	JDistributedObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_read != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JDistributedObjectOperation);
	iop->read.object = j_distributed_object_ref(object);
	iop->read.data = data;
	iop->read.length = length;
	iop->read.offset = offset;
	iop->read.bytes_read = bytes_read;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_distributed_object_read_exec;
	operation->free_func = j_distributed_object_read_free;

	*bytes_read = 0;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * Writes an item.
 *
 * \note
 * j_distributed_object_write() modifies bytes_written even if j_batch_execute() is not called.
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
j_distributed_object_write (JDistributedObject* object, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	JDistributedObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_written != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JDistributedObjectOperation);
	iop->write.object = j_distributed_object_ref(object);
	iop->write.data = data;
	iop->write.length = length;
	iop->write.offset = offset;
	iop->write.bytes_written = bytes_written;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_distributed_object_write_exec;
	operation->free_func = j_distributed_object_write_free;

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
j_distributed_object_status (JDistributedObject* object, gint64* modification_time, guint64* size, JBatch* batch)
{
	JDistributedObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

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

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
