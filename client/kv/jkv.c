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

#include <kv/jkv.h>

#include <jbatch.h>
#include <jbatch-internal.h>
#include <jcommon-internal.h>
#include <jconnection-pool-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jlock-internal.h>
#include <jmessage-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

/**
 * \defgroup JKV KV
 *
 * Data structures and functions for managing objects.
 *
 * @{
 **/

struct JObjectOperation
{
	union
	{
		struct
		{
			JKV* object;
			gint64* modification_time;
			guint64* size;
		}
		get_status;

		struct
		{
			JKV* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		}
		read;

		struct
		{
			JKV* object;
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
 * A JKV.
 **/
struct JKV
{
	/**
	 * The data server index.
	 */
	guint32 index;

	/**
	 * The namespace.
	 **/
	gchar* namespace;

	/**
	 * The key.
	 **/
	gchar* key;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

static
void
j_kv_put_free (gpointer data)
{
	JKV* object = data;

	j_kv_unref(object);
}

static
void
j_kv_delete_free (gpointer data)
{
	JKV* object = data;

	j_kv_unref(object);
}

static
void
j_kv_get_status_free (gpointer data)
{
	JObjectOperation* operation = data;

	j_kv_unref(operation->get_status.object);

	g_slice_free(JObjectOperation, operation);
}

static
gboolean
j_kv_put_exec (JList* operations, JSemantics* semantics)
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
		JKV* object;

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
		/**
		 * Force safe semantics to make the server send a reply.
		 * Otherwise, nasty races can occur when using unsafe semantics:
		 * - The client creates the item and sends its first write.
		 * - The client sends another operation using another connection from the pool.
		 * - The second operation is executed first and fails because the item does not exist.
		 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
		 **/
		message = j_message_new(J_MESSAGE_DATA_CREATE, namespace_len);
		j_message_force_safety(message, J_SEMANTICS_SAFETY_NETWORK);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JKV* object = j_list_iterator_get(it);

		if (data_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_data_create(data_backend, object->namespace, object->key, &object_handle) && ret;
			ret = j_backend_data_close(data_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->key) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->key, name_len);
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
j_kv_delete_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* meta_backend;
	JListIterator* it;
	JMessage* message;
	gchar const* namespace;
	gpointer meta_batch;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JKV* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	meta_backend = j_metadata_backend();

	if (meta_backend != NULL)
	{
		ret = j_backend_meta_batch_start(meta_backend, namespace, &meta_batch);
	}
	else
	{
		message = j_message_new(J_MESSAGE_META_DELETE, namespace_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JKV* kv = j_list_iterator_get(it);

		if (meta_backend != NULL)
		{
			ret = j_backend_meta_delete(meta_backend, meta_batch, kv->key) && ret;
		}
		else
		{
			gsize key_len;

			key_len = strlen(kv->key) + 1;

			j_message_add_operation(message, key_len);
			j_message_append_n(message, kv->key, key_len);
		}
	}

	if (meta_backend != NULL)
	{
		ret = j_backend_meta_batch_execute(meta_backend, meta_batch) && ret;
	}
	else
	{
		GSocketConnection* meta_connection;

		meta_connection = j_connection_pool_pop_meta(index);
		j_message_send(message, meta_connection);

		if (j_message_get_type_modifier(message) & J_MESSAGE_SAFETY_NETWORK)
		{
			JMessage* reply;

			reply = j_message_new_reply(message);
			j_message_receive(reply, meta_connection);

			/* FIXME do something with reply */
			j_message_unref(reply);
		}

		j_message_unref(message);
		j_connection_pool_push_meta(index, meta_connection);
	}

	j_list_iterator_free(it);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_kv_get_status_exec (JList* operations, JSemantics* semantics)
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
		JKV* object = operation->get_status.object;

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
		JKV* object = operation->get_status.object;
		gint64* modification_time = operation->get_status.modification_time;
		guint64* size = operation->get_status.size;

		if (data_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_data_open(data_backend, object->namespace, object->key, &object_handle) && ret;
			ret = j_backend_data_status(data_backend, object_handle, modification_time, size) && ret;
			ret = j_backend_data_close(data_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->key) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->key, name_len);
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
 * JKV* i;
 *
 * i = j_kv_new("JULEA");
 * \endcode
 *
 * \param key         An item key.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_kv_unref().
 **/
JKV*
j_kv_new (guint32 index, gchar const* namespace, gchar const* key)
{
	JKV* kv = NULL;

	JConfiguration* configuration = j_configuration();

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(key != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_data_server_count(configuration), NULL);

	j_trace_enter(G_STRFUNC, NULL);

	kv = g_slice_new(JKV);
	kv->index = index;
	kv->namespace = g_strdup(namespace);
	kv->key = g_strdup(key);
	kv->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return kv;
}

/**
 * Increases an item's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JKV* i;
 *
 * j_kv_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JKV*
j_kv_ref (JKV* kv)
{
	g_return_val_if_fail(kv != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(kv->ref_count));

	j_trace_leave(G_STRFUNC);

	return kv;
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
j_kv_unref (JKV* kv)
{
	g_return_if_fail(kv != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(kv->ref_count)))
	{
		g_free(kv->key);
		g_free(kv->namespace);

		g_slice_free(JKV, kv);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Returns an item's key.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return The key.
 **/
gchar const*
j_kv_get_name (JKV* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);
	j_trace_leave(G_STRFUNC);

	return item->key;
}

/**
 * Creates an object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param key         A key.
 * \param distribution A distribution.
 * \param batch        A batch.
 *
 * \return A new item. Should be freed with j_kv_unref().
 **/
void
j_kv_put (JKV* object, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	operation = j_operation_new();
	// FIXME key = index + namespace
	operation->key = object;
	operation->data = j_kv_ref(object);
	operation->exec_func = j_kv_put_exec;
	operation->free_func = j_kv_put_free;

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
j_kv_delete (JKV* object, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	operation = j_operation_new();
	operation->key = object;
	operation->data = j_kv_ref(object);
	operation->exec_func = j_kv_delete_exec;
	operation->free_func = j_kv_delete_free;

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
j_kv_get_status (JKV* object, gint64* modification_time, guint64* size, JBatch* batch)
{
	JObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JObjectOperation);
	iop->get_status.object = j_kv_ref(object);
	iop->get_status.modification_time = modification_time;
	iop->get_status.size = size;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_kv_get_status_exec;
	operation->free_func = j_kv_get_status_free;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
