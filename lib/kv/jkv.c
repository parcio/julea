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

#include <kv/jkv.h>
#include <kv/jkv-internal.h>

#include <julea.h>

/**
 * \addtogroup JKV
 *
 * @{
 **/

struct JKVOperation
{
	union
	{
		struct
		{
			JKV* kv;
			gpointer* value;
			guint32* value_len;
			JKVGetFunc func;
			gpointer data;
		} get;

		struct
		{
			JKV* kv;
			gpointer* value;
			guint32 value_len;
			GDestroyNotify value_destroy;
		} put;
	};
};

typedef struct JKVOperation JKVOperation;

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

static JBackend* j_kv_backend = NULL;
static GModule* j_kv_module = NULL;

/// \todo copy and use GLib's G_DEFINE_CONSTRUCTOR/DESTRUCTOR
static void __attribute__((constructor)) j_kv_init(void);
static void __attribute__((destructor)) j_kv_fini(void);

/**
 * Initializes the kv client.
 */
static void
j_kv_init(void)
{
	gchar const* kv_backend;
	gchar const* kv_component;
	gchar const* kv_path;

	if (j_kv_backend != NULL && j_kv_module != NULL)
	{
		return;
	}

	kv_backend = j_configuration_get_backend(j_configuration(), J_BACKEND_TYPE_KV);
	kv_component = j_configuration_get_backend_component(j_configuration(), J_BACKEND_TYPE_KV);
	kv_path = j_configuration_get_backend_path(j_configuration(), J_BACKEND_TYPE_KV);

	if (j_backend_load_client(kv_backend, kv_component, J_BACKEND_TYPE_KV, &j_kv_module, &j_kv_backend))
	{
		if (j_kv_backend == NULL || !j_backend_kv_init(j_kv_backend, kv_path))
		{
			g_critical("Could not initialize kv backend %s.\n", kv_backend);
		}
	}
}

/**
 * Shuts down the kv client.
 */
static void
j_kv_fini(void)
{
	if (j_kv_backend == NULL && j_kv_module == NULL)
	{
		return;
	}

	if (j_kv_backend != NULL)
	{
		j_backend_kv_fini(j_kv_backend);
		j_kv_backend = NULL;
	}

	if (j_kv_module != NULL)
	{
		g_module_close(j_kv_module);
		j_kv_module = NULL;
	}
}

static void
j_kv_put_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JKVOperation* operation = data;

	j_kv_unref(operation->put.kv);

	if (operation->put.value_destroy != NULL)
	{
		operation->put.value_destroy(operation->put.value);
	}

	g_slice_free(JKVOperation, operation);
}

static void
j_kv_delete_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JKV* kv = data;

	j_kv_unref(kv);
}

static void
j_kv_get_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JKVOperation* operation = data;

	j_kv_unref(operation->get.kv);

	g_slice_free(JKVOperation, operation);
}

static gboolean
j_kv_put_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = TRUE;

	JBackend* kv_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	JSemanticsSafety safety;
	gchar const* namespace;
	gpointer kv_batch = NULL;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JKVOperation* kop;

		kop = j_list_get_first(operations);
		g_assert(kop != NULL);

		namespace = kop->put.kv->namespace;
		namespace_len = strlen(namespace) + 1;
		index = kop->put.kv->index;
	}

	safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
	it = j_list_iterator_new(operations);
	kv_backend = j_kv_get_backend();

	if (kv_backend == NULL)
	{
		/**
		 * Force safe semantics to make the server send a reply.
		 * Otherwise, nasty races can occur when using unsafe semantics:
		 * - The client creates the item and sends its first write.
		 * - The client sends another operation using another connection from the pool.
		 * - The second operation is executed first and fails because the item does not exist.
		 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
		 **/
		message = j_message_new(J_MESSAGE_KV_PUT, namespace_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}
	else
	{
		ret = j_backend_kv_batch_start(kv_backend, namespace, semantics, &kv_batch);
	}

	while (j_list_iterator_next(it))
	{
		JKVOperation* kop = j_list_iterator_get(it);

		if (kv_backend == NULL)
		{
			gsize key_len;

			key_len = strlen(kop->put.kv->key) + 1;

			j_message_add_operation(message, key_len + 4 + kop->put.value_len);
			j_message_append_n(message, kop->put.kv->key, key_len);
			j_message_append_4(message, &(kop->put.value_len));
			j_message_append_n(message, kop->put.value, kop->put.value_len);
		}
		else
		{
			ret = j_backend_kv_put(kv_backend, kv_batch, kop->put.kv->key, kop->put.value, kop->put.value_len) && ret;
		}
	}

	if (kv_backend == NULL)
	{
		gpointer kv_connection;

		kv_connection = j_connection_pool_pop(J_BACKEND_TYPE_KV, index);
		j_message_send(message, kv_connection);

		if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
		{
			g_autoptr(JMessage) reply = NULL;

			reply = j_message_new_reply(message);
			j_message_receive(reply, kv_connection);

			/// \todo do something with reply
		}

		j_connection_pool_push(J_BACKEND_TYPE_KV, index, kv_connection);
	}
	else
	{
		ret = j_backend_kv_batch_execute(kv_backend, kv_batch) && ret;
	}

	return ret;
}

static gboolean
j_kv_delete_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = TRUE;

	JBackend* kv_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	JSemanticsSafety safety;
	gchar const* namespace;
	gpointer kv_batch = NULL;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JKV* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
	it = j_list_iterator_new(operations);
	kv_backend = j_kv_get_backend();

	if (kv_backend == NULL)
	{
		message = j_message_new(J_MESSAGE_KV_DELETE, namespace_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}
	else
	{
		ret = j_backend_kv_batch_start(kv_backend, namespace, semantics, &kv_batch);
	}

	while (j_list_iterator_next(it))
	{
		JKV* kv = j_list_iterator_get(it);

		if (kv_backend == NULL)
		{
			gsize key_len;

			key_len = strlen(kv->key) + 1;

			j_message_add_operation(message, key_len);
			j_message_append_n(message, kv->key, key_len);
		}
		else
		{
			ret = j_backend_kv_delete(kv_backend, kv_batch, kv->key) && ret;
		}
	}

	if (kv_backend == NULL)
	{
		gpointer kv_connection;

		kv_connection = j_connection_pool_pop(J_BACKEND_TYPE_KV, index);
		j_message_send(message, kv_connection);

		if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
		{
			g_autoptr(JMessage) reply = NULL;

			reply = j_message_new_reply(message);
			j_message_receive(reply, kv_connection);

			/// \todo do something with reply
		}

		j_connection_pool_push(J_BACKEND_TYPE_KV, index, kv_connection);
	}
	else
	{
		ret = j_backend_kv_batch_execute(kv_backend, kv_batch) && ret;
	}

	return ret;
}

static gboolean
j_kv_get_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = TRUE;

	JBackend* kv_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gpointer kv_batch = NULL;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JKVOperation* kop;

		kop = j_list_get_first(operations);
		g_assert(kop != NULL);

		namespace = kop->get.kv->namespace;
		namespace_len = strlen(namespace) + 1;
		index = kop->get.kv->index;
	}

	it = j_list_iterator_new(operations);
	kv_backend = j_kv_get_backend();

	if (kv_backend == NULL)
	{
		/**
		 * Force safe semantics to make the server send a reply.
		 * Otherwise, nasty races can occur when using unsafe semantics:
		 * - The client creates the item and sends its first write.
		 * - The client sends another operation using another connection from the pool.
		 * - The second operation is executed first and fails because the item does not exist.
		 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
		 **/
		message = j_message_new(J_MESSAGE_KV_GET, namespace_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}
	else
	{
		ret = j_backend_kv_batch_start(kv_backend, namespace, semantics, &kv_batch);
	}

	while (j_list_iterator_next(it))
	{
		JKVOperation* kop = j_list_iterator_get(it);

		if (kv_backend == NULL)
		{
			gsize key_len;

			key_len = strlen(kop->get.kv->key) + 1;

			j_message_add_operation(message, key_len);
			j_message_append_n(message, kop->get.kv->key, key_len);
		}
		else
		{
			if (kop->get.func != NULL)
			{
				gpointer value;
				guint32 len;

				ret = j_backend_kv_get(kv_backend, kv_batch, kop->get.kv->key, &value, &len) && ret;

				if (ret)
				{
					// j_backend_kv_get returns a new copy, pass it along
					kop->get.func(value, len, kop->get.data);
				}
			}
			else
			{
				ret = j_backend_kv_get(kv_backend, kv_batch, kop->get.kv->key, kop->get.value, kop->get.value_len) && ret;
			}
		}
	}

	if (kv_backend == NULL)
	{
		g_autoptr(JListIterator) iter = NULL;
		g_autoptr(JMessage) reply = NULL;
		gpointer kv_connection;

		kv_connection = j_connection_pool_pop(J_BACKEND_TYPE_KV, index);
		j_message_send(message, kv_connection);

		reply = j_message_new_reply(message);
		j_message_receive(reply, kv_connection);

		iter = j_list_iterator_new(operations);

		while (j_list_iterator_next(iter))
		{
			JKVOperation* kop = j_list_iterator_get(iter);
			guint32 len;

			len = j_message_get_4(reply);
			ret = (len > 0) && ret;

			if (len > 0)
			{
				gconstpointer data;

				data = j_message_get_n(reply, len);

				if (kop->get.func != NULL)
				{
					gpointer value;

					// data belongs to the message, create a copy for the callback
#if GLIB_CHECK_VERSION(2, 68, 0)
					value = g_memdup2(data, len);
#else
					value = g_memdup(data, len);
#endif
					kop->get.func(value, len, kop->get.data);
				}
				else
				{
#if GLIB_CHECK_VERSION(2, 68, 0)
					*(kop->get.value) = g_memdup2(data, len);
#else
					*(kop->get.value) = g_memdup(data, len);
#endif
					*(kop->get.value_len) = len;
				}
			}
		}

		j_connection_pool_push(J_BACKEND_TYPE_KV, index, kv_connection);
	}
	else
	{
		ret = j_backend_kv_batch_execute(kv_backend, kv_batch) && ret;
	}

	return ret;
}

JKV*
j_kv_new(gchar const* namespace, gchar const* key)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration = j_configuration();
	JKV* kv;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(key != NULL, NULL);

	kv = g_slice_new(JKV);
	kv->index = j_helper_hash(key) % j_configuration_get_server_count(configuration, J_BACKEND_TYPE_KV);
	kv->namespace = g_strdup(namespace);
	kv->key = g_strdup(key);
	kv->ref_count = 1;

	return kv;
}

JKV*
j_kv_new_for_index(guint32 index, gchar const* namespace, gchar const* key)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration = j_configuration();
	JKV* kv;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(key != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_server_count(configuration, J_BACKEND_TYPE_KV), NULL);

	kv = g_slice_new(JKV);
	kv->index = index;
	kv->namespace = g_strdup(namespace);
	kv->key = g_strdup(key);
	kv->ref_count = 1;

	return kv;
}

JKV*
j_kv_ref(JKV* kv)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(kv != NULL, NULL);

	g_atomic_int_inc(&(kv->ref_count));

	return kv;
}

void
j_kv_unref(JKV* kv)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(kv != NULL);

	if (g_atomic_int_dec_and_test(&(kv->ref_count)))
	{
		g_free(kv->key);
		g_free(kv->namespace);

		g_slice_free(JKV, kv);
	}
}

void
j_kv_put(JKV* kv, gpointer value, guint32 value_len, GDestroyNotify value_destroy, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JKVOperation* kop;
	JOperation* operation;

	g_return_if_fail(kv != NULL);

	kop = g_slice_new(JKVOperation);
	kop->put.kv = j_kv_ref(kv);
	kop->put.value = value;
	kop->put.value_len = value_len;
	kop->put.value_destroy = value_destroy;

	operation = j_operation_new();
	/// \todo key = index + namespace
	operation->key = kv;
	operation->data = kop;
	operation->exec_func = j_kv_put_exec;
	operation->free_func = j_kv_put_free;

	j_batch_add(batch, operation);
}

void
j_kv_delete(JKV* kv, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;

	g_return_if_fail(kv != NULL);

	operation = j_operation_new();
	operation->key = kv;
	operation->data = j_kv_ref(kv);
	operation->exec_func = j_kv_delete_exec;
	operation->free_func = j_kv_delete_free;

	j_batch_add(batch, operation);
}

void
j_kv_get(JKV* kv, gpointer* value, guint32* value_len, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JKVOperation* kop;
	JOperation* operation;

	g_return_if_fail(kv != NULL);

	kop = g_slice_new(JKVOperation);
	kop->get.kv = j_kv_ref(kv);
	kop->get.value = value;
	kop->get.value_len = value_len;
	kop->get.func = NULL;
	kop->get.data = NULL;

	operation = j_operation_new();
	operation->key = kv;
	operation->data = kop;
	operation->exec_func = j_kv_get_exec;
	operation->free_func = j_kv_get_free;

	j_batch_add(batch, operation);
}

void
j_kv_get_callback(JKV* kv, JKVGetFunc func, gpointer data, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JKVOperation* kop;
	JOperation* operation;

	g_return_if_fail(kv != NULL);
	g_return_if_fail(func != NULL);

	kop = g_slice_new(JKVOperation);
	kop->get.kv = j_kv_ref(kv);
	kop->get.value = NULL;
	kop->get.value_len = NULL;
	kop->get.func = func;
	kop->get.data = data;

	operation = j_operation_new();
	operation->key = kv;
	operation->data = kop;
	operation->exec_func = j_kv_get_exec;
	operation->free_func = j_kv_get_free;

	j_batch_add(batch, operation);
}

/**
 * Returns the kv backend.
 *
 * \return The kv backend.
 */
JBackend*
j_kv_get_backend(void)
{
	return j_kv_backend;
}

/**
 * @}
 **/
