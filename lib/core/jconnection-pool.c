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

#include <julea-config.h>

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include <jconnection-pool.h>
#include <jconnection-pool-internal.h>

#include <jbackend.h>
#include <jhelper.h>
#include <jmessage.h>
#include <jtrace.h>

/**
 * \addtogroup JConnectionPool
 *
 * @{
 **/

struct JConnectionPoolQueue
{
	GAsyncQueue* queue;
	guint count;
};

typedef struct JConnectionPoolQueue JConnectionPoolQueue;

/**
 * A connection.
 **/
struct JConnectionPool
{
	JConfiguration* configuration;
	JConnectionPoolQueue* object_queues;
	JConnectionPoolQueue* kv_queues;
	JConnectionPoolQueue* db_queues;
	guint object_len;
	guint kv_len;
	guint db_len;
	guint max_count;
};

typedef struct JConnectionPool JConnectionPool;

static JConnectionPool* j_connection_pool = NULL;

void
j_connection_pool_init(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

	g_return_if_fail(j_connection_pool == NULL);

	pool = g_slice_new(JConnectionPool);
	pool->configuration = j_configuration_ref(configuration);
	pool->object_len = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT);
	pool->object_queues = g_new(JConnectionPoolQueue, pool->object_len);
	pool->kv_len = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_KV);
	pool->kv_queues = g_new(JConnectionPoolQueue, pool->kv_len);
	pool->db_len = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_DB);
	pool->db_queues = g_new(JConnectionPoolQueue, pool->db_len);
	pool->max_count = j_configuration_get_max_connections(configuration);

	for (guint i = 0; i < pool->object_len; i++)
	{
		pool->object_queues[i].queue = g_async_queue_new();
		pool->object_queues[i].count = 0;
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		pool->kv_queues[i].queue = g_async_queue_new();
		pool->kv_queues[i].count = 0;
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		pool->db_queues[i].queue = g_async_queue_new();
		pool->db_queues[i].count = 0;
	}

	g_atomic_pointer_set(&j_connection_pool, pool);
}

void
j_connection_pool_fini(void)
{
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

	g_return_if_fail(j_connection_pool != NULL);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	for (guint i = 0; i < pool->object_len; i++)
	{
		struct JConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			j_connection_fini(connection);
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		struct JConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			j_connection_fini(connection);
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		struct JConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			j_connection_fini(connection);
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	g_free(pool->object_queues);
	g_free(pool->kv_queues);
	g_free(pool->db_queues);

	g_slice_free(JConnectionPool, pool);
}

static struct JConnection*
j_connection_pool_pop_internal(GAsyncQueue* queue, guint* count, enum JBackendType backend, guint64 index)
{
	J_TRACE_FUNCTION(NULL);

	struct JConnection* connection;

	g_return_val_if_fail(queue != NULL, NULL);
	g_return_val_if_fail(count != NULL, NULL);

	connection = g_async_queue_try_pop(queue);

	if (connection != NULL)
	{
		return connection;
	}

	if ((guint)g_atomic_int_get(count) < j_connection_pool->max_count)
	{
		if ((guint)g_atomic_int_add(count, 1) < j_connection_pool->max_count)
		{
			g_autoptr(JMessage) message = NULL;
			g_autoptr(JMessage) reply = NULL;

			guint op_count;

			if (!j_connection_init_client(j_connection_pool->configuration, backend, index, &connection))
			{
				g_critical("Can not connect to %s [%d].", "TODO(SERVER ADDR)", g_atomic_int_get(count));
				exit(1);
			}

			message = j_message_new(J_MESSAGE_PING, 0);
			j_message_send(message, connection);

			reply = j_message_new_reply(message);
			j_message_receive(reply, connection);

			op_count = j_message_get_count(reply);

			for (guint i = 0; i < op_count; i++)
			{
				gchar const* backend_str;

				backend_str = j_message_get_string(reply);

				if (g_strcmp0(backend_str, "object") == 0)
				{
					//g_print("Server has object backend_str.\n");
				}
				else if (g_strcmp0(backend_str, "kv") == 0)
				{
					//g_print("Server has kv backend_str.\n");
				}
				else if (g_strcmp0(backend_str, "db") == 0)
				{
					//g_print("Server has db backend_str.\n");
				}
			}
		}
		else
		{
			g_atomic_int_add(count, -1);
		}
	}

	if (connection != NULL)
	{
		return connection;
	}

	connection = g_async_queue_pop(queue);

	return connection;
}

static void
j_connection_pool_push_internal(GAsyncQueue* queue, GSocketConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(queue != NULL);
	g_return_if_fail(connection != NULL);

	g_async_queue_push(queue, connection);
}

gpointer
j_connection_pool_pop(JBackendType backend, guint index)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(j_connection_pool != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			g_return_val_if_fail(index < j_connection_pool->object_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->object_queues[index].queue, &(j_connection_pool->object_queues[index].count), backend, index);
		case J_BACKEND_TYPE_KV:
			g_return_val_if_fail(index < j_connection_pool->kv_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->kv_queues[index].queue, &(j_connection_pool->kv_queues[index].count), backend, index);
		case J_BACKEND_TYPE_DB:
			g_return_val_if_fail(index < j_connection_pool->db_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->db_queues[index].queue, &(j_connection_pool->db_queues[index].count), backend, index);
		default:
			g_assert_not_reached();
	}

	return NULL;
}

void
j_connection_pool_push(JBackendType backend, guint index, gpointer connection)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(j_connection_pool != NULL);
	g_return_if_fail(connection != NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			g_return_if_fail(index < j_connection_pool->object_len);
			j_connection_pool_push_internal(j_connection_pool->object_queues[index].queue, connection);
			break;
		case J_BACKEND_TYPE_KV:
			g_return_if_fail(index < j_connection_pool->kv_len);
			j_connection_pool_push_internal(j_connection_pool->kv_queues[index].queue, connection);
			break;
		case J_BACKEND_TYPE_DB:
			g_return_if_fail(index < j_connection_pool->db_len);
			j_connection_pool_push_internal(j_connection_pool->db_queues[index].queue, connection);
			break;
		default:
			g_assert_not_reached();
	}
}

/**
 * @}
 **/
