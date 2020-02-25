/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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
#include <jhelper-internal.h>
#include <jmessage.h>
#include <jtrace.h>

/**
 * \defgroup JConnectionPool Connection Pool
 *
 * Data structures and functions for managing connection pools.
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
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	g_free(pool->object_queues);
	g_free(pool->kv_queues);
	g_free(pool->db_queues);

	g_slice_free(JConnectionPool, pool);
}

static GSocketConnection*
j_connection_pool_pop_internal(GAsyncQueue* queue, guint* count, gchar const* server)
{
	J_TRACE_FUNCTION(NULL);

	GSocketConnection* connection;

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
			GError* error = NULL;
			g_autoptr(GSocketClient) client = NULL;

			g_autoptr(JMessage) message = NULL;
			g_autoptr(JMessage) reply = NULL;

			guint op_count;

			client = g_socket_client_new();
			connection = g_socket_client_connect_to_host(client, server, 4711, NULL, &error);

			if (error != NULL)
			{
				g_critical("%s", error->message);
				g_error_free(error);
			}

			if (connection == NULL)
			{
				g_critical("Can not connect to %s [%d].", server, g_atomic_int_get(count));
			}

			j_helper_set_nodelay(connection, TRUE);

			message = j_message_new(J_MESSAGE_PING, 0);
			j_message_send(message, connection);

			reply = j_message_new_reply(message);
			j_message_receive(reply, connection);

			op_count = j_message_get_count(reply);

			for (guint i = 0; i < op_count; i++)
			{
				gchar const* backend;

				backend = j_message_get_string(reply);

				if (g_strcmp0(backend, "object") == 0)
				{
					//g_print("Server has object backend.\n");
				}
				else if (g_strcmp0(backend, "kv") == 0)
				{
					//g_print("Server has kv backend.\n");
				}
				else if (g_strcmp0(backend, "db") == 0)
				{
					//g_print("Server has db backend.\n");
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
			return j_connection_pool_pop_internal(j_connection_pool->object_queues[index].queue, &(j_connection_pool->object_queues[index].count), j_configuration_get_server(j_connection_pool->configuration, J_BACKEND_TYPE_OBJECT, index));
		case J_BACKEND_TYPE_KV:
			g_return_val_if_fail(index < j_connection_pool->kv_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->kv_queues[index].queue, &(j_connection_pool->kv_queues[index].count), j_configuration_get_server(j_connection_pool->configuration, J_BACKEND_TYPE_KV, index));
		case J_BACKEND_TYPE_DB:
			g_return_val_if_fail(index < j_connection_pool->db_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->db_queues[index].queue, &(j_connection_pool->db_queues[index].count), j_configuration_get_server(j_connection_pool->configuration, J_BACKEND_TYPE_DB, index));
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
