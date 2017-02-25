/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
#include <glib-object.h>
#include <gio/gio.h>

#include <jconnection-pool-internal.h>

#include <jhelper-internal.h>
#include <jmessage-internal.h>
#include <jtrace-internal.h>

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
	JConnectionPoolQueue* data_queues;
	JConnectionPoolQueue* meta_queues;
	guint data_len;
	guint meta_len;
	guint max_count;
};

typedef struct JConnectionPool JConnectionPool;

static JConnectionPool* j_connection_pool = NULL;

void
j_connection_pool_init (JConfiguration* configuration)
{
	JConnectionPool* pool;

	g_return_if_fail(j_connection_pool == NULL);

	j_trace_enter(G_STRFUNC);

	pool = g_slice_new(JConnectionPool);
	pool->configuration = j_configuration_ref(configuration);
	pool->data_len = j_configuration_get_data_server_count(configuration);
	pool->data_queues = g_new(JConnectionPoolQueue, pool->data_len);
	pool->meta_len = j_configuration_get_metadata_server_count(configuration);
	pool->meta_queues = g_new(JConnectionPoolQueue, pool->meta_len);
	pool->max_count = j_configuration_get_max_connections(configuration);

	if (pool->max_count == 0)
	{
		pool->max_count = j_helper_get_processor_count();
	}

	for (guint i = 0; i < pool->data_len; i++)
	{
		pool->data_queues[i].queue = g_async_queue_new();
		pool->data_queues[i].count = 0;
	}

	for (guint i = 0; i < pool->meta_len; i++)
	{
		pool->meta_queues[i].queue = g_async_queue_new();
		pool->meta_queues[i].count = 0;
	}

	g_atomic_pointer_set(&j_connection_pool, pool);

	j_trace_leave(G_STRFUNC);
}

void
j_connection_pool_fini (void)
{
	JConnectionPool* pool;

	g_return_if_fail(j_connection_pool != NULL);

	j_trace_enter(G_STRFUNC);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	for (guint i = 0; i < pool->data_len; i++)
	{
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->data_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->data_queues[i].queue);
	}

	for (guint i = 0; i < pool->meta_len; i++)
	{
		mongoc_client_t* connection;

		while ((connection = g_async_queue_try_pop(pool->meta_queues[i].queue)) != NULL)
		{
			mongoc_client_destroy(connection);
		}

		g_async_queue_unref(pool->meta_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	g_free(pool->data_queues);
	g_free(pool->meta_queues);

	g_slice_free(JConnectionPool, pool);

	j_trace_leave(G_STRFUNC);
}

GSocketConnection*
j_connection_pool_pop_data (guint index)
{
	GSocketConnection* connection;

	g_return_val_if_fail(j_connection_pool != NULL, NULL);
	g_return_val_if_fail(index < j_connection_pool->data_len, NULL);

	j_trace_enter(G_STRFUNC);

	connection = g_async_queue_try_pop(j_connection_pool->data_queues[index].queue);

	if (connection != NULL)
	{
		goto end;
	}

	if ((guint)g_atomic_int_get(&(j_connection_pool->data_queues[index].count)) < j_connection_pool->max_count)
	{
		if ((guint)g_atomic_int_add(&(j_connection_pool->data_queues[index].count), 1) < j_connection_pool->max_count)
		{
			GError* error = NULL;
			GSocketClient* client;

#ifdef J_USE_HELLO
			JMessage* message;
			JMessage* reply;
#endif

			client = g_socket_client_new();

			connection = g_socket_client_connect_to_host(client, j_configuration_get_data_server(j_connection_pool->configuration, index), 4711, NULL, &error);

			if (error != NULL)
			{
				J_CRITICAL("%s", error->message);
				g_error_free(error);
			}

			if (connection == NULL)
			{
				J_CRITICAL("Can not connect to %s [%d].", j_configuration_get_data_server(j_connection_pool->configuration, index), g_atomic_int_get(&(j_connection_pool->data_queues[index].count)));
			}

			j_helper_set_nodelay(connection, TRUE);

#ifdef J_USE_HELLO
				message = j_message_new(J_MESSAGE_HELLO, 0);
				j_message_send(message, connection);

				reply = j_message_new_reply(message);
				j_message_receive(reply, connection);

				j_message_unref(message);
				j_message_unref(reply);
#endif

			g_object_unref(client);
		}
		else
		{
			g_atomic_int_add(&(j_connection_pool->data_queues[index].count), -1);
		}
	}

	if (connection != NULL)
	{
		goto end;
	}

	connection = g_async_queue_pop(j_connection_pool->data_queues[index].queue);

end:
	j_trace_leave(G_STRFUNC);

	return connection;
}

void
j_connection_pool_push_data (guint index, GSocketConnection* connection)
{
	g_return_if_fail(j_connection_pool != NULL);
	g_return_if_fail(index < j_connection_pool->data_len);
	g_return_if_fail(connection != NULL);

	j_trace_enter(G_STRFUNC);

	g_async_queue_push(j_connection_pool->data_queues[index].queue, connection);

	j_trace_leave(G_STRFUNC);
}

mongoc_client_t*
j_connection_pool_pop_meta (guint index)
{
	mongoc_client_t* connection;

	g_return_val_if_fail(j_connection_pool != NULL, NULL);
	g_return_val_if_fail(index < j_connection_pool->meta_len, NULL);

	j_trace_enter(G_STRFUNC);

	connection = g_async_queue_try_pop(j_connection_pool->meta_queues[index].queue);

	if (connection != NULL)
	{
		goto end;
	}

	if ((guint)g_atomic_int_get(&(j_connection_pool->meta_queues[index].count)) < j_connection_pool->max_count)
	{
		if ((guint)g_atomic_int_add(&(j_connection_pool->meta_queues[index].count), 1) < j_connection_pool->max_count)
		{
			gboolean ret = FALSE;
			mongoc_uri_t* uri;

			uri = mongoc_uri_new_for_host_port(j_configuration_get_metadata_server(j_connection_pool->configuration, index), 27017);

			connection = mongoc_client_new_from_uri(uri);

			if (connection != NULL)
			{
				ret = mongoc_client_get_server_status(connection, NULL, NULL, NULL);
			}

			if (!ret)
			{
				J_CRITICAL("Can not connect to MongoDB %s [%d].", j_configuration_get_metadata_server(j_connection_pool->configuration, index), g_atomic_int_get(&(j_connection_pool->meta_queues[index].count)));
			}
		}
		else
		{
			g_atomic_int_add(&(j_connection_pool->meta_queues[index].count), -1);
		}
	}

	if (connection != NULL)
	{
		goto end;
	}

	connection = g_async_queue_pop(j_connection_pool->meta_queues[index].queue);

end:
	j_trace_leave(G_STRFUNC);

	return connection;
}

void
j_connection_pool_push_meta (guint index, mongoc_client_t* connection)
{
	g_return_if_fail(j_connection_pool != NULL);
	g_return_if_fail(index < j_connection_pool->meta_len);
	g_return_if_fail(connection != NULL);

	j_trace_enter(G_STRFUNC);

	g_async_queue_push(j_connection_pool->meta_queues[index].queue, connection);

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
