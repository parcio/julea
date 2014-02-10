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
#include <glib-object.h>
#include <gio/gio.h>

#include <jconnection-pool-internal.h>

#include <jconnection-internal.h>
#include <jhelper-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JConnectionPool Connection Pool
 *
 * Data structures and functions for managing connection pools.
 *
 * @{
 **/

/**
 * A connection.
 **/
struct JConnectionPool
{
	JConfiguration* configuration;
	GAsyncQueue* queue;
	guint count;
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
	pool->queue = g_async_queue_new();
	pool->count = 0;
	pool->max_count = j_helper_get_processor_count();

	g_atomic_pointer_set(&j_connection_pool, pool);

	j_trace_leave(G_STRFUNC);
}

void
j_connection_pool_fini (void)
{
	JConnection* connection;
	JConnectionPool* pool;

	g_return_if_fail(j_connection_pool != NULL);

	j_trace_enter(G_STRFUNC);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	while ((connection = g_async_queue_try_pop(pool->queue)) != NULL)
	{
		j_connection_disconnect(connection);
		j_connection_unref(connection);
	}

	g_async_queue_unref(pool->queue);
	j_configuration_unref(pool->configuration);

	g_slice_free(JConnectionPool, pool);

	j_trace_leave(G_STRFUNC);
}

JConnection*
j_connection_pool_pop (void)
{
	JConnection* connection;

	g_return_val_if_fail(j_connection_pool != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	connection = g_async_queue_try_pop(j_connection_pool->queue);

	if (connection != NULL)
	{
		goto end;
	}

	if ((guint)g_atomic_int_get(&(j_connection_pool->count)) < j_connection_pool->max_count)
	{
		if ((guint)g_atomic_int_add(&(j_connection_pool->count), 1) < j_connection_pool->max_count)
		{
			connection = j_connection_new(j_connection_pool->configuration);

			// FIXME
			if (!j_connection_connect(connection))
			{
				g_critical("%s: Failed to connect.", G_STRLOC);
			}
		}
		else
		{
			g_atomic_int_add(&(j_connection_pool->count), -1);
		}
	}

	if (connection != NULL)
	{
		goto end;
	}

	connection = g_async_queue_pop(j_connection_pool->queue);

end:
	j_trace_leave(G_STRFUNC);

	return connection;
}

void
j_connection_pool_push (JConnection* connection)
{
	g_return_if_fail(j_connection_pool != NULL);

	j_trace_enter(G_STRFUNC);

	g_async_queue_push(j_connection_pool->queue, connection);

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
