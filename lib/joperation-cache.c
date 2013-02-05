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

#include <joperation-cache-internal.h>

#include <jbackground-operation-internal.h>
#include <jcache-internal.h>
#include <jcommon-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-internal.h>

/**
 * \defgroup JOperationCache Operation Cache
 *
 * @{
 **/

/**
 * An operation cache.
 */
struct JOperationCache
{
	/**
	 * The cache.
	 */
	JCache* cache;

	/**
	 * The queue of operations.
	 */
	GAsyncQueue* queue;

	/**
	 * The thread executing operations in the background.
	 */
	GThread* thread;

	/**
	 * Whether #thread is currently working.
	 */
	gboolean working;

	/**
	 * The mutex for #working.
	 */
	GMutex mutex[1];

	/**
	 * The condition for #working.
	 */
	GCond cond[1];
};

typedef struct JOperationCache JOperationCache;

static JOperationCache* j_operation_cache = NULL;

static
gpointer
j_operation_cache_thread (gpointer data)
{
	JOperationCache* cache = data;
	JBatch* batch;

	j_trace_enter(G_STRFUNC);

	while ((batch = g_async_queue_pop(cache->queue)) != NULL)
	{
		/* data == cache, terminate */
		if (batch == data)
		{
			return NULL;
		}

		j_batch_execute_internal(batch);
		j_batch_unref(batch);

		g_mutex_lock(cache->mutex);

		cache->working = (g_async_queue_length(cache->queue) > 0);

		if (!cache->working)
		{
			g_cond_signal(cache->cond);
		}

		g_mutex_unlock(cache->mutex);
	}

	// FIXME clears all
	j_cache_clear(cache->cache);

	j_trace_leave(G_STRFUNC);

	return NULL;
}

static
gboolean
j_operation_cache_test (JOperation* operation)
{
	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC);

	switch (operation->type)
	{
		case J_OPERATION_CREATE_STORE:
		case J_OPERATION_DELETE_STORE:
		case J_OPERATION_STORE_CREATE_COLLECTION:
		case J_OPERATION_STORE_DELETE_COLLECTION:
		case J_OPERATION_COLLECTION_CREATE_ITEM:
		case J_OPERATION_COLLECTION_DELETE_ITEM:
		case J_OPERATION_ITEM_WRITE:
			ret = TRUE;
			break;

		case J_OPERATION_GET_STORE:
		case J_OPERATION_STORE_GET_COLLECTION:
		case J_OPERATION_COLLECTION_GET_ITEM:
		case J_OPERATION_ITEM_GET_STATUS:
		case J_OPERATION_ITEM_READ:
			ret = FALSE;
			break;

		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);

	return ret;
}

void
j_operation_cache_init (void)
{
	JOperationCache* cache;

	g_return_if_fail(j_operation_cache == NULL);

	j_trace_enter(G_STRFUNC);

	cache = g_slice_new(JOperationCache);
	cache->cache = j_cache_new(J_MIB(50));
	cache->queue = g_async_queue_new_full((GDestroyNotify)j_batch_unref);
	cache->thread = g_thread_new("JOperationCache", j_operation_cache_thread, cache);
	cache->working = FALSE;

	g_mutex_init(cache->mutex);
	g_cond_init(cache->cond);

	g_atomic_pointer_set(&j_operation_cache, cache);

	j_trace_leave(G_STRFUNC);
}

void
j_operation_cache_fini (void)
{
	JOperationCache* cache;

	g_return_if_fail(j_operation_cache != NULL);

	j_trace_enter(G_STRFUNC);

	j_operation_cache_flush();

	cache = g_atomic_pointer_get(&j_operation_cache);
	g_atomic_pointer_set(&j_operation_cache, NULL);

	/* push fake batch */
	g_async_queue_push(cache->queue, cache);
	g_thread_join(cache->thread);

	g_async_queue_unref(cache->queue);
	j_cache_free(cache->cache);

	g_cond_clear(cache->cond);
	g_mutex_clear(cache->mutex);

	g_slice_free(JOperationCache, cache);

	j_trace_leave(G_STRFUNC);
}

gboolean
j_operation_cache_flush (void)
{
	gboolean ret = TRUE;

	j_trace_enter(G_STRFUNC);

	g_mutex_lock(j_operation_cache->mutex);

	while (j_operation_cache->working)
	{
		g_cond_wait(j_operation_cache->cond, j_operation_cache->mutex);
	}

	g_mutex_unlock(j_operation_cache->mutex);

	j_trace_leave(G_STRFUNC);

	return ret;
}

gboolean
j_operation_cache_add (JBatch* batch)
{
	gboolean ret = TRUE;
	JList* operations;
	JListIterator* iterator;
	gboolean can_cache = TRUE;

	j_trace_enter(G_STRFUNC);

	operations = j_batch_get_operations(batch);
	iterator = j_list_iterator_new(operations);

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);

		can_cache = j_operation_cache_test(operation) && can_cache;

		if (!can_cache)
		{
			ret = FALSE;
			break;
		}
	}

	j_list_iterator_free(iterator);

	if (!ret)
	{
		goto end;
	}

	iterator = j_list_iterator_new(operations);

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);

		if (operation->type == J_OPERATION_ITEM_WRITE)
		{
			gpointer data;

			// FIXME never cleared
			data = j_cache_put(j_operation_cache->cache, operation->u.item_write.data, operation->u.item_write.length);

			if (data == NULL)
			{
				ret = FALSE;
				break;
			}

			operation->u.item_write.data = data;
		}
	}

	j_list_iterator_free(iterator);

	if (!ret)
	{
		goto end;
	}

	g_mutex_lock(j_operation_cache->mutex);
	j_operation_cache->working = TRUE;
	g_mutex_unlock(j_operation_cache->mutex);

	g_async_queue_push(j_operation_cache->queue, j_batch_copy(batch));

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}
