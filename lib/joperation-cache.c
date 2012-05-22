/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <glib.h>

#include <joperation-cache-internal.h>

#include <jbackground-operation-internal.h>
#include <jcache-internal.h>
#include <jcommon-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation.h>
#include <joperation-internal.h>
#include <joperation-part-internal.h>

/**
 * \defgroup JOperationCache Operation Cache
 *
 * @{
 **/

struct JOperationCache
{
	JCache* cache;
	JList* list;
};

typedef struct JOperationCache JOperationCache;

static JOperationCache* j_operation_cache = NULL;

void
j_operation_cache_init (void)
{
	JOperationCache* cache;

	g_return_if_fail(j_operation_cache == NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	cache = g_slice_new(JOperationCache);
	cache->cache = j_cache_new(J_MIB(50));
	cache->list = j_list_new((JListFreeFunc)j_operation_unref);

	g_atomic_pointer_set(&j_operation_cache, cache);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

void
j_operation_cache_fini (void)
{
	JOperationCache* cache;

	g_return_if_fail(j_operation_cache != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	j_operation_cache_flush();

	cache = g_atomic_pointer_get(&j_operation_cache);
	g_atomic_pointer_set(&j_operation_cache, NULL);

	j_list_unref(cache->list);
	j_cache_free(cache->cache);

	g_slice_free(JOperationCache, cache);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

gboolean
j_operation_cache_flush (void)
{
	JListIterator* iterator;
	gboolean ret = TRUE;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	iterator = j_list_iterator_new(j_operation_cache->list);

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);

		ret = j_operation_execute_internal(operation) && ret;
	}

	j_list_iterator_free(iterator);

	j_list_delete_all(j_operation_cache->list);
	j_cache_clear(j_operation_cache->cache);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

static
gboolean
j_operation_cache_test (JOperationPart* part)
{
	gboolean ret = FALSE;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	switch (part->type)
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

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

gboolean
j_operation_cache_add (JOperation* operation)
{
	gboolean ret = TRUE;
	JList* operation_parts;
	JListIterator* iterator;
	gboolean can_cache = TRUE;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	operation_parts = j_operation_get_parts(operation);
	iterator = j_list_iterator_new(operation_parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);

		can_cache = j_operation_cache_test(part) && can_cache;

		if (!can_cache)
		{
			break;
		}
	}

	j_list_iterator_free(iterator);

	if (!can_cache)
	{
		ret = FALSE;
		goto end;
	}

	iterator = j_list_iterator_new(operation_parts);

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);

		if (part->type == J_OPERATION_ITEM_WRITE)
		{
			gpointer data;

			data = j_cache_put(j_operation_cache->cache, part->u.item_write.data, part->u.item_write.length);

			if (data == NULL)
			{
				ret = FALSE;
				goto end;
			}

			part->u.item_write.data = data;
		}
	}

	j_list_iterator_free(iterator);

	j_list_append(j_operation_cache->list, j_operation_ref(operation));

end:
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}
