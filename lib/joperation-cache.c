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

#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation.h>
#include <joperation-part-internal.h>

/**
 * \defgroup JOperationCache Operation Cache
 * @{
 **/

struct JOperationCache
{
	JList* list;
};

static JOperationCache* j_operation_cache = NULL;

void
j_operation_cache_init (void)
{
	JOperationCache* cache;

	g_return_if_fail(j_operation_cache == NULL);

	cache = g_slice_new(JOperationCache);
	cache->list = j_list_new((JListFreeFunc)j_operation_part_free);

	g_atomic_pointer_set(&j_operation_cache, cache);
}

void
j_operation_cache_fini (void)
{
	JOperationCache* cache;
	JListIterator* iterator;

	g_return_if_fail(j_operation_cache != NULL);

	cache = g_atomic_pointer_get(&j_operation_cache);
	g_atomic_pointer_set(&j_operation_cache, NULL);

	iterator = j_list_iterator_new(cache->list);

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);

		j_operation_execute(operation);
	}

	j_list_iterator_free(iterator);

	j_list_unref(cache->list);
	g_slice_free(JOperationCache, cache);
}

void
j_operation_cache_add (JOperationPart* part)
{
	j_list_append(j_operation_cache->list, part);
}

/**
 * @}
 **/
