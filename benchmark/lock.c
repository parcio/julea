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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>

#include <jlock-internal.h>

#include "benchmark.h"

static
gchar*
_benchmark_lock (gboolean acquire, gboolean add)
{
	guint const n = (add) ? 1500 : 3000;

	JCollection* collection;
	JItem* item;
	JList* list;
	JListIterator* iterator;
	JLock* lock;
	JBatch* operation;
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	list = j_list_new((JListFreeFunc)j_lock_free);
	semantics = j_benchmark_get_semantics();
	operation = j_batch_new(semantics);

	store = j_store_new("benchmark-store");
	collection = j_collection_new("benchmark-collection");
	item = j_item_new("benchmark-item");

	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_collection_create_item(collection, item, operation);
	j_batch_execute(operation);

	if (acquire)
	{
		j_benchmark_timer_start();
	}

	for (guint i = 0; i < n; i++)
	{
		lock = j_lock_new(item);

		if (add)
		{
			for (guint j = 0; j < n; j++)
			{
				j_lock_add(lock, j);
			}
		}

		j_lock_acquire(lock);
		j_list_append(list, lock);
	}

	if (acquire)
	{
		elapsed = j_benchmark_timer_elapsed();
	}

	iterator = j_list_iterator_new(list);

	if (!acquire)
	{
		j_benchmark_timer_start();
	}

	while (j_list_iterator_next(iterator))
	{
		lock = j_list_iterator_get(iterator);

		j_lock_release(lock);
	}

	if (!acquire)
	{
		elapsed = j_benchmark_timer_elapsed();
	}

	j_list_iterator_free(iterator);

	j_collection_delete_item(collection, item, operation);
	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);
	j_batch_execute(operation);

	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
	j_batch_unref(operation);
	j_semantics_unref(semantics);
	j_list_unref(list);

	return g_strdup_printf("%f seconds (%f/s)", elapsed, (gdouble)n / elapsed);
}

static
gchar*
benchmark_lock_acquire (void)
{
	return _benchmark_lock(TRUE, FALSE);
}

static
gchar*
benchmark_lock_release (void)
{
	return _benchmark_lock(FALSE, FALSE);
}

static
gchar*
benchmark_lock_add (void)
{
	return _benchmark_lock(TRUE, TRUE);
}

void
benchmark_lock (void)
{
	j_benchmark_run("/lock/acquire", benchmark_lock_acquire);
	j_benchmark_run("/lock/release", benchmark_lock_release);
	j_benchmark_run("/lock/add", benchmark_lock_add);
}
