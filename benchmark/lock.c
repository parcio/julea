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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>

#include <jlock-internal.h>

#include "benchmark.h"

static
void
_benchmark_lock (BenchmarkResult* result, gboolean acquire, gboolean add)
{
	guint const n = 3000;

	JCollection* collection;
	JItem* item;
	JList* list;
	JListIterator* iterator;
	JLock* lock;
	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	list = j_list_new((JListFreeFunc)j_lock_free);
	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark-collection", batch);
	item = j_item_create(collection, "benchmark-item", NULL, batch);
	j_batch_execute(batch);

	if (acquire)
	{
		j_benchmark_timer_start();
	}

	for (guint i = 0; i < n; i++)
	{
		lock = j_lock_new(item);

		if (add)
		{
			for (guint j = 0; j < 10; j++)
			{
				/* Avoid overlap. */
				j_lock_add(lock, n + (i * 10) + j);
			}
		}

		j_lock_add(lock, i);
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

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_unref(batch);
	j_semantics_unref(semantics);
	j_list_unref(list);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_lock_acquire (BenchmarkResult* result)
{
	_benchmark_lock(result, TRUE, FALSE);
}

static
void
benchmark_lock_release (BenchmarkResult* result)
{
	_benchmark_lock(result, FALSE, FALSE);
}

static
void
benchmark_lock_add (BenchmarkResult* result)
{
	_benchmark_lock(result, TRUE, TRUE);
}

void
benchmark_lock (void)
{
	j_benchmark_run("/lock/acquire", benchmark_lock_acquire);
	j_benchmark_run("/lock/release", benchmark_lock_release);
	j_benchmark_run("/lock/add", benchmark_lock_add);
}
