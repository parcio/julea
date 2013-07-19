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

#include <julea.h>

#include "benchmark.h"

static
void
_benchmark_collection_create (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 100000 : 1000;

	JBatch* delete_batch;
	JBatch* batch;
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	store = j_create_store("test", batch);
	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		collection = j_store_create_collection(store, name, batch);
		g_free(name);

		j_store_delete_collection(store, collection, delete_batch);
		j_collection_unref(collection);

		if (!use_batch)
		{
			j_batch_execute(batch);
		}
	}

	if (use_batch)
	{
		j_batch_execute(batch);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_delete_store(store, delete_batch);
	j_store_unref(store);
	j_batch_execute(delete_batch);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_collection_create (BenchmarkResult* result)
{
	_benchmark_collection_create(result, FALSE);
}

static
void
benchmark_collection_create_batch (BenchmarkResult* result)
{
	_benchmark_collection_create(result, TRUE);
}

static
void
_benchmark_collection_delete (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 10000;

	JBatch* batch;
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	store = j_create_store("test", batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		collection = j_store_create_collection(store, name, batch);
		g_free(name);

		j_collection_unref(collection);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		j_store_get_collection(store, &collection, name, batch);
		j_batch_execute(batch);
		g_free(name);

		j_store_delete_collection(store, collection, batch);
		j_collection_unref(collection);

		if (!use_batch)
		{
			j_batch_execute(batch);
		}
	}

	if (use_batch)
	{
		j_batch_execute(batch);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_delete_store(store, batch);
	j_store_unref(store);
	j_batch_execute(batch);

	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_collection_delete (BenchmarkResult* result)
{
	_benchmark_collection_delete(result, FALSE);
}

static
void
benchmark_collection_delete_batch (BenchmarkResult* result)
{
	_benchmark_collection_delete(result, TRUE);
}

static
void
benchmark_collection_delete_batch_without_get (BenchmarkResult* result)
{
	guint const n = 10000;

	JBatch* delete_batch;
	JBatch* batch;
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	store = j_create_store("test", batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		collection = j_store_create_collection(store, name, batch);
		g_free(name);

		j_store_delete_collection(store, collection, delete_batch);
		j_collection_unref(collection);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	j_batch_execute(delete_batch);

	elapsed = j_benchmark_timer_elapsed();

	j_delete_store(store, batch);
	j_store_unref(store);
	j_batch_execute(batch);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

void
benchmark_collection (void)
{
	j_benchmark_run("/collection/create", benchmark_collection_create);
	j_benchmark_run("/collection/create-batch", benchmark_collection_create_batch);
	j_benchmark_run("/collection/delete", benchmark_collection_delete);
	j_benchmark_run("/collection/delete-batch", benchmark_collection_delete_batch);
	j_benchmark_run("/collection/delete-batch-without-get", benchmark_collection_delete_batch_without_get);
	/* FIXME get */
}
