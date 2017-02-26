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

#include "benchmark.h"

static
void
_benchmark_item_create (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 100000 : 1000;

	JCollection* collection;
	JBatch* delete_batch;
	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);
		g_free(name);

		j_item_delete(collection, item, delete_batch);
		j_item_unref(item);

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

	j_collection_delete(collection, delete_batch);
	j_collection_unref(collection);
	j_batch_execute(delete_batch);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_item_create (BenchmarkResult* result)
{
	_benchmark_item_create(result, FALSE);
}

static
void
benchmark_item_create_batch (BenchmarkResult* result)
{
	_benchmark_item_create(result, TRUE);
}

static
void
_benchmark_item_delete (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 10000;

	JCollection* collection;
	JBatch* get_batch;
	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	get_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);
		g_free(name);

		j_item_unref(item);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		j_item_get(collection, &item, name, get_batch);
		j_batch_execute(get_batch);
		g_free(name);

		j_item_delete(collection, item, batch);
		j_item_unref(item);

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

	j_collection_delete(collection, batch);
	j_collection_unref(collection);
	j_batch_execute(batch);

	j_batch_unref(get_batch);
	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_item_delete (BenchmarkResult* result)
{
	_benchmark_item_delete(result, FALSE);
}

static
void
benchmark_item_delete_batch (BenchmarkResult* result)
{
	_benchmark_item_delete(result, TRUE);
}

static
void
benchmark_item_delete_batch_without_get (BenchmarkResult* result)
{
	guint const n = 10000;

	JCollection* collection;
	JBatch* delete_batch;
	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);
		g_free(name);

		j_item_delete(collection, item, delete_batch);
		j_item_unref(item);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	j_batch_execute(delete_batch);

	elapsed = j_benchmark_timer_elapsed();

	j_collection_delete(collection, batch);
	j_collection_unref(collection);
	j_batch_execute(batch);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
_benchmark_item_get_status (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 1000 : 1000;

	JCollection* collection;
	JItem* item;
	JBatch* batch;
	JSemantics* semantics;
	gchar dummy[1];
	gdouble elapsed;
	guint64 nb;

	memset(dummy, 0, 1);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	item = j_item_create(collection, "benchmark", NULL, batch);
	j_item_write(item, dummy, 1, 0, &nb, batch);

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_item_get_status(item, J_ITEM_STATUS_ALL, batch);

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

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_execute(batch);

	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_item_get_status (BenchmarkResult* result)
{
	_benchmark_item_get_status(result, FALSE);
}

static
void
benchmark_item_get_status_batch (BenchmarkResult* result)
{
	_benchmark_item_get_status(result, TRUE);
}

static
void
_benchmark_item_read (BenchmarkResult* result, gboolean use_batch, guint block_size)
{
	guint const n = (use_batch) ? 25000 : 25000;

	JCollection* collection;
	JItem* item;
	JBatch* batch;
	JSemantics* semantics;
	gchar dummy[block_size];
	gdouble elapsed;
	guint64 nb = 0;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	item = j_item_create(collection, "benchmark", NULL, batch);

	for (guint i = 0; i < n; i++)
	{
		j_item_write(item, dummy, block_size, i * block_size, &nb, batch);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_item_read(item, dummy, block_size, i * block_size, &nb, batch);

		if (!use_batch)
		{
			j_batch_execute(batch);

			g_assert_cmpuint(nb, ==, block_size);
		}
	}

	if (use_batch)
	{
		j_batch_execute(batch);

		g_assert_cmpuint(nb, ==, n * block_size);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_execute(batch);

	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
	result->bytes = n * block_size;
}

static
void
benchmark_item_read (BenchmarkResult* result)
{
	_benchmark_item_read(result, FALSE, 4 * 1024);
}

static
void
benchmark_item_read_batch (BenchmarkResult* result)
{
	_benchmark_item_read(result, TRUE, 4 * 1024);
}

static
void
_benchmark_item_write (BenchmarkResult* result, gboolean use_batch, guint block_size)
{
	guint const n = (use_batch) ? 25000 : 25000;

	JCollection* collection;
	JItem* item;
	JBatch* batch;
	JSemantics* semantics;
	gchar dummy[block_size];
	gdouble elapsed;
	guint64 nb = 0;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	item = j_item_create(collection, "benchmark", NULL, batch);
	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_item_write(item, &dummy, block_size, i * block_size, &nb, batch);

		if (!use_batch)
		{
			j_batch_execute(batch);

			g_assert_cmpuint(nb, ==, block_size);
		}
	}

	if (use_batch)
	{
		j_batch_execute(batch);

		g_assert_cmpuint(nb, ==, n * block_size);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_execute(batch);

	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
	result->bytes = n * block_size;
}

static
void
benchmark_item_write (BenchmarkResult* result)
{
	_benchmark_item_write(result, FALSE, 4 * 1024);
}

static
void
benchmark_item_write_batch (BenchmarkResult* result)
{
	_benchmark_item_write(result, TRUE, 4 * 1024);
}

static
void
_benchmark_item_unordered_create_delete (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 5000 : 5000;

	JCollection* collection;
	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);
		g_free(name);

		j_item_delete(collection, item, batch);
		j_item_unref(item);

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

	j_collection_delete(collection, batch);
	j_collection_unref(collection);
	j_batch_execute(batch);

	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_item_unordered_create_delete (BenchmarkResult* result)
{
	_benchmark_item_unordered_create_delete(result, FALSE);
}

static
void
benchmark_item_unordered_create_delete_batch (BenchmarkResult* result)
{
	_benchmark_item_unordered_create_delete(result, TRUE);
}

void
benchmark_item (void)
{
	j_benchmark_run("/item/create", benchmark_item_create);
	j_benchmark_run("/item/create-batch", benchmark_item_create_batch);
	j_benchmark_run("/item/delete", benchmark_item_delete);
	j_benchmark_run("/item/delete-batch", benchmark_item_delete_batch);
	j_benchmark_run("/item/delete-batch-without-get", benchmark_item_delete_batch_without_get);
	j_benchmark_run("/item/get-status", benchmark_item_get_status);
	j_benchmark_run("/item/get-status-batch", benchmark_item_get_status_batch);
	/* FIXME get */
	j_benchmark_run("/item/read", benchmark_item_read);
	j_benchmark_run("/item/read-batch", benchmark_item_read_batch);
	j_benchmark_run("/item/write", benchmark_item_write);
	j_benchmark_run("/item/write-batch", benchmark_item_write_batch);

	j_benchmark_run("/item/unordered-create-delete", benchmark_item_unordered_create_delete);
	j_benchmark_run("/item/unordered-create-delete-batch", benchmark_item_unordered_create_delete_batch);
}
