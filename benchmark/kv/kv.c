/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>
#include <julea-kv.h>

#include "benchmark.h"

static
void
_benchmark_kv_put (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 200000;

	JBatch* delete_batch;
	JBatch* batch;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	bson_t* empty;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JKV* object;
		gchar* name;

		// FIXME
		empty = g_slice_new(bson_t);
		bson_init(empty);

		name = g_strdup_printf("benchmark-%d", i);
		object = j_kv_new(0, "benchmark", name);
		j_kv_put(object, empty, batch);
		g_free(name);

		j_kv_delete(object, delete_batch);
		j_kv_unref(object);

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

	j_batch_execute(delete_batch);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_kv_put (BenchmarkResult* result)
{
	_benchmark_kv_put(result, FALSE);
}

static
void
benchmark_kv_put_batch (BenchmarkResult* result)
{
	_benchmark_kv_put(result, TRUE);
}

static
void
_benchmark_kv_delete (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 200000;

	JBatch* batch;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	bson_t* empty;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	for (guint i = 0; i < n; i++)
	{
		JKV* object;
		gchar* name;

		// FIXME
		empty = g_slice_new(bson_t);
		bson_init(empty);

		name = g_strdup_printf("benchmark-%d", i);
		object = j_kv_new(0, "benchmark", name);
		j_kv_put(object, empty, batch);
		g_free(name);

		j_kv_unref(object);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JKV* object;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		object = j_kv_new(0, "benchmark", name);
		g_free(name);

		j_kv_delete(object, batch);
		j_kv_unref(object);

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

	j_batch_execute(batch);

	j_batch_unref(batch);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_kv_delete (BenchmarkResult* result)
{
	_benchmark_kv_delete(result, FALSE);
}

static
void
benchmark_kv_delete_batch (BenchmarkResult* result)
{
	_benchmark_kv_delete(result, TRUE);
}

static
void
_benchmark_kv_unordered_put_delete (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 100000;

	JBatch* batch;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	bson_t* empty;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JKV* object;
		gchar* name;

		// FIXME
		empty = g_slice_new(bson_t);
		bson_init(empty);

		name = g_strdup_printf("benchmark-%d", i);
		object = j_kv_new(0, "benchmark", name);
		j_kv_put(object, empty, batch);
		g_free(name);

		j_kv_delete(object, batch);
		j_kv_unref(object);

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

	j_batch_execute(batch);

	j_batch_unref(batch);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_kv_unordered_put_delete (BenchmarkResult* result)
{
	_benchmark_kv_unordered_put_delete(result, FALSE);
}

static
void
benchmark_kv_unordered_put_delete_batch (BenchmarkResult* result)
{
	_benchmark_kv_unordered_put_delete(result, TRUE);
}

void
benchmark_kv (void)
{
	j_benchmark_run("/kv/put", benchmark_kv_put);
	j_benchmark_run("/kv/put-batch", benchmark_kv_put_batch);
	j_benchmark_run("/kv/delete", benchmark_kv_delete);
	j_benchmark_run("/kv/delete-batch", benchmark_kv_delete_batch);
	j_benchmark_run("/kv/unordered-put-delete", benchmark_kv_unordered_put_delete);
	j_benchmark_run("/kv/unordered-put-delete-batch", benchmark_kv_unordered_put_delete_batch);
}
