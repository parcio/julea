/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include <julea.h>
#include <jclient-item.h>

#include "benchmark.h"

static
void
_benchmark_collection_create (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 100000 : 1000;

	JBatch* delete_batch;
	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		collection = j_collection_create(name, batch);
		g_free(name);

		j_collection_delete(collection, delete_batch);
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
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		collection = j_collection_create(name, batch);
		g_free(name);

		j_collection_unref(collection);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		j_collection_get(&collection, name, batch);
		j_batch_execute(batch);
		g_free(name);

		j_collection_delete(collection, batch);
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
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		collection = j_collection_create(name, batch);
		g_free(name);

		j_collection_delete(collection, delete_batch);
		j_collection_unref(collection);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	j_batch_execute(delete_batch);

	elapsed = j_benchmark_timer_elapsed();

	j_batch_execute(batch);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
_benchmark_collection_unordered_create_delete (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 5000 : 5000;

	JBatch* batch;
	JSemantics* semantics;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("benchmark-%d", i);
		collection = j_collection_create(name, batch);
		g_free(name);

		j_collection_delete(collection, batch);
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

	j_batch_execute(batch);

	j_batch_unref(batch);
	j_semantics_unref(semantics);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_collection_unordered_create_delete (BenchmarkResult* result)
{
	_benchmark_collection_unordered_create_delete(result, FALSE);
}

static
void
benchmark_collection_unordered_create_delete_batch (BenchmarkResult* result)
{
	_benchmark_collection_unordered_create_delete(result, TRUE);
}

void
benchmark_collection (void)
{
	j_benchmark_run("/collection/create", benchmark_collection_create);
	j_benchmark_run("/collection/create-batch", benchmark_collection_create_batch);
	j_benchmark_run("/collection/delete", benchmark_collection_delete);
	j_benchmark_run("/collection/delete-batch", benchmark_collection_delete_batch);
	j_benchmark_run("/collection/delete-batch-without-get", benchmark_collection_delete_batch_without_get);

	j_benchmark_run("/collection/unordered-create-delete", benchmark_collection_unordered_create_delete);
	j_benchmark_run("/collection/unordered-create-delete-batch", benchmark_collection_unordered_create_delete_batch);
	/* FIXME get */
}
