/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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
#include <julea-item.h>

#include "benchmark.h"

static
void
_benchmark_item_create (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 100000 : 1000;

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JItem) item = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);

		j_item_delete(item, delete_batch);

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
	j_batch_execute(delete_batch);

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

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JBatch) get_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	get_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JItem) item = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JItem) item = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		j_item_get(collection, &item, name, get_batch);
		j_batch_execute(get_batch);

		j_item_delete(item, batch);

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
	j_batch_execute(batch);

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

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JItem) item = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);

		j_item_delete(item, delete_batch);
	}

	j_batch_execute(batch);

	j_benchmark_timer_start();

	j_batch_execute(delete_batch);

	elapsed = j_benchmark_timer_elapsed();

	j_collection_delete(collection, batch);
	j_batch_execute(batch);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
_benchmark_item_get_status (BenchmarkResult* result, gboolean use_batch)
{
	guint const n = (use_batch) ? 1000 : 1000;

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JItem) item = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
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
		j_item_get_status(item, batch);

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

	j_item_delete(item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

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

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JItem) item = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
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
	g_assert_cmpuint(nb, ==, n * block_size);

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

	j_item_delete(item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

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

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JItem) item = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
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

	j_item_delete(item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

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
	guint const n = 5000;

	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	collection = j_collection_create("benchmark", batch);
	j_batch_execute(batch);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JItem) item = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		item = j_item_create(collection, name, NULL, batch);

		j_item_delete(item, batch);

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
	j_batch_execute(batch);

	result->elapsed_time = elapsed;
	result->operations = n * 2;
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
	j_benchmark_run("/item/item/create", benchmark_item_create);
	j_benchmark_run("/item/item/create-batch", benchmark_item_create_batch);
	j_benchmark_run("/item/item/delete", benchmark_item_delete);
	j_benchmark_run("/item/item/delete-batch", benchmark_item_delete_batch);
	j_benchmark_run("/item/item/delete-batch-without-get", benchmark_item_delete_batch_without_get);
	j_benchmark_run("/item/item/get-status", benchmark_item_get_status);
	j_benchmark_run("/item/item/get-status-batch", benchmark_item_get_status_batch);
	/* FIXME get */
	j_benchmark_run("/item/item/read", benchmark_item_read);
	j_benchmark_run("/item/item/read-batch", benchmark_item_read_batch);
	j_benchmark_run("/item/item/write", benchmark_item_write);
	j_benchmark_run("/item/item/write-batch", benchmark_item_write_batch);

	j_benchmark_run("/item/item/unordered-create-delete", benchmark_item_unordered_create_delete);
	j_benchmark_run("/item/item/unordered-create-delete-batch", benchmark_item_unordered_create_delete_batch);
}
