/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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
#include <julea-item.h>

#include "benchmark.h"

static void
_benchmark_collection_create(BenchmarkRun* run, gboolean use_batch)
{
	guint const n = 1000;

	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	while (j_benchmark_iterate(run))
	{
		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JCollection) collection = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			collection = j_collection_create(name, batch);

			j_collection_delete(collection, delete_batch);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}

		j_benchmark_timer_stop(run);

		ret = j_batch_execute(delete_batch);
		g_assert_true(ret);
	}

	run->operations = n;
}

static void
benchmark_collection_create(BenchmarkRun* run)
{
	_benchmark_collection_create(run, FALSE);
}

static void
benchmark_collection_create_batch(BenchmarkRun* run)
{
	_benchmark_collection_create(run, TRUE);
}

static void
_benchmark_collection_delete(BenchmarkRun* run, gboolean use_batch)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JCollection) collection = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			collection = j_collection_create(name, batch);
		}

		ret = j_batch_execute(batch);
		g_assert_true(ret);

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JCollection) collection = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			j_collection_get(&collection, name, batch);
			ret = j_batch_execute(batch);
			g_assert_true(ret);

			j_collection_delete(collection, batch);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}

		j_benchmark_timer_stop(run);
	}

	run->operations = n;
}

static void
benchmark_collection_delete(BenchmarkRun* run)
{
	_benchmark_collection_delete(run, FALSE);
}

static void
benchmark_collection_delete_batch(BenchmarkRun* run)
{
	_benchmark_collection_delete(run, TRUE);
}

static void
benchmark_collection_delete_batch_without_get(BenchmarkRun* run)
{
	guint const n = 1000;

	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JCollection) collection = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			collection = j_collection_create(name, batch);

			j_collection_delete(collection, delete_batch);
		}

		ret = j_batch_execute(batch);
		g_assert_true(ret);

		j_benchmark_timer_start(run);

		ret = j_batch_execute(delete_batch);
		g_assert_true(ret);

		j_benchmark_timer_stop(run);
	}

	run->operations = n;
}

static void
_benchmark_collection_unordered_create_delete(BenchmarkRun* run, gboolean use_batch)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JCollection) collection = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			collection = j_collection_create(name, batch);
			j_collection_delete(collection, batch);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	j_benchmark_timer_stop(run);

	run->operations = n * 2;
}

static void
benchmark_collection_unordered_create_delete(BenchmarkRun* run)
{
	_benchmark_collection_unordered_create_delete(run, FALSE);
}

static void
benchmark_collection_unordered_create_delete_batch(BenchmarkRun* run)
{
	_benchmark_collection_unordered_create_delete(run, TRUE);
}

void
benchmark_collection(void)
{
	j_benchmark_add("/item/collection/create", benchmark_collection_create);
	j_benchmark_add("/item/collection/create-batch", benchmark_collection_create_batch);
	j_benchmark_add("/item/collection/delete", benchmark_collection_delete);
	j_benchmark_add("/item/collection/delete-batch", benchmark_collection_delete_batch);
	j_benchmark_add("/item/collection/delete-batch-without-get", benchmark_collection_delete_batch_without_get);
	j_benchmark_add("/item/collection/unordered-create-delete", benchmark_collection_unordered_create_delete);
	j_benchmark_add("/item/collection/unordered-create-delete-batch", benchmark_collection_unordered_create_delete_batch);
	/// \todo get
}
