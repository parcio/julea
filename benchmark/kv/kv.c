/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2021 Michael Kuhn
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

static void
_benchmark_kv_put(BenchmarkRun* run, gboolean use_batch)
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
			g_autoptr(JKV) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_kv_new("benchmark", name);
			j_kv_put(object, g_strdup("empty"), 6, g_free, batch);

			j_kv_delete(object, delete_batch);

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
benchmark_kv_put(BenchmarkRun* run)
{
	_benchmark_kv_put(run, FALSE);
}

static void
benchmark_kv_put_batch(BenchmarkRun* run)
{
	_benchmark_kv_put(run, TRUE);
}

static void
_benchmark_kv_get_callback(gpointer value, guint32 len, gpointer data)
{
	(void)len;
	(void)data;

	g_free(value);
}

static void
_benchmark_kv_get(BenchmarkRun* run, gboolean use_batch)
{
	guint const n = 1000;

	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) object = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		object = j_kv_new("benchmark", name);
		j_kv_put(object, g_strdup(name), strlen(name), g_free, batch);

		j_kv_delete(object, delete_batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JKV) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_kv_new("benchmark", name);
			j_kv_get_callback(object, _benchmark_kv_get_callback, NULL, batch);

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

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	run->operations = n;
}

static void
benchmark_kv_get(BenchmarkRun* run)
{
	_benchmark_kv_get(run, FALSE);
}

static void
benchmark_kv_get_batch(BenchmarkRun* run)
{
	_benchmark_kv_get(run, TRUE);
}

static void
_benchmark_kv_delete(BenchmarkRun* run, gboolean use_batch)
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
			g_autoptr(JKV) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_kv_new("benchmark", name);
			j_kv_put(object, g_strdup("empty"), 6, g_free, batch);
		}

		ret = j_batch_execute(batch);
		g_assert_true(ret);

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JKV) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_kv_new("benchmark", name);

			j_kv_delete(object, batch);

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
benchmark_kv_delete(BenchmarkRun* run)
{
	_benchmark_kv_delete(run, FALSE);
}

static void
benchmark_kv_delete_batch(BenchmarkRun* run)
{
	_benchmark_kv_delete(run, TRUE);
}

static void
_benchmark_kv_unordered_put_delete(BenchmarkRun* run, gboolean use_batch)
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
			g_autoptr(JKV) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_kv_new("benchmark", name);
			j_kv_put(object, g_strdup("empty"), 6, g_free, batch);
			j_kv_delete(object, batch);

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
benchmark_kv_unordered_put_delete(BenchmarkRun* run)
{
	_benchmark_kv_unordered_put_delete(run, FALSE);
}

static void
benchmark_kv_unordered_put_delete_batch(BenchmarkRun* run)
{
	_benchmark_kv_unordered_put_delete(run, TRUE);
}

void
benchmark_kv(void)
{
	j_benchmark_add("/kv/put", benchmark_kv_put);
	j_benchmark_add("/kv/put-batch", benchmark_kv_put_batch);
	j_benchmark_add("/kv/get", benchmark_kv_get);
	j_benchmark_add("/kv/get-batch", benchmark_kv_get_batch);
	j_benchmark_add("/kv/delete", benchmark_kv_delete);
	j_benchmark_add("/kv/delete-batch", benchmark_kv_delete_batch);
	j_benchmark_add("/kv/unordered-put-delete", benchmark_kv_unordered_put_delete);
	j_benchmark_add("/kv/unordered-put-delete-batch", benchmark_kv_unordered_put_delete_batch);
}
