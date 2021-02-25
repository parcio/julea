/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2020 Michael Kuhn
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
#include <julea-object.h>

#include "benchmark.h"

static void
_benchmark_object_create(BenchmarkRun* run, gboolean use_batch)
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
			g_autoptr(JObject) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_object_new(run->namespace, name);
			j_object_create(object, batch);

			j_object_delete(object, delete_batch);

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
benchmark_object_create(BenchmarkRun* run)
{
	_benchmark_object_create(run, FALSE);
}

static void
benchmark_object_create_batch(BenchmarkRun* run)
{
	_benchmark_object_create(run, TRUE);
}

static void
_benchmark_object_delete(BenchmarkRun* run, gboolean use_batch)
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
			g_autoptr(JObject) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_object_new(run->namespace, name);
			j_object_create(object, batch);
		}

		ret = j_batch_execute(batch);
		g_assert_true(ret);

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JObject) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_object_new(run->namespace, name);

			j_object_delete(object, batch);

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
benchmark_object_delete(BenchmarkRun* run)
{
	_benchmark_object_delete(run, FALSE);
}

static void
benchmark_object_delete_batch(BenchmarkRun* run)
{
	_benchmark_object_delete(run, TRUE);
}

static void
_benchmark_object_status(BenchmarkRun* run, gboolean use_batch)
{
	guint const n = (use_batch) ? 10000 : 1000;

	g_autoptr(JObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gchar dummy[1];
	gint64 modification_time;
	guint64 size;
	gboolean ret;

	memset(dummy, 0, 1);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_object_new(run->namespace, "benchmark");
	j_object_create(object, batch);
	j_object_write(object, dummy, 1, 0, &size, batch);

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			j_object_status(object, &modification_time, &size, batch);

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

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	run->operations = n;
}

static void
benchmark_object_status(BenchmarkRun* run)
{
	_benchmark_object_status(run, FALSE);
}

static void
benchmark_object_status_batch(BenchmarkRun* run)
{
	_benchmark_object_status(run, TRUE);
}

static void
_benchmark_object_read(BenchmarkRun* run, gboolean use_batch, guint block_size)
{
	guint const n = (use_batch) ? 10000 : 1000;

	g_autoptr(JObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gchar dummy[block_size];
	guint64 nb = 0;
	gboolean ret;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_object_new(run->namespace, "benchmark");
	j_object_create(object, batch);

	for (guint i = 0; i < n; i++)
	{
		j_object_write(object, dummy, block_size, i * block_size, &nb, batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nb, ==, n * block_size);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			j_object_read(object, dummy, block_size, i * block_size, &nb, batch);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
				g_assert_cmpuint(nb, ==, block_size);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
			g_assert_cmpuint(nb, ==, n * block_size);
		}
	}

	j_benchmark_timer_stop(run);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	run->operations = n;
	run->bytes = n * block_size;
}

static void
benchmark_object_read(BenchmarkRun* run)
{
	_benchmark_object_read(run, FALSE, 4 * 1024);
}

static void
benchmark_object_read_batch(BenchmarkRun* run)
{
	_benchmark_object_read(run, TRUE, 4 * 1024);
}

static void
_benchmark_object_write(BenchmarkRun* run, gboolean use_batch, guint block_size)
{
	guint const n = (use_batch) ? 10000 : 1000;

	g_autoptr(JObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gchar dummy[block_size];
	guint64 nb = 0;
	gboolean ret;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_object_new(run->namespace, "benchmark");
	j_object_create(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			j_object_write(object, &dummy, block_size, i * block_size, &nb, batch);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
				g_assert_cmpuint(nb, ==, block_size);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
			g_assert_cmpuint(nb, ==, n * block_size);
		}
	}

	j_benchmark_timer_stop(run);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	run->operations = n;
	run->bytes = n * block_size;
}

static void
benchmark_object_write(BenchmarkRun* run)
{
	_benchmark_object_write(run, FALSE, 4 * 1024);
}

static void
benchmark_object_write_batch(BenchmarkRun* run)
{
	_benchmark_object_write(run, TRUE, 4 * 1024);
}

static void
_benchmark_object_unordered_create_delete(BenchmarkRun* run, gboolean use_batch)
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
			g_autoptr(JObject) object = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-%d", i);
			object = j_object_new(run->namespace, name);
			j_object_create(object, batch);
			j_object_delete(object, batch);

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
benchmark_object_unordered_create_delete(BenchmarkRun* run)
{
	_benchmark_object_unordered_create_delete(run, FALSE);
}

static void
benchmark_object_unordered_create_delete_batch(BenchmarkRun* run)
{
	_benchmark_object_unordered_create_delete(run, TRUE);
}

void
benchmark_object(void)
{
	j_benchmark_add("/object/object/create", benchmark_object_create);
	j_benchmark_add("/object/object/create-batch", benchmark_object_create_batch);
	j_benchmark_add("/object/object/delete", benchmark_object_delete);
	j_benchmark_add("/object/object/delete-batch", benchmark_object_delete_batch);
	j_benchmark_add("/object/object/status", benchmark_object_status);
	j_benchmark_add("/object/object/status-batch", benchmark_object_status_batch);
	/* FIXME get */
	j_benchmark_add("/object/object/read", benchmark_object_read);
	j_benchmark_add("/object/object/read-batch", benchmark_object_read_batch);
	j_benchmark_add("/object/object/write", benchmark_object_write);
	j_benchmark_add("/object/object/write-batch", benchmark_object_write_batch);
	j_benchmark_add("/object/object/unordered-create-delete", benchmark_object_unordered_create_delete);
	j_benchmark_add("/object/object/unordered-create-delete-batch", benchmark_object_unordered_create_delete_batch);
}
