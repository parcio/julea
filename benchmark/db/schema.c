/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2020 Michael Kuhn
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
#include <julea-db.h>

#include "benchmark.h"

static void
_benchmark_db_schema_create(BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 100;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);
	delete_batch = j_batch_new(semantics);

	while (j_benchmark_iterate())
	{
		j_benchmark_timer_start();

		for (guint i = 0; i < n; i++)
		{
			g_autoptr(GError) error = NULL;
			g_autoptr(JDBSchema) schema = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-schema-%d", i);
			schema = j_db_schema_new("benchmark-ns", name, &error);

			for (guint j = 0; j < 10; j++)
			{
				g_autofree gchar* fname = NULL;

				fname = g_strdup_printf("field%d", j);
				j_db_schema_add_field(schema, fname, J_DB_TYPE_STRING, &error);
				//j_db_schema_add_index(schema, idx_file, &error);
			}

			// FIXME Do not pass error, will not exist anymore when batch is executed
			j_db_schema_create(schema, batch, NULL);
			j_db_schema_delete(schema, delete_batch, NULL);

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

		j_benchmark_timer_stop();

		ret = j_batch_execute(delete_batch);
		g_assert_true(ret);
	}

	result->operations = n;
}

static void
benchmark_db_schema_create(BenchmarkResult* result)
{
	_benchmark_db_schema_create(result, FALSE);
}

static void
benchmark_db_schema_create_batch(BenchmarkResult* result)
{
	_benchmark_db_schema_create(result, TRUE);
}

static void
_benchmark_db_schema_delete(BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 100;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	while (j_benchmark_iterate())
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JDBSchema) schema = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-schema-%d", i);
			schema = j_db_schema_new("benchmark-ns", name, NULL);

			for (guint j = 0; j < 10; j++)
			{
				g_autofree gchar* fname = NULL;

				fname = g_strdup_printf("field%d", j);
				j_db_schema_add_field(schema, fname, J_DB_TYPE_STRING, NULL);
				//j_db_schema_add_index(schema, idx_file, &error);
			}

			// FIXME Do not pass error, will not exist anymore when batch is executed
			j_db_schema_create(schema, batch, NULL);
		}

		ret = j_batch_execute(batch);
		g_assert_true(ret);

		j_benchmark_timer_start();

		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JDBSchema) schema = NULL;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-schema-%d", i);
			schema = j_db_schema_new("benchmark-ns", name, NULL);

			// FIXME Do not pass error, will not exist anymore when batch is executed
			j_db_schema_delete(schema, batch, NULL);

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

		j_benchmark_timer_stop();
	}

	result->operations = n;
}

static void
benchmark_db_schema_delete(BenchmarkResult* result)
{
	_benchmark_db_schema_delete(result, FALSE);
}

static void
benchmark_db_schema_delete_batch(BenchmarkResult* result)
{
	_benchmark_db_schema_delete(result, TRUE);
}

void
benchmark_db_schema(void)
{
	j_benchmark_run("/db/schema/create", benchmark_db_schema_create);
	j_benchmark_run("/db/schema/create-batch", benchmark_db_schema_create_batch);
	j_benchmark_run("/db/schema/delete", benchmark_db_schema_delete);
	j_benchmark_run("/db/schema/delete-batch", benchmark_db_schema_delete_batch);
}
