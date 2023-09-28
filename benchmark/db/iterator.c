/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019-2020 Michael Straßberger
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
#include <julea-db.h>

#include "benchmark.h"

#include "common.c"

static void
_benchmark_db_get_simple(BenchmarkRun* run, gchar const* namespace, gboolean use_index_all, gboolean use_index_single)
{
	gboolean ret;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	g_autoptr(GError) b_s_error = NULL;
	g_autoptr(JDBSchema) b_scheme = NULL;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	b_scheme = _benchmark_db_prepare_scheme(namespace, false, use_index_all, use_index_single, batch, delete_batch);

	g_assert_nonnull(b_scheme);
	g_assert_nonnull(run);

	_benchmark_db_insert(NULL, b_scheme, NULL, true, false, false, false);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (gint i = 0; i < ((use_index_all || use_index_single) ? N : (N / N_GET_DIVIDER)); i++)
		{
			JDBType field_type;
			g_autofree gpointer field_value;
			guint64 field_length;
			g_autoptr(JDBIterator) iterator;
			g_autofree gchar* string = _benchmark_db_get_identifier(i);
			g_autoptr(JDBSelector) selector = j_db_selector_new(b_scheme, J_DB_SELECTOR_MODE_AND, &b_s_error);

			ret = j_db_selector_add_field(selector, "string", J_DB_SELECTOR_OPERATOR_EQ, string, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			iterator = j_db_iterator_new(b_scheme, selector, &b_s_error);
			g_assert_nonnull(iterator);
			g_assert_null(b_s_error);

			ret = j_db_iterator_next(iterator, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_iterator_get_field(iterator, NULL, "string", &field_type, &field_value, &field_length, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);
		}
	}

	j_benchmark_timer_stop(run);

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	run->operations = ((use_index_all || use_index_single) ? N : (N / N_GET_DIVIDER));
}

static void
_benchmark_db_get_range(BenchmarkRun* run, gchar const* namespace, gboolean use_index_all, gboolean use_index_single)
{
	gboolean ret;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	g_autoptr(GError) b_s_error = NULL;
	g_autoptr(JDBSchema) b_scheme = NULL;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	b_scheme = _benchmark_db_prepare_scheme(namespace, false, use_index_all, use_index_single, batch, delete_batch);

	g_assert_nonnull(b_scheme);
	g_assert_nonnull(run);

	_benchmark_db_insert(NULL, b_scheme, NULL, true, false, false, false);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (gint i = 0; i < N_GET_DIVIDER; i++)
		{
			JDBType field_type;
			g_autofree gpointer field_value;
			guint64 field_length;

			gint64 range_begin = (i * (CLASS_MODULUS / N_GET_DIVIDER)) - CLASS_LIMIT;
			gint64 range_end = ((i + 1) * (CLASS_MODULUS / N_GET_DIVIDER)) - (CLASS_LIMIT + 1);

			g_autoptr(JDBIterator) iterator;
			g_autoptr(JDBSelector) selector = j_db_selector_new(b_scheme, J_DB_SELECTOR_MODE_AND, &b_s_error);

			ret = j_db_selector_add_field(selector, "sint", J_DB_SELECTOR_OPERATOR_GE, &range_begin, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_selector_add_field(selector, "sint", J_DB_SELECTOR_OPERATOR_LE, &range_end, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			iterator = j_db_iterator_new(b_scheme, selector, &b_s_error);
			g_assert_nonnull(iterator);
			g_assert_null(b_s_error);

			ret = j_db_iterator_next(iterator, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_iterator_get_field(iterator, NULL, "string", &field_type, &field_value, &field_length, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);
		}
	}

	j_benchmark_timer_stop(run);

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	run->operations = N_GET_DIVIDER;
}

static void
benchmark_db_get_simple(BenchmarkRun* run)
{
	_benchmark_db_get_simple(run, "benchmark_get_simple", false, false);
}

static void
benchmark_db_get_simple_index_single(BenchmarkRun* run)
{
	_benchmark_db_get_simple(run, "benchmark_get_simple_index_single", false, true);
}

static void
benchmark_db_get_simple_index_all(BenchmarkRun* run)
{
	_benchmark_db_get_simple(run, "benchmark_get_simple_index_all", true, false);
}

static void
benchmark_db_get_simple_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_get_simple(run, "benchmark_get_simple_index_mixed", true, true);
}

static void
benchmark_db_get_range(BenchmarkRun* run)
{
	_benchmark_db_get_range(run, "benchmark_get_range", false, false);
}

static void
benchmark_db_get_range_index_single(BenchmarkRun* run)
{
	_benchmark_db_get_range(run, "benchmark_get_range_index_single", false, true);
}

static void
benchmark_db_get_range_index_all(BenchmarkRun* run)
{
	_benchmark_db_get_range(run, "benchmark_get_range_index_all", true, false);
}

static void
benchmark_db_get_range_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_get_range(run, "benchmark_get_range_index_mixed", true, true);
}

void
benchmark_db_iterator(void)
{
	j_benchmark_add("/db/iterator/get-simple", benchmark_db_get_simple);
	j_benchmark_add("/db/iterator/get-simple-index-single", benchmark_db_get_simple_index_single);
	j_benchmark_add("/db/iterator/get-simple-index-all", benchmark_db_get_simple_index_all);
	j_benchmark_add("/db/iterator/get-simple-index-mixed", benchmark_db_get_simple_index_mixed);
	j_benchmark_add("/db/iterator/get-range", benchmark_db_get_range);
	j_benchmark_add("/db/iterator/get-range-index-single", benchmark_db_get_range_index_single);
	j_benchmark_add("/db/iterator/get-range-index-all", benchmark_db_get_range_index_all);
	j_benchmark_add("/db/iterator/get-range-index-mixed", benchmark_db_get_range_index_mixed);
}
