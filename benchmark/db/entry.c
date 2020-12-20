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
_benchmark_db_delete(BenchmarkRun* run, gchar const* namespace, gboolean use_batch, gboolean use_index_all, gboolean use_index_single)
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

	while (j_benchmark_iterate(run))
	{
		_benchmark_db_insert(NULL, b_scheme, NULL, true, false, false, false);

		j_benchmark_timer_start(run);

		for (gint i = 0; i < ((use_index_all || use_index_single) ? N : (N / N_GET_DIVIDER)); i++)
		{
			g_autoptr(JDBEntry) entry = j_db_entry_new(b_scheme, &b_s_error);
			g_autofree gchar* string = _benchmark_db_get_identifier(i);
			g_autoptr(JDBSelector) selector = j_db_selector_new(b_scheme, J_DB_SELECTOR_MODE_AND, &b_s_error);

			ret = j_db_selector_add_field(selector, "string", J_DB_SELECTOR_OPERATOR_EQ, string, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_entry_delete(entry, selector, batch, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

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

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	run->operations = ((use_index_all || use_index_single) ? N : (N / N_GET_DIVIDER));
}

static void
_benchmark_db_update(BenchmarkRun* run, gchar const* namespace, gboolean use_batch, gboolean use_index_all, gboolean use_index_single)
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
			gint64 i_signed = (((i + N_PRIME) * SIGNED_FACTOR) % CLASS_MODULUS) - CLASS_LIMIT;
			g_autoptr(JDBSelector) selector = j_db_selector_new(b_scheme, J_DB_SELECTOR_MODE_AND, &b_s_error);
			g_autoptr(JDBEntry) entry = j_db_entry_new(b_scheme, &b_s_error);
			g_autofree gchar* string = _benchmark_db_get_identifier(i);

			g_assert_null(b_s_error);

			ret = j_db_entry_set_field(entry, "sint", &i_signed, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_selector_add_field(selector, "string", J_DB_SELECTOR_OPERATOR_EQ, string, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_entry_update(entry, selector, batch, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

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

	run->operations = ((use_index_all || use_index_single) ? N : (N / N_GET_DIVIDER));
}

static void
benchmark_db_insert(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert", false, false, false, true);
}

static void
benchmark_db_insert_batch(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_batch", true, false, false, true);
}

static void
benchmark_db_insert_index_single(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_index_single", false, false, true, true);
}

static void
benchmark_db_insert_batch_index_single(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_batch_index_single", true, false, true, true);
}

static void
benchmark_db_insert_index_all(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_index_all", false, true, false, true);
}

static void
benchmark_db_insert_batch_index_all(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_batch_index_all", true, true, false, true);
}

static void
benchmark_db_insert_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_index_mixed", false, true, true, true);
}

static void
benchmark_db_insert_batch_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_insert(run, NULL, "benchmark_insert_batch_index_mixed", true, true, true, true);
}

static void
benchmark_db_delete(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete", false, false, false);
}

static void
benchmark_db_delete_batch(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_batch", true, false, false);
}

static void
benchmark_db_delete_index_single(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_index_single", false, false, true);
}

static void
benchmark_db_delete_batch_index_single(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_batch_index_single", true, false, true);
}

static void
benchmark_db_delete_index_all(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_index_all", false, true, false);
}

static void
benchmark_db_delete_batch_index_all(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_batch_index_all", true, true, false);
}

static void
benchmark_db_delete_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_index_mixed", false, true, true);
}

static void
benchmark_db_delete_batch_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_delete(run, "benchmark_delete_batch_index_mixed", true, true, true);
}

static void
benchmark_db_update(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update", false, false, false);
}

static void
benchmark_db_update_batch(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_batch", true, false, false);
}

static void
benchmark_db_update_index_single(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_index_single", false, false, true);
}

static void
benchmark_db_update_batch_index_single(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_batch_index_single", true, false, true);
}

static void
benchmark_db_update_index_all(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_index_all", false, true, false);
}

static void
benchmark_db_update_batch_index_all(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_batch_index_all", true, true, false);
}

static void
benchmark_db_update_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_index_mixed", false, true, true);
}

static void
benchmark_db_update_batch_index_mixed(BenchmarkRun* run)
{
	_benchmark_db_update(run, "benchmark_update_batch_index_mixed", true, true, true);
}

void
benchmark_db_entry(void)
{
	j_benchmark_add("/db/entry/insert", benchmark_db_insert);
	j_benchmark_add("/db/entry/insert-batch", benchmark_db_insert_batch);
	j_benchmark_add("/db/entry/insert-index-single", benchmark_db_insert_index_single);
	j_benchmark_add("/db/entry/insert-batch-index-single", benchmark_db_insert_batch_index_single);
	j_benchmark_add("/db/entry/insert-index-all", benchmark_db_insert_index_all);
	j_benchmark_add("/db/entry/insert-batch-index-all", benchmark_db_insert_batch_index_all);
	j_benchmark_add("/db/entry/insert-index-mixed", benchmark_db_insert_index_mixed);
	j_benchmark_add("/db/entry/insert-batch-index-mixed", benchmark_db_insert_batch_index_mixed);
	j_benchmark_add("/db/entry/delete", benchmark_db_delete);
	j_benchmark_add("/db/entry/delete-batch", benchmark_db_delete_batch);
	j_benchmark_add("/db/entry/delete-index-single", benchmark_db_delete_index_single);
	j_benchmark_add("/db/entry/delete-batch-index-single", benchmark_db_delete_batch_index_single);
	j_benchmark_add("/db/entry/delete-index-all", benchmark_db_delete_index_all);
	j_benchmark_add("/db/entry/delete-batch-index-all", benchmark_db_delete_batch_index_all);
	j_benchmark_add("/db/entry/delete-index-mixed", benchmark_db_delete_index_mixed);
	j_benchmark_add("/db/entry/delete-batch-index-mixed", benchmark_db_delete_batch_index_mixed);
	j_benchmark_add("/db/entry/update", benchmark_db_update);
	j_benchmark_add("/db/entry/update-batch", benchmark_db_update_batch);
	j_benchmark_add("/db/entry/update-index-single", benchmark_db_update_index_single);
	j_benchmark_add("/db/entry/update-batch-index-single", benchmark_db_update_batch_index_single);
	j_benchmark_add("/db/entry/update-index-all", benchmark_db_update_index_all);
	j_benchmark_add("/db/entry/update-batch-index-all", benchmark_db_update_batch_index_all);
	j_benchmark_add("/db/entry/update-index-mixed", benchmark_db_update_index_mixed);
	j_benchmark_add("/db/entry/update-batch-index-mixed", benchmark_db_update_batch_index_mixed);
}
