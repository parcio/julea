/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 - 2020 Michael Strassberger
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

// 頑張って！

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>
#include <julea-db.h>

#include "benchmark.h"

#define N (1 << 12)
#define N_GET_DIVIDER (N >> 8)
#define N_PRIME 11971
#define N_MODULUS 256
#define CLASS_MODULUS (N >> 3)
#define CLASS_LIMIT (CLASS_MODULUS >> 1)
#define SIGNED_FACTOR N_PRIME
#define USIGNED_FACTOR N_PRIME
#define FLOAT_FACTOR (3.1415926)

static char*
_benchmark_db_get_identifier(gint64 i)
{
	return g_strdup_printf("%x-benchmark-%ld", (guint32)(i * SIGNED_FACTOR % N_MODULUS), i);
}

static JDBSchema*
_benchmark_db_prepare_scheme(gchar const* namespace, gboolean use_batch, gboolean use_index_all, gboolean use_index_single, JBatch* batch, JBatch* delete_batch)
{
	gboolean ret;
	g_autoptr(GError) b_s_error = NULL;
	g_autoptr(JDBSchema) b_scheme = j_db_schema_new(namespace, "table", &b_s_error);

	g_assert_nonnull(b_scheme);
	g_assert_null(b_s_error);
	ret = j_db_schema_add_field(b_scheme, "string", J_DB_TYPE_STRING, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);
	ret = j_db_schema_add_field(b_scheme, "float", J_DB_TYPE_FLOAT64, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);
	ret = j_db_schema_add_field(b_scheme, "uint", J_DB_TYPE_UINT64, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);
	ret = j_db_schema_add_field(b_scheme, "sint", J_DB_TYPE_UINT64, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);
	ret = j_db_schema_add_field(b_scheme, "blob", J_DB_TYPE_BLOB, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);

	if (use_index_all)
	{
		gchar const* names[5];
		names[0] = "string";
		names[1] = "float";
		names[2] = "uint";
		names[3] = "sint";
		names[4] = NULL;
		ret = j_db_schema_add_index(b_scheme, names, &b_s_error);
		g_assert_null(b_s_error);
		g_assert_true(ret);
	}
	if (use_index_single)
	{
		gchar const* names[2];
		names[1] = NULL;
		names[0] = "string";
		ret = j_db_schema_add_index(b_scheme, names, &b_s_error);
		g_assert_null(b_s_error);
		g_assert_true(ret);
		names[0] = "float";
		ret = j_db_schema_add_index(b_scheme, names, &b_s_error);
		g_assert_null(b_s_error);
		g_assert_true(ret);
		names[0] = "uint";
		ret = j_db_schema_add_index(b_scheme, names, &b_s_error);
		g_assert_null(b_s_error);
		g_assert_true(ret);
		names[0] = "sint";
		ret = j_db_schema_add_index(b_scheme, names, &b_s_error);
		g_assert_null(b_s_error);
		g_assert_true(ret);
	}

	ret = j_db_schema_create(b_scheme, batch, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);
	ret = j_db_schema_delete(b_scheme, delete_batch, &b_s_error);
	g_assert_null(b_s_error);
	g_assert_true(ret);

	if (!use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	return j_db_schema_ref(b_scheme);
}

static void
_benchmark_db_insert(BenchmarkRun* run, JDBSchema* scheme, gchar const* namespace, gboolean use_batch, gboolean use_index_all, gboolean use_index_single, gboolean use_timer)
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

	if (use_timer)
	{
		g_assert_null(scheme);
		g_assert_nonnull(run);

		b_scheme = _benchmark_db_prepare_scheme(namespace, use_batch, use_index_all, use_index_single, batch, delete_batch);
		g_assert_nonnull(b_scheme);

		j_benchmark_timer_start(run);
	}
	else
	{
		g_assert_true(use_batch);
		g_assert_null(run);
		g_assert_nonnull(scheme);

		b_scheme = scheme;
	}

	while (true)
	{
		for (gint i = 0; i < N; i++)
		{
			gint64 i_signed = ((i * SIGNED_FACTOR) % CLASS_MODULUS) - CLASS_LIMIT;
			guint64 i_usigned = ((i * USIGNED_FACTOR) % CLASS_MODULUS);
			gdouble i_float = i_signed * FLOAT_FACTOR;
			g_autofree gchar* string = _benchmark_db_get_identifier(i);
			g_autoptr(JDBEntry) entry = j_db_entry_new(b_scheme, &b_s_error);
			g_assert_null(b_s_error);

			ret = j_db_entry_set_field(entry, "string", string, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);
			ret = j_db_entry_set_field(entry, "float", &i_float, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);
			ret = j_db_entry_set_field(entry, "sint", &i_signed, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);
			ret = j_db_entry_set_field(entry, "uint", &i_usigned, 0, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			ret = j_db_entry_insert(entry, batch, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
			}
		}

		if (use_batch || !use_timer)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
		if (use_timer && j_benchmark_iterate(run))
			continue;
		else
			break;
	}

	// Reuse Insert function for other Benchmarks with use_timer flag
	if (use_timer)
	{
		j_benchmark_timer_stop(run);
		ret = j_batch_execute(delete_batch);
		g_assert_true(ret);

		run->operations = N;
	}
}

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
			gsize field_length;
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

			ret = j_db_iterator_get_field(iterator, "string", &field_type, &field_value, &field_length, &b_s_error);
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
		for (gint i = 0; i < N_GET_DIVIDER; i++)
		{
			JDBType field_type;
			g_autofree gpointer field_value;
			gsize field_length;

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

			ret = j_db_iterator_get_field(iterator, "string", &field_type, &field_value, &field_length, &b_s_error);
			g_assert_true(ret);
			g_assert_null(b_s_error);
		}

	j_benchmark_timer_stop(run);

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	run->operations = N_GET_DIVIDER;
}

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

	_benchmark_db_insert(NULL, b_scheme, NULL, true, false, false, false);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
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
	}
	j_benchmark_timer_stop(run);

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
benchmark_db(void)
{
	j_benchmark_add("/db/insert", benchmark_db_insert);
	j_benchmark_add("/db/insert-batch", benchmark_db_insert_batch);
	j_benchmark_add("/db/insert-index-single", benchmark_db_insert_index_single);
	j_benchmark_add("/db/insert-batch-index-single", benchmark_db_insert_batch_index_single);
	j_benchmark_add("/db/insert-index-all", benchmark_db_insert_index_all);
	j_benchmark_add("/db/insert-batch-index-all", benchmark_db_insert_batch_index_all);
	j_benchmark_add("/db/insert-index-mixed", benchmark_db_insert_index_mixed);
	j_benchmark_add("/db/insert-batch-index-mixed", benchmark_db_insert_batch_index_mixed);
	j_benchmark_add("/db/get-simple", benchmark_db_get_simple);
	j_benchmark_add("/db/get-simple-index-single", benchmark_db_get_simple_index_single);
	j_benchmark_add("/db/get-simple-index-all", benchmark_db_get_simple_index_all);
	j_benchmark_add("/db/get-simple-index-mixed", benchmark_db_get_simple_index_mixed);
	j_benchmark_add("/db/get-range", benchmark_db_get_range);
	j_benchmark_add("/db/get-range-index-single", benchmark_db_get_range_index_single);
	j_benchmark_add("/db/get-range-index-all", benchmark_db_get_range_index_all);
	j_benchmark_add("/db/get-range-index-mixed", benchmark_db_get_range_index_mixed);
	j_benchmark_add("/db/delete", benchmark_db_delete);
	j_benchmark_add("/db/delete-batch", benchmark_db_delete_batch);
	j_benchmark_add("/db/delete-index-single", benchmark_db_delete_index_single);
	j_benchmark_add("/db/delete-batch-index-single", benchmark_db_delete_batch_index_single);
	j_benchmark_add("/db/delete-index-all", benchmark_db_delete_index_all);
	j_benchmark_add("/db/delete-batch-index-all", benchmark_db_delete_batch_index_all);
	j_benchmark_add("/db/delete-index-mixed", benchmark_db_delete_index_mixed);
	j_benchmark_add("/db/delete-batch-index-mixed", benchmark_db_delete_batch_index_mixed);
	j_benchmark_add("/db/update", benchmark_db_update);
	j_benchmark_add("/db/update-batch", benchmark_db_update_batch);
	j_benchmark_add("/db/update-index-single", benchmark_db_update_index_single);
	j_benchmark_add("/db/update-batch-index-single", benchmark_db_update_batch_index_single);
	j_benchmark_add("/db/update-index-all", benchmark_db_update_index_all);
	j_benchmark_add("/db/update-batch-index-all", benchmark_db_update_batch_index_all);
	j_benchmark_add("/db/update-index-mixed", benchmark_db_update_index_mixed);
	j_benchmark_add("/db/update-batch-index-mixed", benchmark_db_update_batch_index_mixed);
}