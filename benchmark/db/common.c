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

		j_db_schema_ref(scheme);
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
		{
			continue;
		}
		else
		{
			break;
		}
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
