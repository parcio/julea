/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019-2021 Michael Kuhn
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
#include <math.h>

#include <julea.h>
#include <julea-db.h>

#include "test.h"

static void
test_db_schema_new_free(void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(JDBSchema) schema = NULL;

		schema = j_db_schema_new("test-ns", "test-schema", &error);
		g_assert_nonnull(schema);
		g_assert_no_error(error);
	}
}

static void
test_db_schema_create_delete(void)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	gboolean ret;

	gchar const* idx_string[] = { "string-0", NULL };
	gchar const* idx_uint[] = { "uint-0", NULL };
	gchar const* idx_float[] = { "float-0", NULL };

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(JDBSchema) schema = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("test-schema-%d", i);
		schema = j_db_schema_new("test-ns", name, &error);
		g_assert_nonnull(schema);
		g_assert_no_error(error);

		ret = j_db_schema_add_field(schema, "string-0", J_DB_TYPE_STRING, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "string-1", J_DB_TYPE_STRING, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "uint-0", J_DB_TYPE_UINT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "uint-1", J_DB_TYPE_UINT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "float-0", J_DB_TYPE_FLOAT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "float-1", J_DB_TYPE_FLOAT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_schema_add_index(schema, idx_string, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_index(schema, idx_uint, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_index(schema, idx_float, &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		// FIXME Do not pass error, will not exist anymore when batch is executed
		ret = j_db_schema_create(schema, batch, NULL);
		g_assert_true(ret);

		// FIXME Do not pass error, will not exist anymore when batch is executed
		ret = j_db_schema_delete(schema, batch, NULL);
		g_assert_true(ret);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);
}

static void
test_db_invalid_schema_get(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("test-ns", "non-existant", NULL);

	res = j_db_schema_get(schema, batch, &error);
	g_assert_true(res);

	res = j_batch_execute(batch);
	g_assert_false(res);
	g_assert_nonnull(error);
}

static void
test_db_invalid_schema_delete(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	bool ret;

	schema = j_db_schema_new("test-ns", "test", NULL);
	j_db_schema_add_field(schema, "test-si64", J_DB_TYPE_SINT64, NULL);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_db_schema_create(schema, batch, &error);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_null(error);

	j_db_schema_delete(schema, batch, &error);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_null(error);

	j_db_schema_delete(schema, batch, &error);
	ret = j_batch_execute(batch);
	g_assert_false(ret);
	g_assert_nonnull(error);
}

void
test_db_schema(void)
{
	g_test_add_func("/db/schema/new_free", test_db_schema_new_free);
	g_test_add_func("/db/schema/create_delete", test_db_schema_create_delete);
	g_test_add_func("/db/schema/invalid_get", test_db_invalid_schema_get);
	g_test_add_func("/db/schema/invalid_delete", test_db_invalid_schema_delete);
}
