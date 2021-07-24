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
	ret = j_db_schema_add_field(schema, "test-si64", J_DB_TYPE_SINT64, NULL);
	g_assert_true(ret);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	ret = j_db_schema_create(schema, batch, &error);
	g_assert_true(ret);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	j_db_schema_delete(schema, batch, &error);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	j_db_schema_delete(schema, batch, &error);
	ret = j_batch_execute(batch);
	g_assert_false(ret);
	g_assert_nonnull(error);
}

static void
test_db_schema_equals_different_name(void)
{
	g_autoptr(JDBSchema) schema1 = NULL;
	g_autoptr(JDBSchema) schema2 = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res, equal;

	schema1 = j_db_schema_new("equal", "same1", NULL);
	schema2 = j_db_schema_new("equal", "same2", NULL);

	res = j_db_schema_equals(schema1, schema2, &equal, &error);
	g_assert_true(res);
	g_assert_false(equal);
	g_assert_no_error(error);
}

static void
test_db_schema_equals_same_name_different_fields(void)
{
	g_autoptr(JDBSchema) schema1 = NULL;
	g_autoptr(JDBSchema) schema2 = NULL;
	g_autoptr(GRand) rnd = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res, equal;
	gchar field_name[] = "field_dd";

	schema1 = j_db_schema_new("equal", "same", NULL);
	schema2 = j_db_schema_new("equal", "same", NULL);

	rnd = g_rand_new();

	for (int i = 0; i <= g_rand_int_range(rnd, 1, 100); ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		// generate some random type fields (excluding id type)
		res = j_db_schema_add_field(schema1, field_name, g_rand_int_range(rnd, 0, 8), NULL);
		g_assert_true(res);
		res = j_db_schema_add_field(schema2, field_name, g_rand_int_range(rnd, 0, 8), NULL);
		g_assert_true(res);
	}

	res = j_db_schema_equals(schema1, schema2, &equal, &error);
	g_assert_true(res);
	g_assert_false(equal);
	g_assert_no_error(error);
}

static void
test_db_schema_equals_same_name_same_fields(void)
{
	g_autoptr(JDBSchema) schema1 = NULL;
	g_autoptr(JDBSchema) schema2 = NULL;
	g_autoptr(GRand) rnd = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res, equal;
	gint field = 0;
	gchar field_name[] = "field_dd";

	schema1 = j_db_schema_new("equal", "same", NULL);
	schema2 = j_db_schema_new("equal", "same", NULL);

	rnd = g_rand_new();

	for (int i = 0; i <= g_rand_int_range(rnd, 1, 100); ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		// generate some random type fields (excluding id type)
		field = g_rand_int_range(rnd, 0, 8);
		res = j_db_schema_add_field(schema1, field_name, field, NULL);
		g_assert_true(res);
		res = j_db_schema_add_field(schema2, field_name, field, NULL);
		g_assert_true(res);
	}

	res = j_db_schema_equals(schema1, schema2, &equal, &error);
	g_assert_true(res);
	g_assert_true(equal);
	g_assert_no_error(error);
}

static void
test_db_schema_get_all_fields(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(GRand) rnd = NULL;
	g_autoptr(GError) error = NULL;
	g_autofree JDBType* res_types = NULL;
	g_autofree gchar** res_names = NULL;
	g_autofree JDBType* types = NULL;
	g_autofree gchar** names = NULL;
	gchar field_name[] = "field_dd";
	gint field = 0;
	guint32 field_count;
	guint32 res_field_count;

	schema = j_db_schema_new("equal", "same", NULL);
	rnd = g_rand_new();

	field_count = g_rand_int_range(rnd, 1, 100);
	types = g_new(JDBType, field_count);
	names = g_new0(gchar*, field_count);

	for (guint32 i = 0; i < field_count; ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		// generate some random type fields (excluding id type)
		field = g_rand_int_range(rnd, 0, 8);
		types[i] = field;
		names[i] = g_strdup(field_name);
		g_assert_true(j_db_schema_add_field(schema, field_name, field, NULL));
	}

	res_field_count = j_db_schema_get_all_fields(schema, &res_names, &res_types, &error);
	g_assert_no_error(error);
	g_assert_cmpint(res_field_count, ==, field_count);

	for (guint32 i = 0; i < field_count; ++i)
	{
		g_assert_cmpstr(names[i], ==, res_names[i]);
		g_assert_cmpint(types[i], ==, res_types[i]);
		g_free(names[i]);
		g_free(res_names[i]);
	}
}

void
test_db_schema(void)
{
	g_test_add_func("/db/schema/new_free", test_db_schema_new_free);
	g_test_add_func("/db/schema/create_delete", test_db_schema_create_delete);
	g_test_add_func("/db/schema/invalid_get", test_db_invalid_schema_get);
	g_test_add_func("/db/schema/invalid_delete", test_db_invalid_schema_delete);
	g_test_add_func("/db/schema/equals_different_name", test_db_schema_equals_different_name);
	g_test_add_func("/db/schema/equals_same_name_different_fields", test_db_schema_equals_same_name_different_fields);
	g_test_add_func("/db/schema/equals_same_name_same_fields", test_db_schema_equals_same_name_same_fields);
	g_test_add_func("/db/schema/get_all_fields", test_db_schema_get_all_fields);
}
