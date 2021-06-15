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

#define EPS 1e-9

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
test_db_entry_new_free(void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(JDBEntry) entry = NULL;
		g_autoptr(JDBSchema) schema = NULL;

		schema = j_db_schema_new("test-ns", "test-schema", &error);
		g_assert_nonnull(schema);
		g_assert_no_error(error);

		entry = j_db_entry_new(schema, &error);
		g_assert_nonnull(entry);
		g_assert_no_error(error);
	}
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
	g_error_free(error);
}

static void
test_db_invalid_entry_set(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res;
	gint64 val = 42;

	schema = j_db_schema_new("test-ns", "test", NULL);
	j_db_schema_add_field(schema, "test-si64", J_DB_TYPE_SINT64, NULL);
	entry = j_db_entry_new(schema, NULL);

	res = j_db_entry_set_field(entry, "not-existant", &val, sizeof(gint64), &error);
	g_assert_false(res);
	g_assert_nonnull(error);
	g_error_free(error);
}

static void
test_db_invalid_entry_insert(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res;
	gint64 val = 42;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("test-ns", "test", NULL);
	j_db_schema_add_field(schema, "test-si64", J_DB_TYPE_SINT64, NULL);
	
	entry = j_db_entry_new(schema, NULL);
	j_db_entry_set_field(entry, "test-si64", &val, sizeof(gint64), NULL);

	res = j_db_entry_insert(entry, batch, &error);
	g_assert_true(res);

	res = j_batch_execute(batch);
	g_assert_false(res);
	g_assert_nonnull(error);
}

static void
test_db_entry_insert_update_delete(void)
{
	guint const n = 1000;

	gchar const* str = "demo.bp";
	gint32 si32 = -42;
	guint32 ui32 = 42;
	gint64 si64 = -84;
	guint64 ui64 = 4;
	gfloat fl32 = 1.337;
	gdouble fl64 = 42.42;
	guint8 blob[5] = { 1, 2, 3, 4, 5 };
	gpointer val;
	JDBType type;
	guint64 length;

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	g_autoptr(JDBEntry) delete_entry = NULL;
	g_autoptr(JDBEntry) update_entry = NULL;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	gboolean ret;

	schema = j_db_schema_new("test-ns", "test-schema", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "string-0", J_DB_TYPE_STRING, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "sint32-0", J_DB_TYPE_SINT32, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "uint32-0", J_DB_TYPE_UINT32, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "sint64-0", J_DB_TYPE_SINT64, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "uint64-0", J_DB_TYPE_UINT64, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "float32-0", J_DB_TYPE_FLOAT32, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "float64-0", J_DB_TYPE_FLOAT64, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "blob-0", J_DB_TYPE_BLOB, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_create(schema, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JDBEntry) entry = NULL;

		entry = j_db_entry_new(schema, &error);
		g_assert_nonnull(entry);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "string-0", str, strlen(str), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "sint32-0", &si32, sizeof(si32), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "uint32-0", &ui32, sizeof(ui32), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "sint64-0", &si64, sizeof(si64), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "uint64-0", &ui64, sizeof(ui64), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "float32-0", &fl32, sizeof(fl32), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "float64-0", &fl64, sizeof(fl64), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "blob-0", blob, sizeof(blob), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		// FIXME Do not pass error, will not exist anymore when batch is executed
		ret = j_db_entry_insert(entry, batch, NULL);
		g_assert_true(ret);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	// check all written values

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);

	ret = j_db_selector_add_field(selector, "string-0", J_DB_SELECTOR_OPERATOR_EQ, str, strlen(str), &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	iterator = j_db_iterator_new(schema, selector, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	while (j_db_iterator_next(iterator, &error))
	{
		g_assert_no_error(error);

		ret = j_db_iterator_get_field(iterator, "string-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpstr((gchar*)val, ==, str);
		g_assert_cmpint(type, ==, J_DB_TYPE_STRING);
		g_free(val);

		ret = j_db_iterator_get_field(iterator, "sint32-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpint(*(gint32*)val, ==, si32);
		g_assert_cmpint(type, ==, J_DB_TYPE_SINT32);
		g_free(val);

		ret = j_db_iterator_get_field(iterator, "uint32-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpuint(*(guint32*)val, ==, ui32);
		g_assert_cmpint(type, ==, J_DB_TYPE_UINT32);

		ret = j_db_iterator_get_field(iterator, "sint64-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpint(*(gint64*)val, ==, si64);
		g_assert_cmpint(type, ==, J_DB_TYPE_SINT64);
		g_free(val);

		ret = j_db_iterator_get_field(iterator, "uint64-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpuint(*(guint64*)val, ==, ui64);
		g_assert_cmpint(type, ==, J_DB_TYPE_UINT64);
		g_free(val);

		ret = j_db_iterator_get_field(iterator, "float32-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpfloat(fabs(*(gfloat*)val - fl32), <, EPS);
		g_assert_cmpint(type, ==, J_DB_TYPE_FLOAT32);
		g_free(val);

		ret = j_db_iterator_get_field(iterator, "float64-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpfloat(fabs(*(gdouble*)val - fl64), <, EPS);
		g_assert_cmpint(type, ==, J_DB_TYPE_FLOAT64);
		g_free(val);

		ret = j_db_iterator_get_field(iterator, "blob-0", &type, &val, &length, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		g_assert_cmpmem((guint8*)val, length, blob, sizeof(blob));
		g_assert_cmpint(type, ==, J_DB_TYPE_BLOB);
		g_free(val);
	}

	g_error_free(error);
	error = NULL;

	// update values

	update_entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(update_entry);
	g_assert_no_error(error);

	ui32 = 3;
	ret = j_db_entry_set_field(update_entry, "uint32-0", &ui32, sizeof(ui32), &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_entry_update(update_entry, selector, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	// delete new entry

	delete_entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(delete_entry);
	g_assert_no_error(error);

	ret = j_db_entry_delete(delete_entry, selector, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	// delete schema

	ret = j_db_schema_delete(schema, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
}

static void
schema_create(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_file[] = {
		"file", NULL
	};
	gchar const* idx_name[] = {
		"name", NULL
	};
	gchar const* idx_min[] = {
		"min", NULL
	};
	gchar const* idx_max[] = {
		"max", NULL
	};

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "name", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "dimensions", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "min", J_DB_TYPE_FLOAT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "max", J_DB_TYPE_FLOAT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_file, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_index(schema, idx_name, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_index(schema, idx_min, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_index(schema, idx_max, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
entry_insert(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	gchar const* name = "temperature";
	guint64 dim = 4;
	gdouble min = 1.0;
	gdouble max = 42.0;

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "file", file, strlen(file), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "name", name, strlen(name), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "dimensions", &dim, sizeof(dim), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "min", &min, sizeof(min), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "max", &max, sizeof(max), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
iterator_get(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	JDBType type;
	guint64 len;
	g_autofree gchar* name = NULL;
	g_autofree guint64* dim = NULL;
	g_autofree gdouble* min = NULL;
	g_autofree gdouble* max = NULL;

	guint entries = 0;

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(success);
	g_assert_no_error(error);

	iterator = j_db_iterator_new(schema, selector, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	while (j_db_iterator_next(iterator, NULL))
	{
		success = j_db_iterator_get_field(iterator, "name", &type, (gpointer*)&name, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, "dimensions", &type, (gpointer*)&dim, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, "min", &type, (gpointer*)&min, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, "max", &type, (gpointer*)&max, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		entries++;
	}

	g_assert_cmpuint(entries, ==, 1);
}

static void
entry_update(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	g_autoptr(JDBIterator) it = NULL;
	gpointer val = NULL;
	JDBType type;
	guint64 val_length;

	gchar const* file = "demo.bp";
	gchar const* name = "temperature";
	gdouble min = 2.0;
	gdouble max = 22.0;

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error);
	g_assert_true(success);
	g_assert_no_error(error);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "min", &min, sizeof(min), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "max", &max, sizeof(max), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_update(entry, selector, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	// test if values were actually updated

	it = j_db_iterator_new(schema, selector, &error);
	g_assert_nonnull(it);
	g_assert_no_error(error);

	while (j_db_iterator_next(it, &error))
	{
		g_assert_no_error(error);
		success = j_db_iterator_get_field(it, "min", &type, &val, &val_length, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		// g_assert_cmpfloat_with_epsilon(*(double*)val, 2.0, EPS);
		g_assert_cmpfloat(fabs(*(double*)val - 2.0), <, EPS);
		g_assert_cmpint(type, ==, J_DB_TYPE_FLOAT64);
		g_assert_cmpuint(val_length, ==, sizeof(gdouble));
		g_free(val);
		success = j_db_iterator_get_field(it, "max", &type, &val, &val_length, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		g_assert_cmpfloat(fabs(*(double*)val - 22.0), <, EPS);
		// g_assert_cmpfloat_with_epsilon(*(double*)val, 22.0, EPS);
		g_assert_cmpint(type, ==, J_DB_TYPE_FLOAT64);
		g_assert_cmpuint(val_length, ==, sizeof(gdouble));
		g_free(val);
	}
}

static void
entry_delete(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	gchar const* name = "temperature";

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error);
	g_assert_true(success);
	g_assert_no_error(error);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);
	success = j_db_entry_delete(entry, selector, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
test_db_all(void)
{
	schema_create();
	entry_insert();
	iterator_get();
	entry_update();
	entry_delete();
	schema_delete();
}

void
test_db_db(void)
{
	// FIXME add more tests
	g_test_add_func("/db/schema/new_free", test_db_schema_new_free);
	g_test_add_func("/db/schema/create_delete", test_db_schema_create_delete);
	g_test_add_func("/db/schema/invalid_get", test_db_invalid_schema_get);
	g_test_add_func("/db/entry/new_free", test_db_entry_new_free);
	g_test_add_func("/db/entry/insert_update_delete", test_db_entry_insert_update_delete);
	g_test_add_func("/db/entry/invalid_insert", test_db_invalid_entry_insert);
	g_test_add_func("/db/entry/invalid_set", test_db_invalid_entry_set);
	g_test_add_func("/db/all", test_db_all);
}
