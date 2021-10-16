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
test_db_entry_new_free(void)
{
	guint const n = 100000;

	J_TEST_TRAP_START;

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
	
	J_TEST_TRAP_END;
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

	J_TEST_TRAP_START;

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
		g_free(val);

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

	J_TEST_TRAP_END;
}

static void
test_db_entry_id(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	gint64 val = 42;
	g_autofree gpointer id = NULL;
	guint64 len = 0;
	gboolean res;

	J_TEST_TRAP_START;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("test-ns", "test", NULL);
	j_db_schema_add_field(schema, "test-si64", J_DB_TYPE_SINT64, NULL);
	res = j_db_schema_create(schema, batch, &error);
	g_assert_true(res);

	entry = j_db_entry_new(schema, NULL);
	j_db_entry_set_field(entry, "test-si64", &val, sizeof(gint64), NULL);

	res = j_db_entry_insert(entry, batch, &error);
	g_assert_true(res);

	res = j_batch_execute(batch);
	g_assert_true(res);
	g_assert_no_error(error);

	res = j_db_entry_get_id(entry, &id, &len, &error);
	g_assert_true(res);
	g_assert_no_error(error);

	res = j_db_schema_delete(schema, batch, &error);
	g_assert_true(res);

	res = j_batch_execute(batch);
	g_assert_true(res);
	g_assert_no_error(error);

	J_TEST_TRAP_END;
}

static void
test_db_entry_invalid_insert(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res;
	gint64 val = 42;

	J_TEST_TRAP_START;

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

	J_TEST_TRAP_END;
}

static void
test_db_entry_set_nonexistant(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(GError) error = NULL;
	gboolean res;
	gint64 val = 42;

	J_TEST_TRAP_START;

	schema = j_db_schema_new("test-ns", "test", NULL);
	j_db_schema_add_field(schema, "test-si64", J_DB_TYPE_SINT64, NULL);
	entry = j_db_entry_new(schema, NULL);

	res = j_db_entry_set_field(entry, "not-existant", &val, sizeof(gint64), &error);
	g_assert_false(res);
	g_assert_nonnull(error);

	J_TEST_TRAP_END;
}

void
test_db_entry(void)
{
	g_test_add_func("/db/entry/new_free", test_db_entry_new_free);
	g_test_add_func("/db/entry/insert_update_delete", test_db_entry_insert_update_delete);
	g_test_add_func("/db/entry/get_id", test_db_entry_id);
	g_test_add_func("/db/entry/insert_in_local_schema", test_db_entry_invalid_insert);
	g_test_add_func("/db/entry/set_non_existing_field", test_db_entry_set_nonexistant);
}
