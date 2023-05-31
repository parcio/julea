/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019-2023 Michael Kuhn
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

#include "test.h"

static void
test_db_schema_new_free(void)
{
	guint const n = 100000;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(JDBSchema) schema = NULL;

		schema = j_db_schema_new("test-ns", "test-schema", &error);
		g_assert_nonnull(schema);
		g_assert_no_error(error);
	}
	J_TEST_TRAP_END;
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

	J_TEST_TRAP_START;
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

		/// \todo Do not pass error, will not exist anymore when batch is executed
		ret = j_db_schema_create(schema, batch, NULL);
		g_assert_true(ret);

		/// \todo Do not pass error, will not exist anymore when batch is executed
		ret = j_db_schema_delete(schema, batch, NULL);
		g_assert_true(ret);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	J_TEST_TRAP_END;
}

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

	gchar const* file = "demo.bp";
	guint64 dim = 4;

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	g_autoptr(JDBEntry) delete_entry = NULL;
	g_autoptr(JDBEntry) update_entry = NULL;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	gboolean ret;

	J_TEST_TRAP_START;
	schema = j_db_schema_new("test-ns", "test-schema", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "string-0", J_DB_TYPE_STRING, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_add_field(schema, "uint-0", J_DB_TYPE_UINT64, &error);
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

		ret = j_db_entry_set_field(entry, "string-0", file, strlen(file), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_entry_set_field(entry, "uint-0", &dim, sizeof(dim), &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		/// \todo Do not pass error, will not exist anymore when batch is executed
		ret = j_db_entry_insert(entry, batch, NULL);
		g_assert_true(ret);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);

	ret = j_db_selector_add_field(selector, "string-0", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	update_entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(update_entry);
	g_assert_no_error(error);

	dim = 3;
	ret = j_db_entry_set_field(update_entry, "uint-0", &dim, sizeof(dim), &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_entry_update(update_entry, selector, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	delete_entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(delete_entry);
	g_assert_no_error(error);

	ret = j_db_entry_delete(delete_entry, selector, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_db_schema_delete(schema, batch, &error);
	g_assert_true(ret);
	g_assert_no_error(error);

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	J_TEST_TRAP_END;
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
		success = j_db_iterator_get_field(iterator, schema, "name", &type, (gpointer*)&name, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, schema, "dimensions", &type, (gpointer*)&dim, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, schema, "min", &type, (gpointer*)&min, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, schema, "max", &type, (gpointer*)&max, &len, &error);
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
	J_TEST_TRAP_START;
	schema_create();
	entry_insert();
	iterator_get();
	entry_update();
	entry_delete();
	schema_delete();
	J_TEST_TRAP_END;
}

void
test_db_db(void)
{
	/// \todo add more tests
	g_test_add_func("/db/schema/new_free", test_db_schema_new_free);
	g_test_add_func("/db/schema/create_delete", test_db_schema_create_delete);
	g_test_add_func("/db/entry/new_free", test_db_entry_new_free);
	g_test_add_func("/db/entry/insert_update_delete", test_db_entry_insert_update_delete);
	g_test_add_func("/db/all", test_db_all);
}
