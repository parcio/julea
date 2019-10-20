/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019 Michael Kuhn
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

static
void
test_db_schema_new_free (void)
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

static
void
test_db_schema_create_delete (void)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	gboolean ret;

	gchar const* idx_file[] = {"file", NULL};
	gchar const* idx_name[] = {"name", NULL};
	gchar const* idx_min[] = {"min", NULL};
	gchar const* idx_max[] = {"max", NULL};

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(JDBSchema) schema = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("test_schema_%d", i);

		// FIXME namespace and name are not properly checked, "-" causes errors
		schema = j_db_schema_new("test_ns", name, &error);
		g_assert_nonnull(schema);
		g_assert_no_error(error);

		ret = j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "name", J_DB_TYPE_STRING, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "dimensions", J_DB_TYPE_UINT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "min", J_DB_TYPE_FLOAT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_field(schema, "max", J_DB_TYPE_FLOAT64, &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_schema_add_index(schema, idx_file, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_index(schema, idx_name, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_index(schema, idx_min, &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		ret = j_db_schema_add_index(schema, idx_max, &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_schema_create(schema, batch, &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		ret = j_db_schema_delete(schema, batch, &error);
		g_assert_true(ret);
		g_assert_no_error(error);

		// FIXME reference counting is broken, should be outside loop
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}
}

static
void
schema_create (void)
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
	success = j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	success = j_db_schema_add_field(schema, "name", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	success = j_db_schema_add_field(schema, "dimensions", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	success = j_db_schema_add_field(schema, "min", J_DB_TYPE_FLOAT64, &error);
	g_assert_true(success);
	success = j_db_schema_add_field(schema, "max", J_DB_TYPE_FLOAT64, &error);
	g_assert_true(success);

	success = j_db_schema_add_index(schema, idx_file, &error);
	g_assert_true(success);
	success = j_db_schema_add_index(schema, idx_name, &error);
	g_assert_true(success);
	success = j_db_schema_add_index(schema, idx_min, &error);
	g_assert_true(success);
	success = j_db_schema_add_index(schema, idx_max, &error);
	g_assert_true(success);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static
void
entry_insert (void)
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
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	success = j_db_entry_set_field(entry, "file", file, strlen(file), &error);
	g_assert_true(success);
	success = j_db_entry_set_field(entry, "name", name, strlen(name), &error);
	g_assert_true(success);
	success = j_db_entry_set_field(entry, "dimensions", &dim, sizeof(dim), &error);
	g_assert_true(success);
	success = j_db_entry_set_field(entry, "min", &min, sizeof(min), &error);
	g_assert_true(success);
	success = j_db_entry_set_field(entry, "max", &max, sizeof(max), &error);
	g_assert_true(success);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static
void
iterator_get (void)
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

	schema = j_db_schema_new("adios2", "variables", &error);
	g_assert_nonnull(schema);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	success = j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(success);

	iterator = j_db_iterator_new(schema, selector, &error);
	g_assert_nonnull(iterator);

	while (j_db_iterator_next(iterator, NULL))
	{
		success = j_db_iterator_get_field(iterator, "name", &type, (gpointer*)&name, &len, &error);
		g_assert_true(success);
		success = j_db_iterator_get_field(iterator, "dimensions", &type, (gpointer*)&dim, &len, &error);
		g_assert_true(success);
		success = j_db_iterator_get_field(iterator, "min", &type, (gpointer*)&min, &len, &error);
		g_assert_true(success);
		success = j_db_iterator_get_field(iterator, "max", &type, (gpointer*)&max, &len, &error);
		g_assert_true(success);
	}
}

static
void
entry_update (void)
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
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	success = j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(success);
	success = j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error);
	g_assert_true(success);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	success = j_db_entry_set_field(entry, "min", &min, sizeof(min), &error);
	g_assert_true(success);
	success = j_db_entry_set_field(entry, "max", &max, sizeof(max), &error);
	g_assert_true(success);
	success = j_db_entry_update(entry, selector, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static
void
entry_delete (void)
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
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	success = j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), &error);
	g_assert_true(success);
	success = j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error);
	g_assert_true(success);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	success = j_db_entry_delete(entry, selector, batch, &error);
	g_assert_true(success);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static
void
test_db_all (void)
{
	schema_create();
	entry_insert();
	iterator_get();
	entry_update();
	entry_delete();
}

void
test_db (void)
{
	g_test_add_func("/db/schema/new_free", test_db_schema_new_free);
	g_test_add_func("/db/schema/create_delete", test_db_schema_create_delete);
	g_test_add_func("/db/all", test_db_all);
}
