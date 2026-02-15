/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019-2024 Michael Kuhn
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
		success = j_db_iterator_get_field(it, schema, "min", &type, &val, &val_length, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		// g_assert_cmpfloat_with_epsilon(*(double*)val, 2.0, EPS);
		g_assert_cmpfloat(fabs(*(double*)val - 2.0), <, EPS);
		g_assert_cmpint(type, ==, J_DB_TYPE_FLOAT64);
		g_assert_cmpuint(val_length, ==, sizeof(gdouble));
		g_free(val);
		success = j_db_iterator_get_field(it, schema, "max", &type, &val, &val_length, &error);
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
	g_test_add_func("/db/all", test_db_all);
}
