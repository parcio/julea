
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

#include <julea.h>
#include <julea-db.h>

#include "test.h"

static int num_insert_selects = 0;

static JDBSchema*
schema_create(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	gchar const* idx_name[] = { "name", NULL };

	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	JDBSchema* schema = NULL;
	schema = j_db_schema_new("parallel", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_add_field(schema, "name", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_name, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);

	return schema;
}

static void
schema_delete(void)
{
	gboolean success = false;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("parallel", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
test_parallel_insert_select(gpointer data, gpointer schema)
{
	gboolean success = TRUE;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBIterator) iterator = NULL;

	JDBType type;
	guint64 len;
	g_autofree gchar* name = NULL;
	guint entries = 0;

	g_autofree gchar* string = g_strdup_printf("test-db-parallel-insert-select-%d", GPOINTER_TO_INT(data));

	// Create entry.
	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);

	success = j_db_entry_set_field(entry, "name", string, strlen(string) + 1, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);

	// Select entry.
	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);

	success = j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, string, strlen(string) + 1, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	// Iterate over found entries. (Should only be one)
	iterator = j_db_iterator_new(schema, selector, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	while (j_db_iterator_next(iterator, NULL))
	{
		success = j_db_iterator_get_field(iterator, schema, "name", &type, (gpointer*)&name, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		g_assert_cmpstr(name, ==, string);
		g_assert_cmpint(len, ==, strlen(string));
		g_assert_cmpint(type, ==, J_DB_TYPE_STRING);

		entries++;
	}

	g_assert_cmpuint(entries, ==, 1);
	g_atomic_int_inc(&num_insert_selects);
}

static void
test_parallel_insert_select_setup(void)
{
	guint const num_threads = 1000;
	g_autoptr(JDBSchema) schema = NULL;
	GThreadPool* thread_pool;

	J_TEST_TRAP_START;

	schema = schema_create();

	thread_pool = g_thread_pool_new(test_parallel_insert_select, schema, 4, TRUE, NULL);
	// Starts at 1 to avoid 0 getting converted to a null pointer.
	for (guint i = 1; i <= num_threads; i++)
	{
		g_thread_pool_push(thread_pool, GINT_TO_POINTER(i), NULL);
	}

	g_thread_pool_free(thread_pool, FALSE, TRUE);
	g_assert_cmpuint(g_atomic_int_get(&num_insert_selects), ==, num_threads);

	schema_delete();
	J_TEST_TRAP_END;
}

void
test_db_parallel(void)
{
	g_test_add_func("/db/db-parallel/test_parallel_insert_select_setup", test_parallel_insert_select_setup);
}
