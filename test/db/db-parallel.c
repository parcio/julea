
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

static void
schema_create(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("parallel", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "int1", J_DB_TYPE_UINT32, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete(void)
{
	gboolean success;
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
entry_insert_select_parallel(gpointer data, gpointer user_data)
{
	gboolean success = TRUE;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	// Create entry.
	entry = j_db_entry_new(user_data, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);

	success = j_db_entry_set_field(entry, "int1", &data, sizeof(value), &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

void
entry_insert_select_parallel_setup(void)
{

	schema_delete();
	schema_create();

	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("parallel", "variables", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);


	guint const n = 200;

	// J_TEST_TRAP_START;
	GThreadPool* thread_pool = g_thread_pool_new(entry_insert_select_parallel, schema, 4, TRUE, NULL);

	for (guint i = 0; i < n; i++)
		g_thread_pool_push(thread_pool, GUINT_TO_POINTER(i), NULL);

	g_thread_pool_free(thread_pool, FALSE, TRUE);
	// g_assert_cmpuint(g_atomic_int_get(&num_put_gets), ==, n);
	// J_TEST_TRAP_END;

}



void
test_db_parallel(void)
{
	g_test_add_func("db/parallel", entry_insert_select_parallel_setup);
}
