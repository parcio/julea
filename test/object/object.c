/*
 * JULEA - Flexible storage framework
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
#include <julea-object.h>

#include "test.h"

static void
test_object_new_free(void)
{
	guint const n = 100000;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JObject) object = NULL;

		object = j_object_new("test", "test-object");
		g_assert_true(object != NULL);
	}
	J_TEST_TRAP_END;
}

static void
test_object_create_delete(void)
{
	guint const n = 100;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JObject) object_noexist = NULL;
	gboolean ret;

	J_TEST_TRAP_START;
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JObject) object = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("test-object-%u", i);
		object = j_object_new("test", name);
		g_assert_true(object != NULL);

		j_object_create(object, batch);
		j_object_delete(object, batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	object_noexist = j_object_new("test", "test-object-noexist");
	g_assert_true(object_noexist != NULL);

	j_object_delete(object_noexist, batch);
	ret = j_batch_execute(batch);
	g_assert_false(ret);
	J_TEST_TRAP_END;
}

static void
test_object_read_write(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JObject) object = NULL;
	g_autoptr(JObject) object_noexist = NULL;
	g_autofree gchar* buffer = NULL;
	g_autofree gchar* buffer2 = NULL;
	guint64 max_operation_size;
	guint64 nbytes = 0;
	gboolean ret;

	J_TEST_TRAP_START;
	max_operation_size = j_configuration_get_max_operation_size(j_configuration());

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	buffer = g_malloc0(max_operation_size + 1);

	object = j_object_new("test", "test-object-rw");
	g_assert_true(object != NULL);

	j_object_create(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_object_write(object, buffer, 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, 1);

	j_object_write(object, buffer, max_operation_size - 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, max_operation_size - 1);

	j_object_read(object, buffer, max_operation_size - 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, max_operation_size - 1);

	j_object_write(object, buffer, max_operation_size, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, max_operation_size);

	j_object_read(object, buffer, max_operation_size, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, max_operation_size);

	j_object_write(object, buffer, max_operation_size + 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, max_operation_size + 1);

	j_object_read(object, buffer, max_operation_size + 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, max_operation_size + 1);

	j_object_write(object, buffer, max_operation_size - 1, 0, &nbytes, batch);
	j_object_write(object, buffer, max_operation_size, 0, &nbytes, batch);
	j_object_write(object, buffer, max_operation_size + 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, 3 * max_operation_size);

	j_object_read(object, buffer, max_operation_size - 1, 0, &nbytes, batch);
	j_object_read(object, buffer, max_operation_size, 0, &nbytes, batch);
	j_object_read(object, buffer, max_operation_size + 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, 3 * max_operation_size);

	buffer2 = g_malloc0(max_operation_size + 1);
	memset(buffer2, 'j', max_operation_size + 1);

	j_object_write(object, buffer2, max_operation_size + 1, 0, &nbytes, batch);
	j_object_read(object, buffer, max_operation_size + 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, 2 * max_operation_size + 2);
	g_assert_cmpmem(buffer, max_operation_size + 1, buffer2, max_operation_size + 1);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	object_noexist = j_object_new("test", "test-object-rw-noexist");
	g_assert_true(object_noexist != NULL);

	j_object_write(object_noexist, buffer, 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_false(ret);
	g_assert_cmpuint(nbytes, ==, 0);

	j_object_read(object_noexist, buffer, 1, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_false(ret);
	g_assert_cmpuint(nbytes, ==, 0);
	J_TEST_TRAP_END;
}

static void
test_object_status(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JObject) object = NULL;
	g_autofree gchar* buffer = NULL;
	gint64 modification_time = 0;
	guint64 nbytes = 0;
	guint64 size = 0;
	gboolean ret;

	J_TEST_TRAP_START;
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	buffer = g_malloc0(42);

	object = j_object_new("test", "test-object-status");
	g_assert_true(object != NULL);

	j_object_create(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_object_write(object, buffer, 42, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, 42);

	j_object_status(object, &modification_time, &size, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpint(modification_time, !=, 0);
	g_assert_cmpuint(size, ==, 42);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	J_TEST_TRAP_END;
}

static void
test_object_sync(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JObject) object = NULL;
	g_autofree gchar* buffer = NULL;
	guint64 nbytes = 0;
	gboolean ret;

	J_TEST_TRAP_START;
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	buffer = g_malloc0(42);

	object = j_object_new("test", "test-object-sync");
	g_assert_true(object != NULL);

	j_object_create(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_object_write(object, buffer, 42, 0, &nbytes, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nbytes, ==, 42);

	j_object_sync(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
	J_TEST_TRAP_END;
}

void
test_object_object(void)
{
	g_test_add_func("/object/object/new_free", test_object_new_free);
	g_test_add_func("/object/object/create_delete", test_object_create_delete);
	g_test_add_func("/object/object/read_write", test_object_read_write);
	g_test_add_func("/object/object/status", test_object_status);
	g_test_add_func("/object/object/sync", test_object_sync);
}
