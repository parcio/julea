/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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
test_object_iterator_new_free(void)
{
	J_TEST_TRAP_START;
	guint const n = 10;

	guint32 server_count;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JObjectIterator) iterator = NULL;
		g_autoptr(JObjectIterator) iterator_prefix = NULL;

		iterator = j_object_iterator_new("test-ns", NULL);
		g_assert_nonnull(iterator);

		iterator_prefix = j_object_iterator_new("test-ns", "test-object-new-free");
		g_assert_nonnull(iterator_prefix);
	}

	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JObjectIterator) iterator = NULL;
		g_autoptr(JObjectIterator) iterator_prefix = NULL;

		iterator = j_object_iterator_new_for_index(i, "test-ns", NULL);
		g_assert_nonnull(iterator);

		iterator_prefix = j_object_iterator_new_for_index(i, "test-ns", "test-object-new-free-index");
		g_assert_nonnull(iterator_prefix);
	}
	J_TEST_TRAP_END;
}

static void
test_object_iterator_next_get(void)
{
	J_TEST_TRAP_START;
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JObjectIterator) object_iterator = NULL;
	g_autoptr(JObjectIterator) object_iterator_prefix = NULL;
	gboolean ret;
	guint32 server_count;

	guint objects = 0;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JObject) object = NULL;

		g_autofree gchar* key = NULL;

		key = g_strdup_printf("test-key-next-get-%d-%d", i % 2, i);
		object = j_object_new("test-ns", key);
		j_object_create(object, batch);
		j_object_delete(object, delete_batch);

		g_assert_nonnull(object);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	object_iterator = j_object_iterator_new("test-ns", NULL);

	while (j_object_iterator_next(object_iterator))
	{
		gchar const* key;

		key = j_object_iterator_get(object_iterator);
		g_assert_true(g_str_has_prefix(key, "test-key-next-get-"));
		objects++;
	}

	g_assert_cmpuint(objects, ==, n);

	objects = 0;
	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JObjectIterator) iterator = NULL;

		iterator = j_object_iterator_new_for_index(i, "test-ns", NULL);

		while (j_object_iterator_next(iterator))
		{
			gchar const* key;

			key = j_object_iterator_get(iterator);
			g_assert_true(g_str_has_prefix(key, "test-key-next-get-"));
			objects++;
		}
	}

	g_assert_cmpuint(objects, ==, n);

	objects = 0;
	object_iterator_prefix = j_object_iterator_new("test-ns", "test-key-next-get-1-");

	while (j_object_iterator_next(object_iterator_prefix))
	{
		gchar const* key;

		key = j_object_iterator_get(object_iterator_prefix);
		g_assert_true(g_str_has_prefix(key, "test-key-next-get-1-"));
		objects++;
	}

	g_assert_cmpuint(objects, ==, n / 2);

	objects = 0;
	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_OBJECT);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JObjectIterator) iterator = NULL;

		iterator = j_object_iterator_new_for_index(i, "test-ns", "test-key-next-get-1-");

		while (j_object_iterator_next(iterator))
		{
			gchar const* key;

			key = j_object_iterator_get(iterator);
			g_assert_true(g_str_has_prefix(key, "test-key-next-get-1-"));
			objects++;
		}
	}

	g_assert_cmpuint(objects, ==, n / 2);

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);
	J_TEST_TRAP_END;
}

void
test_object_object_iterator(void)
{
	g_test_add_func("/object/object-iterator/new_free", test_object_iterator_new_free);
	g_test_add_func("/object/object-iterator/next_get", test_object_iterator_next_get);
}
