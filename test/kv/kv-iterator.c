/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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
#include <julea-kv.h>

#include "test.h"

static void
test_kv_iterator_new_free(void)
{
	guint const n = 1000;

	guint32 server_count;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKVIterator) iterator = NULL;
		g_autoptr(JKVIterator) iterator_prefix = NULL;

		iterator = j_kv_iterator_new("test-ns", NULL);
		g_assert_nonnull(iterator);

		iterator_prefix = j_kv_iterator_new("test-ns", "test-kv-new-free");
		g_assert_nonnull(iterator_prefix);
	}

	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_KV);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JKVIterator) iterator = NULL;
		g_autoptr(JKVIterator) iterator_prefix = NULL;

		iterator = j_kv_iterator_new_for_index(i, "test-ns", NULL);
		g_assert_nonnull(iterator);

		iterator_prefix = j_kv_iterator_new_for_index(i, "test-ns", "test-kv-new-free-index");
		g_assert_nonnull(iterator_prefix);
	}
}

static void
test_kv_iterator_next_get(void)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JKVIterator) kv_iterator = NULL;
	g_autoptr(JKVIterator) kv_iterator_prefix = NULL;
	gboolean ret;
	guint32 server_count;

	guint kvs = 0;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;

		g_autofree gchar* key = NULL;
		gchar* value = NULL;

		key = g_strdup_printf("test-key-next-get-%d-%d", i % 2, i);
		value = g_strdup_printf("test-value-%d", i);
		kv = j_kv_new("test-ns", key);
		j_kv_put(kv, value, strlen(value) + 1, g_free, batch);
		j_kv_delete(kv, delete_batch);

		g_assert_nonnull(kv);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	kv_iterator = j_kv_iterator_new("test-ns", NULL);

	while (j_kv_iterator_next(kv_iterator))
	{
		gchar const* key;
		gconstpointer value;
		guint32 len;

		key = j_kv_iterator_get(kv_iterator, &value, &len);
		g_assert_true(g_str_has_prefix(key, "test-key-next-get-"));
		g_assert_true(g_str_has_prefix(value, "test-value-"));
		kvs++;
	}

	g_assert_cmpuint(kvs, ==, n);

	kvs = 0;
	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_KV);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JKVIterator) iterator = NULL;

		iterator = j_kv_iterator_new_for_index(i, "test-ns", NULL);

		while (j_kv_iterator_next(iterator))
		{
			gchar const* key;
			gconstpointer value;
			guint32 len;

			key = j_kv_iterator_get(iterator, &value, &len);
			g_assert_true(g_str_has_prefix(key, "test-key-next-get-"));
			g_assert_true(g_str_has_prefix(value, "test-value-"));
			kvs++;
		}
	}

	g_assert_cmpuint(kvs, ==, n);

	kvs = 0;
	kv_iterator_prefix = j_kv_iterator_new("test-ns", "test-key-next-get-1-");

	while (j_kv_iterator_next(kv_iterator_prefix))
	{
		gchar const* key;
		gconstpointer value;
		guint32 len;

		key = j_kv_iterator_get(kv_iterator_prefix, &value, &len);
		g_assert_true(g_str_has_prefix(key, "test-key-next-get-1-"));
		g_assert_true(g_str_has_prefix(value, "test-value-"));
		kvs++;
	}

	g_assert_cmpuint(kvs, ==, n / 2);

	kvs = 0;
	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_KV);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JKVIterator) iterator = NULL;

		iterator = j_kv_iterator_new_for_index(i, "test-ns", "test-key-next-get-1-");

		while (j_kv_iterator_next(iterator))
		{
			gchar const* key;
			gconstpointer value;
			guint32 len;

			key = j_kv_iterator_get(iterator, &value, &len);
			g_assert_true(g_str_has_prefix(key, "test-key-next-get-1-"));
			g_assert_true(g_str_has_prefix(value, "test-value-"));
			kvs++;
		}
	}

	g_assert_cmpuint(kvs, ==, n / 2);

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);
}

void
test_kv_kv_iterator(void)
{
	g_test_add_func("/kv/kv-iterator/new_free", test_kv_iterator_new_free);
	g_test_add_func("/kv/kv-iterator/next_get", test_kv_iterator_next_get);
}
