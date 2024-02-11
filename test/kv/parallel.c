/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2022-2024 Michael Kuhn
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

#define TEST_THREAD_ASSERT(x) \
	if (!(x)) \
	{ \
		return; \
	}

static gint num_put_gets = 0;

static void
test_parallel_put_get_func(gpointer data, gpointer user_data)
{
	g_autoptr(JKV) kv = data;

	g_autoptr(JBatch) batch = NULL;
	g_autofree gchar* value = NULL;
	g_autofree gchar* get_value = NULL;
	guint32 get_len = 0;
	gboolean ret;

	(void)user_data;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
	ret = j_batch_execute(batch);
	TEST_THREAD_ASSERT(ret);

	j_kv_get(kv, (gpointer)&get_value, &get_len, batch);
	ret = j_batch_execute(batch);
	TEST_THREAD_ASSERT(ret);

	TEST_THREAD_ASSERT(g_strcmp0(value, get_value) == 0);
	TEST_THREAD_ASSERT(strlen(value) + 1 == get_len);

	j_kv_delete(kv, batch);
	ret = j_batch_execute(batch);
	TEST_THREAD_ASSERT(ret);

	g_atomic_int_inc(&num_put_gets);
}

static void
test_parallel_put_get(void)
{
	guint const n = 1000;

	GThreadPool* thread_pool;

	J_TEST_TRAP_START;
	thread_pool = g_thread_pool_new(test_parallel_put_get_func, NULL, j_configuration_get_max_connections(j_configuration()), TRUE, NULL);

	for (guint i = 0; i < n; i++)
	{
		JKV* kv = NULL;
		g_autofree gchar* key = NULL;

		key = g_strdup_printf("test-kv-parallel-put-get-%d", i);
		kv = j_kv_new("test", key);
		g_assert_nonnull(kv);

		g_thread_pool_push(thread_pool, kv, NULL);
	}

	g_thread_pool_free(thread_pool, FALSE, TRUE);
	g_assert_cmpuint(g_atomic_int_get(&num_put_gets), ==, n);
	J_TEST_TRAP_END;
}

static gint num_iterator = 0;

static void
test_parallel_iterator_func(gpointer data, gpointer user_data)
{
	g_autoptr(JKVIterator) kv_iterator = NULL;
	guint kvs = 0;

	(void)user_data;

	kv_iterator = j_kv_iterator_new("test", "test-kv-parallel-iterator-");

	while (j_kv_iterator_next(kv_iterator))
	{
		gchar const* key;
		gconstpointer value;
		guint32 len;

		key = j_kv_iterator_get(kv_iterator, &value, &len);
		TEST_THREAD_ASSERT(g_str_has_prefix(key, "test-kv-parallel-iterator-"));
		TEST_THREAD_ASSERT(g_strcmp0(value, "kv-value") == 0);
		TEST_THREAD_ASSERT(len == strlen("kv-value") + 1);
		kvs++;
	}

	TEST_THREAD_ASSERT(kvs == GPOINTER_TO_UINT(data));

	g_atomic_int_inc(&num_iterator);
}

static void
test_parallel_iterator(void)
{
	guint const n = 100;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autofree gchar* value = NULL;
	gboolean ret;

	GThreadPool* thread_pool;

	J_TEST_TRAP_START;
	thread_pool = g_thread_pool_new(test_parallel_iterator_func, NULL, j_configuration_get_max_connections(j_configuration()), TRUE, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;
		g_autofree gchar* key = NULL;

		key = g_strdup_printf("test-kv-parallel-iterator-%d", i);
		kv = j_kv_new("test", key);
		g_assert_nonnull(kv);

		j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
		j_kv_delete(kv, delete_batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	for (guint i = 0; i < n; i++)
	{
		g_thread_pool_push(thread_pool, GUINT_TO_POINTER(n), NULL);
	}

	g_thread_pool_free(thread_pool, FALSE, TRUE);

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	g_assert_cmpuint(g_atomic_int_get(&num_iterator), ==, n);
	J_TEST_TRAP_END;
}

void
test_kv_parallel(void)
{
	g_test_add_func("/kv/parallel/put_get", test_parallel_put_get);
	g_test_add_func("/kv/parallel/iterator", test_parallel_iterator);
}
