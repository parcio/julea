/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019-2020 Michael Kuhn
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
test_kv_new_free(void)
{
	guint const n = 1000;

	guint32 server_count;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;

		kv = j_kv_new("test", "test-kv-new-free");
		g_assert_nonnull(kv);
	}

	server_count = j_configuration_get_server_count(j_configuration(), J_BACKEND_TYPE_KV);

	for (guint i = 0; i < server_count; i++)
	{
		g_autoptr(JKV) kv = NULL;

		kv = j_kv_new_for_index(i, "test", "test-kv-new-free-index");
		g_assert_nonnull(kv);
	}
}

static void
test_kv_ref_unref(void)
{
	guint const n = 1000;

	g_autoptr(JKV) kv = NULL;

	kv = j_kv_new("test", "test-kv-ref-unref");
	g_assert_nonnull(kv);

	for (guint i = 0; i < n; i++)
	{
		JKV* ref_kv;

		ref_kv = j_kv_ref(kv);
		g_assert_true(kv == ref_kv);
		j_kv_unref(kv);
	}
}

static void
test_kv_put_delete(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autofree gchar* value = NULL;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	kv = j_kv_new("test", "test-kv-put-delete");
	g_assert_nonnull(kv);

	j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
	j_kv_delete(kv, batch);

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_kv_delete(kv, batch);
	ret = j_batch_execute(batch);
	// FIXME this should return FALSE
	//g_assert_false(ret);
}

static void
test_kv_put_update(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autofree gchar* get_value;
	g_autofree gchar* value1 = NULL;
	g_autofree gchar* value2 = NULL;
	guint32 get_len;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value1 = g_strdup("first-value");
	value2 = g_strdup("second-value");

	kv = j_kv_new("test", "test-kv-put-update");
	g_assert_nonnull(kv);

	j_kv_put(kv, value1, strlen(value1) + 1, NULL, batch);
	j_kv_put(kv, value2, strlen(value2) + 1, NULL, batch);
	j_kv_get(kv, (gpointer)&get_value, &get_len, batch);
	j_kv_delete(kv, batch);

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	g_assert_cmpstr(get_value, ==, value2);
	g_assert_cmpuint(get_len, ==, strlen(value2) + 1);
}

static void
test_kv_get(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autofree gchar* get_value = NULL;
	g_autofree gchar* value = NULL;
	guint32 get_len = 42;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	kv = j_kv_new("test", "test-kv-get");
	g_assert_nonnull(kv);

	j_kv_get(kv, (gpointer)&get_value, &get_len, batch);
	ret = j_batch_execute(batch);
	g_assert_false(ret);

	g_assert_null(get_value);
	g_assert_cmpuint(get_len, ==, 42);

	j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_kv_get(kv, (gpointer)&get_value, &get_len, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	g_assert_cmpstr(value, ==, get_value);
	g_assert_cmpuint(strlen(value) + 1, ==, get_len);

	j_kv_delete(kv, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
}

static guint num_callbacks = 0;

static void
get_callback(gpointer value, guint32 length, gpointer data)
{
	g_assert_cmpstr(value, ==, "kv-value");
	g_assert_cmpuint(length, ==, strlen("kv-value") + 1);
	g_assert_null(data);

	g_free(value);

	num_callbacks++;
}

static void
test_kv_get_callback(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autofree gchar* value = NULL;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	kv = j_kv_new("test", "test-kv-get-callback");
	g_assert_nonnull(kv);

	j_kv_get_callback(kv, get_callback, NULL, batch);
	ret = j_batch_execute(batch);
	g_assert_false(ret);

	j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_kv_get_callback(kv, get_callback, NULL, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_kv_delete(kv, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	g_assert_cmpuint(num_callbacks, ==, 1);
}

void
test_kv_kv(void)
{
	g_test_add_func("/kv/kv/new_free", test_kv_new_free);
	g_test_add_func("/kv/kv/ref_unref", test_kv_ref_unref);
	g_test_add_func("/kv/kv/put_delete", test_kv_put_delete);
	g_test_add_func("/kv/kv/put_update", test_kv_put_update);
	g_test_add_func("/kv/kv/get", test_kv_get);
	g_test_add_func("/kv/kv/get_callback", test_kv_get_callback);
}
