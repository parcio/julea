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
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;

		kv = j_kv_new("test", "test-kv");
		g_assert_true(kv != NULL);
	}
}

static void
test_kv_ref_unref(void)
{
	guint const n = 100000;

	g_autoptr(JKV) kv = NULL;

	kv = j_kv_new("test", "test-kv");
	g_assert_true(kv != NULL);

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
	guint const n = 100;

	g_autoptr(JBatch) batch = NULL;
	g_autofree gchar* value = NULL;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("test-kv-%u", i);
		kv = j_kv_new("test", name);
		g_assert_true(kv != NULL);

		j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
		j_kv_delete(kv, batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);
}

static void
test_kv_get(void)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autofree gchar* get_value;
	g_autofree gchar* value = NULL;
	guint32 get_len;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = g_strdup("kv-value");

	kv = j_kv_new("test", "test-kv");
	g_assert_true(kv != NULL);

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

static void
get_callback(gpointer value, guint32 length, gpointer data)
{
	g_assert_cmpstr(value, ==, "kv-value");
	g_assert_cmpuint(length, ==, strlen("kv-value") + 1);
	g_assert_null(data);

	g_free(value);
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

	kv = j_kv_new("test", "test-kv");
	g_assert_true(kv != NULL);

	j_kv_put(kv, value, strlen(value) + 1, NULL, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_kv_get_callback(kv, get_callback, NULL, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_kv_delete(kv, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);
}

void
test_kv_kv(void)
{
	g_test_add_func("/kv/kv/new_free", test_kv_new_free);
	g_test_add_func("/kv/kv/ref_unref", test_kv_ref_unref);
	g_test_add_func("/kv/kv/put_delete", test_kv_put_delete);
	g_test_add_func("/kv/kv/get", test_kv_get);
	g_test_add_func("/kv/kv/get_callback", test_kv_get_callback);
}
