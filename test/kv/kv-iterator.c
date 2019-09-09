/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

static
void
test_kv_iterator_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;

		kv = j_kv_new("test-ns", "test-kv");

		g_assert(kv != NULL);
	}
}

static
void
test_kv_iterator_next_get (void)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JKVIterator) kv_iterator = NULL;
	gboolean ret;

	guint kvs = 0;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JKV) kv = NULL;

		g_autofree gchar* key = NULL;
		gchar* value = NULL;

		key = g_strdup_printf("test-key-%d", i);
		value = g_strdup_printf("test-value-%d", i);
		kv = j_kv_new("test-ns", key);
		j_kv_put(kv, value, strlen(value) + 1, g_free, batch);
		j_kv_delete(kv, delete_batch);

		g_assert(kv != NULL);
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
		g_assert(g_str_has_prefix(key, "test-key-"));
		g_assert(g_str_has_prefix(value, "test-value-"));
		kvs++;
	}

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	g_assert_cmpuint(kvs, ==, n);
}

void
test_kv_iterator (void)
{
	g_test_add_func("/kv/kv-iterator/new_free", test_kv_iterator_new_free);
	g_test_add_func("/kv/kv-iterator/next_get", test_kv_iterator_next_get);
}
