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
#include <julea-item.h>

#include "test.h"

static
void
test_item_iterator_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JBatch) batch = NULL;
		g_autoptr(JCollection) collection = NULL;
		g_autoptr(JItem) item = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		collection = j_collection_create("test-collection", batch);
		item = j_item_create(collection, "test-item", NULL, batch);

		g_assert_true(item != NULL);
	}
}

static
void
test_item_iterator_next_get (void)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JItemIterator) item_iterator = NULL;
	gboolean ret;

	guint items = 0;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	collection = j_collection_create("test-collection", batch);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JItem) item = NULL;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("test-item-%d", i);
		item = j_item_create(collection, name, NULL, batch);
		j_item_delete(item, delete_batch);

		g_assert_true(item != NULL);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	item_iterator = j_item_iterator_new(collection);

	while (j_item_iterator_next(item_iterator))
	{
		g_autoptr(JItem) item = j_item_iterator_get(item_iterator);
		(void)item;
		items++;
	}

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	g_assert_cmpuint(items, ==, n);
}

void
test_item_iterator (void)
{
	g_test_add_func("/item/item-iterator/new_free", test_item_iterator_new_free);
	g_test_add_func("/item/item-iterator/next_get", test_item_iterator_next_get);
}
