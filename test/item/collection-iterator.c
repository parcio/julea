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
test_collection_iterator_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JBatch) batch = NULL;
		g_autoptr(JCollection) collection = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		collection = j_collection_create("test-collection", batch);

		g_assert(collection != NULL);
	}
}

static
void
test_collection_iterator_next_get (void)
{
	guint const n = 1000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JCollectionIterator) collection_iterator = NULL;
	gboolean ret;

	guint collections = 0;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JCollection) collection = NULL;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("test-collection-%d", i);
		collection = j_collection_create(name, batch);
		j_collection_delete(collection, delete_batch);

		g_assert(collection != NULL);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	collection_iterator = j_collection_iterator_new();

	while (j_collection_iterator_next(collection_iterator))
	{
		g_autoptr(JCollection) collection = j_collection_iterator_get(collection_iterator);
		(void)collection;
		collections++;
	}

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	g_assert_cmpuint(collections, ==, n);
}

void
test_collection_iterator (void)
{
	g_test_add_func("/item/collection-iterator/new_free", test_collection_iterator_new_free);
	g_test_add_func("/item/collection-iterator/next_get", test_collection_iterator_next_get);
}
