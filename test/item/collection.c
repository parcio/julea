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
test_collection_fixture_setup (JCollection** collection, gconstpointer data)
{
	g_autoptr(JBatch) batch = NULL;

	(void)data;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	*collection = j_collection_create("test-collection", batch);
}

static
void
test_collection_fixture_teardown (JCollection** collection, gconstpointer data)
{
	(void)data;

	j_collection_unref(*collection);
}

static
void
test_collection_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JBatch) batch = NULL;
		g_autoptr(JCollection) collection = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		collection = j_collection_create("test-collection", batch);

		g_assert_true(collection != NULL);
	}
}

static
void
test_collection_name (JCollection** collection, gconstpointer data)
{
	(void)data;

	g_assert_cmpstr(j_collection_get_name(*collection), ==, "test-collection");
}

void
test_collection (void)
{
	g_test_add_func("/item/collection/new_free", test_collection_new_free);
	g_test_add("/item/collection/name", JCollection*, NULL, test_collection_fixture_setup, test_collection_name, test_collection_fixture_teardown);
}
