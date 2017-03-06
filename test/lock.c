/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include <jlock-internal.h>

#include "test.h"

static
void
test_lock_new_free (void)
{
	guint const n = 100;

	JCollection* collection;
	JItem* item;
	JBatch* batch;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	collection = j_collection_create("test-collection", batch);
	item = j_item_create(collection, "test-item", NULL, batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JLock* lock;

		lock = j_lock_new(item);
		g_assert(lock != NULL);
		j_lock_free(lock);
	}

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_unref(batch);
}

static
void
test_lock_acquire_release (void)
{
	guint const n = 1000;

	JCollection* collection;
	JItem* item;
	JBatch* batch;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	collection = j_collection_create("test-collection", batch);
	item = j_item_create(collection, "test-item", NULL, batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JLock* lock;
		gboolean ret;

		lock = j_lock_new(item);

		ret = j_lock_acquire(lock);
		g_assert(ret);
		ret = j_lock_release(lock);
		g_assert(ret);

		j_lock_free(lock);
	}

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_unref(batch);
}

static
void
test_lock_add (void)
{
	guint const n = 1000;

	JCollection* collection;
	JItem* item;
	JLock* lock;
	JBatch* batch;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	collection = j_collection_create("test-collection", batch);
	item = j_item_create(collection, "test-item", NULL, batch);
	j_batch_execute(batch);

	lock = j_lock_new(item);

	for (guint i = 0; i < n; i++)
	{
		j_lock_add(lock, i);
	}

	ret = j_lock_acquire(lock);
	g_assert(ret);
	ret = j_lock_release(lock);
	g_assert(ret);

	j_lock_free(lock);

	j_item_delete(collection, item, batch);
	j_collection_delete(collection, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_batch_unref(batch);
}

void
test_lock (void)
{
	g_test_add_func("/lock/new_free", test_lock_new_free);
	g_test_add_func("/lock/acquire_release", test_lock_acquire_release);
	g_test_add_func("/lock/add", test_lock_add);
}
