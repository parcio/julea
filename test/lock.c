/*
 * Copyright (c) 2010-2013 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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
	JStore* store;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	store = j_store_new("test-store");
	collection = j_collection_new("test-collection");
	item = j_item_new("test-item", NULL);

	j_create_store(store, batch);
	j_store_create_collection(store, collection, batch);
	j_collection_create_item(collection, item, batch);
	j_batch_execute(batch);

	for (guint i = 0; i < n; i++)
	{
		JLock* lock;

		lock = j_lock_new(item);
		g_assert(lock != NULL);
		j_lock_free(lock);
	}

	j_collection_delete_item(collection, item, batch);
	j_store_delete_collection(store, collection, batch);
	j_delete_store(store, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
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
	JStore* store;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	store = j_store_new("test-store");
	collection = j_collection_new("test-collection");
	item = j_item_new("test-item", NULL);

	j_create_store(store, batch);
	j_store_create_collection(store, collection, batch);
	j_collection_create_item(collection, item, batch);
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

	j_collection_delete_item(collection, item, batch);
	j_store_delete_collection(store, collection, batch);
	j_delete_store(store, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
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
	JStore* store;
	gboolean ret;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	store = j_store_new("test-store");
	collection = j_collection_new("test-collection");
	item = j_item_new("test-item", NULL);

	j_create_store(store, batch);
	j_store_create_collection(store, collection, batch);
	j_collection_create_item(collection, item, batch);
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

	j_collection_delete_item(collection, item, batch);
	j_store_delete_collection(store, collection, batch);
	j_delete_store(store, batch);
	j_batch_execute(batch);

	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
	j_batch_unref(batch);
}

void
test_lock (void)
{
	g_test_add_func("/lock/new_free", test_lock_new_free);
	g_test_add_func("/lock/acquire_release", test_lock_acquire_release);
	g_test_add_func("/lock/add", test_lock_add);
}
