/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <glib.h>

#include <julea.h>

#include "benchmark.h"

static
gchar*
_benchmark_item_create (gboolean batch)
{
	guint const n = (batch) ? 100000 : 1000;

	JCollection* collection;
	JOperation* delete_operation;
	JOperation* operation;
	JStore* store;
	gdouble elapsed;

	delete_operation = j_operation_new(NULL);
	operation = j_operation_new(NULL);

	store = j_store_new("test");
	collection = j_collection_new("test");
	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_operation_execute(operation);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		item = j_item_new(name);
		g_free(name);

		j_collection_create_item(collection, item, operation);
		j_collection_delete_item(collection, item, delete_operation);
		j_item_unref(item);

		if (!batch)
		{
			j_operation_execute(operation);
		}
	}

	if (batch)
	{
		j_operation_execute(operation);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_store_delete_collection(store, collection, delete_operation);
	j_delete_store(store, delete_operation);
	j_collection_unref(collection);
	j_store_unref(store);
	j_operation_execute(delete_operation);

	j_operation_free(delete_operation);
	j_operation_free(operation);

	return g_strdup_printf("%f seconds (%f/s)", elapsed, (gdouble)n / elapsed);
}

static
gchar*
benchmark_item_create (void)
{
	return _benchmark_item_create(FALSE);
}

static
gchar*
benchmark_item_create_batch (void)
{
	return _benchmark_item_create(TRUE);
}

static
gchar*
_benchmark_item_delete (gboolean batch)
{
	guint const n = 10000;

	JCollection* collection;
	JOperation* operation;
	JStore* store;
	gdouble elapsed;

	operation = j_operation_new(NULL);

	store = j_store_new("test");
	collection = j_collection_new("test");
	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_operation_execute(operation);

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		item = j_item_new(name);
		g_free(name);

		j_collection_create_item(collection, item, operation);
		j_item_unref(item);
	}

	j_operation_execute(operation);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		j_collection_get_item(collection, &item, name, J_ITEM_STATUS_NONE, operation);
		j_operation_execute(operation);
		g_free(name);

		j_collection_delete_item(collection, item, operation);
		j_item_unref(item);

		if (!batch)
		{
			j_operation_execute(operation);
		}
	}

	if (batch)
	{
		j_operation_execute(operation);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);
	j_collection_unref(collection);
	j_store_unref(store);
	j_operation_execute(operation);

	j_operation_free(operation);

	return g_strdup_printf("%f seconds (%f/s)", elapsed, (gdouble)n / elapsed);
}

static
gchar*
benchmark_item_delete (void)
{
	return _benchmark_item_delete(FALSE);
}

static
gchar*
benchmark_item_delete_batch (void)
{
	return _benchmark_item_delete(TRUE);
}

static
gchar*
benchmark_item_delete_batch_without_get (void)
{
	guint const n = 100000;

	JCollection* collection;
	JOperation* delete_operation;
	JOperation* operation;
	JStore* store;
	gdouble elapsed;

	delete_operation = j_operation_new(NULL);
	operation = j_operation_new(NULL);

	store = j_store_new("test");
	collection = j_collection_new("test");
	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_operation_execute(operation);

	for (guint i = 0; i < n; i++)
	{
		JItem* item;
		gchar* name;

		name = g_strdup_printf("test-%d", i);
		item = j_item_new(name);
		g_free(name);

		j_collection_create_item(collection, item, operation);
		j_collection_delete_item(collection, item, delete_operation);
		j_item_unref(item);
	}

	j_operation_execute(operation);

	j_benchmark_timer_start();

	j_operation_execute(delete_operation);

	elapsed = j_benchmark_timer_elapsed();

	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);
	j_collection_unref(collection);
	j_store_unref(store);
	j_operation_execute(operation);

	j_operation_free(delete_operation);
	j_operation_free(operation);

	return g_strdup_printf("%f seconds (%f/s)", elapsed, (gdouble)n / elapsed);
}

void
benchmark_item (void)
{
	j_benchmark_run("/item/create", benchmark_item_create);
	j_benchmark_run("/item/create-batch", benchmark_item_create_batch);
	j_benchmark_run("/item/delete", benchmark_item_delete);
	j_benchmark_run("/item/delete-batch", benchmark_item_delete_batch);
	j_benchmark_run("/item/delete-batch-without-get", benchmark_item_delete_batch_without_get);
	/* FIXME get */
}
