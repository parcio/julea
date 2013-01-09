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

#include <string.h>

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
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_operation = j_operation_new(semantics);
	operation = j_operation_new(semantics);

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

	j_operation_unref(delete_operation);
	j_operation_unref(operation);
	j_semantics_unref(semantics);

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
	JOperation* get_operation;
	JOperation* operation;
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	get_operation = j_operation_new(semantics);
	operation = j_operation_new(semantics);

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
		j_collection_get_item(collection, &item, name, J_ITEM_STATUS_NONE, get_operation);
		j_operation_execute(get_operation);
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

	j_operation_unref(get_operation);
	j_operation_unref(operation);
	j_semantics_unref(semantics);

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
	JSemantics* semantics;
	JStore* store;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	delete_operation = j_operation_new(semantics);
	operation = j_operation_new(semantics);

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

	j_operation_unref(delete_operation);
	j_operation_unref(operation);
	j_semantics_unref(semantics);

	return g_strdup_printf("%f seconds (%f/s)", elapsed, (gdouble)n / elapsed);
}

static
gchar*
_benchmark_item_get_status (gboolean batch)
{
	guint const n = (batch) ? 1000 : 1000;

	JCollection* collection;
	JItem* item;
	JOperation* operation;
	JSemantics* semantics;
	JStore* store;
	gchar dummy[1];
	gchar* ret;
	gdouble elapsed;
	guint64 nb;

	memset(dummy, 0, 1);

	semantics = j_benchmark_get_semantics();
	operation = j_operation_new(semantics);

	store = j_store_new("test");
	collection = j_collection_new("test");
	item = j_item_new("test");
	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_collection_create_item(collection, item, operation);
	j_item_write(item, dummy, 1, 0, &nb, operation);

	j_operation_execute(operation);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_item_get_status(item, J_ITEM_STATUS_MODIFICATION_TIME | J_ITEM_STATUS_SIZE, operation);

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

	j_collection_delete_item(collection, item, operation);
	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);
	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
	j_operation_execute(operation);

	j_operation_unref(operation);
	j_semantics_unref(semantics);

	ret = g_strdup_printf("%f seconds (%f/s)", elapsed, n / elapsed);

	return ret;
}

static
gchar*
benchmark_item_get_status (void)
{
	return _benchmark_item_get_status(FALSE);
}

static
gchar*
benchmark_item_get_status_batch (void)
{
	return _benchmark_item_get_status(TRUE);
}

static
gchar*
_benchmark_item_read (gboolean batch, guint block_size)
{
	guint const n = (batch) ? 1000 : 1000;

	JCollection* collection;
	JItem* item;
	JOperation* operation;
	JSemantics* semantics;
	JStore* store;
	gchar dummy[block_size];
	gchar* ret;
	gchar* size;
	gdouble elapsed;
	guint64 nb;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	operation = j_operation_new(semantics);

	store = j_store_new("test");
	collection = j_collection_new("test");
	item = j_item_new("test");
	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_collection_create_item(collection, item, operation);

	for (guint i = 0; i < n; i++)
	{
		j_item_write(item, dummy, block_size, i * block_size, &nb, operation);
	}

	j_operation_execute(operation);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_item_read(item, dummy, block_size, i * block_size, &nb, operation);

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

	j_collection_delete_item(collection, item, operation);
	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);
	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
	j_operation_execute(operation);

	j_operation_unref(operation);
	j_semantics_unref(semantics);

	size = g_format_size((n * block_size) / elapsed);
	ret = g_strdup_printf("%f seconds (%s/s)", elapsed, size);
	g_free(size);

	return ret;
}

static
gchar*
benchmark_item_read (void)
{
	return _benchmark_item_read(FALSE, 4 * 1024);
}

static
gchar*
benchmark_item_read_batch (void)
{
	return _benchmark_item_read(TRUE, 4 * 1024);
}

static
gchar*
_benchmark_item_write (gboolean batch, guint block_size)
{
	guint const n = (batch) ? 1000 : 1000;

	JCollection* collection;
	JItem* item;
	JOperation* operation;
	JSemantics* semantics;
	JStore* store;
	gchar dummy[block_size];
	gchar* ret;
	gchar* size;
	gdouble elapsed;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	operation = j_operation_new(semantics);

	store = j_store_new("test");
	collection = j_collection_new("test");
	item = j_item_new("test");
	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_collection_create_item(collection, item, operation);
	j_operation_execute(operation);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		guint64 nb;

		j_item_write(item, &dummy, block_size, i * block_size, &nb, operation);

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

	j_collection_delete_item(collection, item, operation);
	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);
	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);
	j_operation_execute(operation);

	j_operation_unref(operation);
	j_semantics_unref(semantics);

	size = g_format_size((n * block_size) / elapsed);
	ret = g_strdup_printf("%f seconds (%s/s)", elapsed, size);
	g_free(size);

	return ret;
}

static
gchar*
benchmark_item_write (void)
{
	return _benchmark_item_write(FALSE, 4 * 1024);
}

static
gchar*
benchmark_item_write_batch (void)
{
	return _benchmark_item_write(TRUE, 4 * 1024);
}

void
benchmark_item (void)
{
	j_benchmark_run("/item/create", benchmark_item_create);
	j_benchmark_run("/item/create-batch", benchmark_item_create_batch);
	j_benchmark_run("/item/delete", benchmark_item_delete);
	j_benchmark_run("/item/delete-batch", benchmark_item_delete_batch);
	j_benchmark_run("/item/delete-batch-without-get", benchmark_item_delete_batch_without_get);
	j_benchmark_run("/item/get-status", benchmark_item_get_status);
	j_benchmark_run("/item/get-status-batch", benchmark_item_get_status_batch);
	/* FIXME get */
	j_benchmark_run("/item/read", benchmark_item_read);
	j_benchmark_run("/item/read-batch", benchmark_item_read_batch);
	j_benchmark_run("/item/write", benchmark_item_write);
	j_benchmark_run("/item/write-batch", benchmark_item_write_batch);
}
