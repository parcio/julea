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

#include <jbatch-internal.h>

#include "test.h"

static gint test_operation_flag;

static
void
on_operation_completed (JBatch* operation, gboolean ret, gpointer user_data)
{
	g_atomic_int_set(&test_operation_flag, 1);
}

static
void
test_operation_new_free (void)
{
	JBatch* operation;

	operation = j_operation_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	g_assert(operation != NULL);

	j_operation_unref(operation);
}

static
void
test_operation_semantics (void)
{
	JBatch* operation;
	JSemantics* semantics;

	operation = j_operation_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	g_assert(j_operation_get_semantics(operation) != NULL);

	j_operation_unref(operation);

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	operation = j_operation_new(semantics);

	g_assert(j_operation_get_semantics(operation) == semantics);

	j_semantics_unref(semantics);
	j_operation_unref(operation);
}

static
void
_test_operation_execute (gboolean async)
{
	JCollection* collection;
	JItem* item;
	JBatch* operation;
	JStore* store;

	if (async)
	{
		g_atomic_int_set(&test_operation_flag, 0);
	}

	operation = j_operation_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	store = j_store_new("test");
	collection = j_collection_new("test");
	item = j_item_new("item");

	j_create_store(store, operation);
	j_store_create_collection(store, collection, operation);
	j_collection_create_item(collection, item, operation);
	j_collection_delete_item(collection, item, operation);
	j_store_delete_collection(store, collection, operation);
	j_delete_store(store, operation);

	j_item_unref(item);
	j_collection_unref(collection);
	j_store_unref(store);

	if (async)
	{
		j_operation_execute_async(operation, on_operation_completed, NULL);
	}
	else
	{
		j_operation_execute(operation);
	}

	j_operation_unref(operation);

	if (async)
	{
		while (g_atomic_int_get(&test_operation_flag) != 1)
		{
			g_usleep(1000);
		}
	}
}

static
void
test_operation_execute (void)
{
	_test_operation_execute(FALSE);
}

static
void
test_operation_execute_async (void)
{
	_test_operation_execute(TRUE);
}

void
test_operation (void)
{
	g_test_add_func("/operation/new_free", test_operation_new_free);
	g_test_add_func("/operation/semantics", test_operation_semantics);
	g_test_add_func("/operation/execute", test_operation_execute);
	g_test_add_func("/operation/execute_async", test_operation_execute_async);
}
