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

#include "test.h"

static
void
test_collection_fixture_setup (JCollection** collection, gconstpointer data)
{
	JBatch* batch;
	JStore* store;

	(void)data;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	store = j_store_new("test-store");
	*collection = j_store_create_collection(store, "test-collection", batch);

	j_store_unref(store);
	j_batch_unref(batch);
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
		JBatch* batch;
		JCollection* collection;
		JStore* store;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		store = j_store_new("test-store");
		collection = j_store_create_collection(store, "test-collection", batch);
		j_collection_unref(collection);
		j_batch_unref(batch);

		g_assert(collection != NULL);
		j_store_unref(store);
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
	g_test_add_func("/collection/new_free", test_collection_new_free);
	g_test_add("/collection/name", JCollection*, NULL, test_collection_fixture_setup, test_collection_name, test_collection_fixture_teardown);
}
