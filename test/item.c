/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
test_item_fixture_setup (JItem** item, gconstpointer data)
{
	JBatch* batch;
	JCollection* collection;

	(void)data;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	collection = j_collection_create("test-collection", batch);
	*item = j_item_create(collection, "test-item", NULL, batch);

	j_collection_unref(collection);
	j_batch_unref(batch);
}

static
void
test_item_fixture_teardown (JItem** item, gconstpointer data)
{
	(void)data;

	j_item_unref(*item);
}

static
void
test_item_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JBatch* batch;
		JCollection* collection;
		JItem* item;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		collection = j_collection_create("test-collection", batch);
		item = j_item_create(collection, "test-item", NULL, batch);
		j_collection_unref(collection);
		j_batch_unref(batch);

		g_assert(item != NULL);
		j_item_unref(item);
	}
}

static
void
test_item_name (JItem** item, gconstpointer data)
{
	(void)data;

	g_assert_cmpstr(j_item_get_name(*item), ==, "test-item");
}

static
void
test_item_size (JItem** item, gconstpointer data)
{
	(void)data;

	g_assert_cmpuint(j_item_get_size(*item), ==, 0);
}

static
void
test_item_modification_time (JItem** item, gconstpointer data)
{
	(void)data;

	g_assert_cmpuint(j_item_get_modification_time(*item), >, 0);
}

void
test_item (void)
{
	g_test_add_func("/item/new_free", test_item_new_free);
	g_test_add("/item/name", JItem*, NULL, test_item_fixture_setup, test_item_name, test_item_fixture_teardown);
	g_test_add("/item/size", JItem*, NULL, test_item_fixture_setup, test_item_size, test_item_fixture_teardown);
	g_test_add("/item/modification_time", JItem*, NULL, test_item_fixture_setup, test_item_modification_time, test_item_fixture_teardown);
}
