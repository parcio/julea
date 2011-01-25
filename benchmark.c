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
#include <glib-object.h>

#include <julea.h>

int
main (int argc, char** argv)
{
	JCollectionIterator* citerator;
	JConnection* connection;
	JStore* store;
	JStoreIterator* siterator;
	JSemantics* semantics;
	JList* collections;
	JListIterator* cliterator;

	if (!g_thread_get_initialized())
	{
		g_thread_init(NULL);
	}

	g_type_init();

	if (!j_init())
	{
		return 1;
	}

	connection = j_connection_new();

	if (!j_connection_connect(connection))
	{
		j_connection_unref(connection);

		return 1;
	}

	store = j_connection_get(connection, "JULEA");

	semantics = j_semantics_new();
	j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_LAX);

	//j_store_set_semantics(store, semantics);

	collections = j_list_new((JListFreeFunc)j_collection_unref);

	for (guint i = 0; i < 10; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%u", i);

		collection = j_collection_new(name);
		j_collection_set_semantics(collection, semantics);

		j_list_append(collections, collection);

		g_free(name);
	}

	j_store_create(store, collections);

	cliterator = j_list_iterator_new(collections);

	while (j_list_iterator_next(cliterator))
	{
		JCollection* collection = j_list_iterator_get(cliterator);
		JList* items;

		items = j_list_new((JListFreeFunc)j_item_unref);

		for (guint i = 0; i < 10000; i++)
		{
			JItem* item;
			gchar* name;

			name = g_strdup_printf("test-%u", i);

			item = j_item_new(name);
			//j_item_set_semantics(item, semantics);

			j_list_append(items, item);

			g_free(name);
		}

		j_collection_create(collection, items);

		j_list_unref(items);
	}

	j_list_iterator_free(cliterator);

	siterator = j_store_iterator_new(store);

	while (j_store_iterator_next(siterator))
	{
		JCollection* collection = j_store_iterator_get(siterator);

		g_print("%s ", j_collection_name(collection));
		j_collection_unref(collection);
	}

	g_print("\n");

	j_store_iterator_free(siterator);

	citerator = j_collection_iterator_new(j_list_get(collections, 0));

	while (j_collection_iterator_next(citerator))
	{
		JItem* item = j_collection_iterator_get(citerator);

		g_print("%s ", j_item_name(item));
		j_item_unref(item);
	}

	g_print("\n");

	j_collection_iterator_free(citerator);

	j_list_unref(collections);

	j_semantics_unref(semantics);
	j_store_unref(store);
	j_connection_unref(connection);

	return 0;
}
