/*
 * Copyright (c) 2010 Michael Kuhn
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

#include "julea.h"

int main (int argc, char** argv)
{
	JConnection* connection;
	JStore* store;
	JSemantics* semantics;
	GList* collections = NULL;
	GList* items = NULL;

	if (argc != 2)
	{
		return 1;
	}

	connection = j_connection_new();

	if (!j_connection_connect(connection, argv[1]))
	{
		return 1;
	}

	store = j_connection_get(connection, "JULEA");

	semantics = j_semantics_new();
	j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_LAX);

	for (guint j = 0; j < 10; j++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%u", j);

		collection = j_collection_new(name);
		j_collection_set_semantics(collection, semantics);

		collections = g_list_prepend(collections, collection);

		g_free(name);
	}

	j_store_create(store, collections);

	/*
	list<Collection>::iterator it;

	for (it = collections.begin(); it != collections.end(); ++it)
	{
		for (int i = 0; i < 10000; i++)
		{
			string name("test-" + lexical_cast<string>(i));
			Item item(name);

	//		item->SetSemantics(semantics);
			items.push_back(item);
		}

		(*it)->Create(items);
	}

	Collection::Iterator iterator(collections.front());

	while (iterator.More())
	{
		Item i = iterator.Next();

//		cout << i->Name() << endl;
	}
	*/

	for (GList* l = collections; l != NULL; l = l->next)
	{
		j_collection_unref(l->data);
	}

	g_list_free(collections);

	j_semantics_unref(semantics);
	j_store_unref(store);
	j_connection_unref(connection);

	return 0;
}
