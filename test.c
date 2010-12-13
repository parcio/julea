#include <glib.h>

#include "julea.h"

int main (int argc, char** argv)
{
	JConnection* connection;
	JStore* store;
	JSemantics* semantics;
	JCollection* collections;
	JItem* items;

	if (argc < 2)
	{
		return 1;
	}

	connection = j_connection_new();

	for (int i = 1; i < argc; ++i)
	{
		j_connection_add_server(connection, argv[i]);
	}

	j_connection_connect(connection);
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

		//collections.push_back(collection);
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

	return 0;
}
