#include <iostream>

#include <boost/lexical_cast.hpp>

#include "julea.h"

using namespace boost;
using namespace std;
using namespace JULEA;

int main (int argc, char** argv)
{
	if (argc < 2)
	{
		return 1;
	}

	Store* s = new Store(argv[1]);
	list<Collection*> collectionsList;
	map<string, Collection*> collections;
	list<string> itemsList;
	map<string, Item*> items;

	for (int j = 0; j < 10; j++)
	{
		string name("test-" + lexical_cast<string>(j));

		collectionsList.push_back(new Collection(name));
	}

	for (int j = 0; j < 10; j++)
	{
		for (int i = 0; i < 10000; i++)
		{
			string name("test-" + lexical_cast<string>(j) + "-" + lexical_cast<string>(i));

			itemsList.push_back(name);
		}
	}

	map<string, Collection*>::iterator it;

	s->Create(collectionsList);

	for (it = collections.begin(); it != collections.end(); ++it)
	{
//		items = it->second->Create(itemsList);
	}

	/*
	Collection::Iterator i(Collection("test-3"));

	while (i.More())
	{
		Item f = i.Next();

		cout << f.Name() << endl;
	}
	*/

	return 0;
}
