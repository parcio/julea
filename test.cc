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

	for (int j = 0; j < 10; j++)
	{
		string path("test-" + lexical_cast<string>(j));
		Collection* c = s->Get(path);

//		d.Create();

		for (int i = 0; i < 10000; i++)
		{
			string name("test-" + lexical_cast<string>(j) + "-" + lexical_cast<string>(i));
			Item* i = c->Get(name);

			cout << i->Name() << endl;
		}

//		d.CreateFiles();
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
