#include <iostream>

#include <boost/lexical_cast.hpp>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "julea.h"

using namespace boost;
using namespace std;
using namespace mongo;

int main (int argc, char** argv)
{
	if (argc < 2)
	{
		return 1;
	}

	JULEA::Store* s = new JULEA::Store(argv[1]);

	for (int j = 0; j < 10; j++)
	{
		string path("test-" + lexical_cast<string>(j));
		JULEA::Collection* d = s->Get(path);

//		d.Create();

		for (int i = 0; i < 10000; i++)
		{
			string name("test-" + lexical_cast<string>(j) + "-" + lexical_cast<string>(i));
			JULEA::Item* i = d->Get(name);

			cout << i->Name() << endl;
		}

//		d.CreateFiles();
	}

	/*
	JULEA::Collection::Iterator i(JULEA::Collection("test-3"));

	while (i.More())
	{
		JULEA::Item f = i.Next();

		cout << f.Name() << endl;
	}
	*/

	return 0;
}
