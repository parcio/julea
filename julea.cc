#include <iostream>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "julea.h"

using namespace std;
using namespace mongo;

int main (int argc, char** argv)
{
	/*
	BSONObj o;

	c->ensureIndex("fs.files", BSONObjBuilder().append("name", 1).obj());

	for (int i = 0; i < 1000000; i++)
	{
		o = BSONObjBuilder()
			.genOID()
			.append("name", i)
			.obj();

		c->insert("fs.files", o);
	}

	auto_ptr<DBClientCursor> cur = c->query("fs.files", BSONObjBuilder().append("name", 1).obj());

	while (cur->more())
	{
		cout << cur->next().toString() << endl;
	}
	*/

//	c.done();

	JULEA::Directory d("/");

	d.Create();

	for (int j = 0; j < 1; j++)
	{
	for (int i = 0; i < 10000; i++)
	{
		JULEA::File f;

		string name("test");

		name += JULEA::Common::ToString(i);
		f = JULEA::File(name);

		d.Add(f);
	}
		d.CreateFiles();
}

	return 0;
}
