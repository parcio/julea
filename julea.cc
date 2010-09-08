#include <iostream>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "common.h"
#include "julea.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	string FileSystem::host;

	string const& File::Name ()
	{
		return name;
	}

	void Directory::Add (Directory const& directory)
	{
		newDirectories.push_back(directory);
	}

	void Directory::Add (File const& file)
	{
		newFiles.push_back(file);
	}

	bool Directory::Create ()
	{
		ScopedDbConnection c(FileSystem::Host());

		BSONObj d;
		BSONObj e;

		c->ensureIndex("JULEA.Directories", BSONObjBuilder().append("Path", 1).obj());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Directories", BSONObjBuilder().append("Path", path).obj(), 1);

		if (cur->more())
		{
			d = cur->next();
			id = d.getField("_id").OID();
			eid = d.getField("Entries").OID();
			c.done();

			return true;
		}

		e = BSONObjBuilder()
			.genOID()
			.obj();

		c->insert("JULEA.DirectoryEntries", e);

		eid = e.getField("_id").OID();

		d = BSONObjBuilder()
			.genOID()
			.append("Path", path)
			.append("Entries", eid)
			.obj();

		c->insert("JULEA.Directories", d);

		id = d.getField("_id").OID();

		c.done();

		return true;
	}

	bool Directory::CreateFiles ()
	{
		ScopedDbConnection c(FileSystem::Host());

		BSONObjBuilder u;
		BSONObj d;

		vector<BSONObj> fv;
		list<File>::iterator it;

		c->ensureIndex("JULEA.Directories", BSONObjBuilder().append("Path", 1).obj());
		c->ensureIndex("JULEA.Files", BSONObjBuilder().append("Path", 1).obj());

		if (!id.isSet())
		{
			Create();
		}

		for (it = this->newFiles.begin(); it != this->newFiles.end(); it++)
		{
			BSONObj f;
			BSONElement fid;

			File file = *it;

			string name = file.Name();
			string fpath = Common::BuildFilename(path, name);

			f = BSONObjBuilder()
				.genOID()
				.append("Path", fpath)
				.obj();

			fv.push_back(f);

			fid = f.getField("_id");

			u.append("$set", BSONObjBuilder()
				.append(name, fid.OID())
				.obj()
			);
		}

		c->insert("JULEA.Files", fv);

		c->update("JULEA.DirectoryEntries",
			BSONObjBuilder()
				.append("_id", eid)
				.obj(),
			u.obj()
		);

		newFiles.clear();

		c.done();
	}

}
