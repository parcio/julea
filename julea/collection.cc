#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "collection.h"

#include "store.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	BSONObj Collection::Serialize ()
	{
		BSONObj o;

		if (!m_id.isSet())
		{
			m_id.init();
		}

		o = BSONObjBuilder()
			.append("_id", m_id)
			.append("Name", m_name)
			.obj();

		return o;
	}

	void Collection::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		m_name = o.getField("Name").String();
	}

	mongo::OID const& Collection::ID () const
	{
		return m_id;
	}

	Collection* Collection::Ref ()
	{
		m_refCount++;

		return this;
	}

	bool Collection::Unref ()
	{
		m_refCount--;

		if (m_refCount == 0)
		{
			delete this;

			return true;
		}

		return false;
	}

	Collection::Collection (Store* store, string const& name)
		: m_name(name), m_refCount(1), m_store(store->Ref())
	{
		m_id.clear();

		ScopedDbConnection c(m_store->Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", BSONObjBuilder().append("Name", m_name).obj(), 1);

		if (cur->more())
		{
			Deserialize(cur->next());
		}

		c.done();
	}

	Collection::~Collection ()
	{
		m_store->Unref();
	}

	string const& Collection::Name () const
	{
		return m_name;
	}

	Item* Collection::Get (string const& name)
	{
		string path(m_name + "/" + name);
		Item* item = new Item(this, path);
		newItems.push_back(item);

		return item;
	}

	/*
	bool Collection::Create ()
	{
		if (m_id.isSet())
		{
			return true;
		}

		ScopedDbConnection c(Store::Host());

		BSONObj d;
		BSONObj e;

		c->ensureIndex("JULEA.Directories", BSONObjBuilder().append("Path", 1).obj());

		c->insert("JULEA.Directories", Serialize());

		c.done();

		return true;
	}
	*/

	/*
	bool Collection::CreateFiles ()
	{
		ScopedDbConnection c(Store::Host());

		BSONObj d;

		vector<BSONObj> fv;
		vector<BSONObj> dev;
		list<Item>::iterator it;

		if (!m_id.isSet())
		{
			Create();
		}

		for (it = newFiles.begin(); it != newFiles.end(); ++it)
		{
			BSONObj f;
			BSONElement fid;

			Item file = *it;

			fv.push_back(file.Serialize());

			fid = f.getField("_id");

			f = BSONObjBuilder()
				.genOID()
				.append("Collection", m_id)
			//	.append("Name", name)
				.append("ID", fid.OID())
				.obj();

			dev.push_back(f);
		}

		c->ensureIndex("JULEA.Directories", BSONObjBuilder().append("Path", 1).obj());
		c->ensureIndex("JULEA.Files", BSONObjBuilder().append("Path", 1).obj());

		c->insert("JULEA.Files", fv);
		c->insert("JULEA.DirectoryEntries", dev);

		newFiles.clear();

		c.done();

		return true;
	}
	*/
}
