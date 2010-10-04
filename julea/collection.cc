#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "collection.h"

#include "store.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	BSONObj _Collection::Serialize ()
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

	void _Collection::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		m_name = o.getField("Name").String();
	}

	mongo::OID const& _Collection::ID () const
	{
		return m_id;
	}

	_Collection::_Collection (string const& name)
		: m_name(name), m_store(0)
	{
		m_id.clear();
	}

	_Collection::_Collection (_Store* store, string const& name)
		: m_name(name), m_store(store->Ref())
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

	_Collection::_Collection (_Store* store, BSONObj const& obj)
		: m_name(""), m_store(store->Ref())
	{
		m_id.clear();

		Deserialize(obj);
	}

	_Collection::~_Collection ()
	{
		m_store->Unref();
	}

	string const& _Collection::Name () const
	{
		return m_name;
	}

	_Item* _Collection::Get (string const& name)
	{
		string path(m_name + "/" + name);
		_Item* item = new _Item(this, path);
		newItems.push_back(item);

		return item;
	}

	/*
	bool _Collection::Create ()
	{
		if (m_id.isSet())
		{
			return true;
		}

		ScopedDbConnection c(_Store::Host());

		BSONObj d;
		BSONObj e;

		c->ensureIndex("JULEA.Directories", BSONObjBuilder().append("Path", 1).obj());

		c->insert("JULEA.Directories", Serialize());

		c.done();

		return true;
	}
	*/

	/*
	bool _Collection::CreateFiles ()
	{
		ScopedDbConnection c(_Store::Host());

		BSONObj d;

		vector<BSONObj> fv;
		vector<BSONObj> dev;
		list<_Item>::iterator it;

		if (!m_id.isSet())
		{
			Create();
		}

		for (it = newFiles.begin(); it != newFiles.end(); ++it)
		{
			BSONObj f;
			BSONElement fid;

			_Item file = *it;

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
