#include <iostream>

#include <boost/filesystem.hpp>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "julea.h"

using namespace boost::filesystem;
using namespace std;
using namespace mongo;

namespace JULEA
{
	Exception::Exception (string const& description) throw()
		: description(description)
	{
	}

	Exception::~Exception () throw()
	{
	}

	const char* Exception::what () const throw()
	{
		return description.c_str();
	}

	string Store::m_host;

	void Store::Initialize (string const& host)
	{
		m_host = host;

		if (m_host.empty())
		{
			throw Exception("Store not initialized.");
		}

		ScopedDbConnection c(m_host);

		c.done();
	}

	string const& Store::Host ()
	{
		return m_host;
	}

	BSONObj Item::Serialize ()
	{
		BSONObj o;

		if (!m_id.isSet())
		{
			m_id.init();
		}

		o = BSONObjBuilder()
			.append("_id", m_id)
			.append("CollectionID", m_collectionID)
			.append("Name", m_name)
			.obj();

		return o;
	}

	void Item::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		m_collectionID = o.getField("CollectionID").OID();
		m_name = o.getField("Name").String();
	}

	Item::Item (Collection const& coll, string const& name)
		: m_collectionID(coll.ID()), m_name(name)
	{
		m_id.clear();

		ScopedDbConnection c(Store::Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Items", BSONObjBuilder().append("CollectionID", m_collectionID).append("Name", m_name).obj(), 1);

		if (cur->more())
		{
			Deserialize(cur->next());
		}

		c.done();
	}

	Item::~Item ()
	{
	}

	string const& Item::Name () const
	{
		return m_name;
	}

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

	Collection::Collection (string const& name)
		: m_name(name)
	{
		m_id.clear();

		ScopedDbConnection c(Store::Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", BSONObjBuilder().append("Name", m_name).obj(), 1);

		if (cur->more())
		{
			Deserialize(cur->next());
		}

		c.done();
	}

	Collection::~Collection ()
	{
	}

	Item Collection::Add (string const& name)
	{
		path path(m_name + "/" + name);
		Item item(*this, path.string());
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
