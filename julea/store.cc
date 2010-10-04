#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "store.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	_Store::_Store (string const& host)
		: m_host(host)
	{
		if (m_host.empty())
		{
			throw Exception("Store not initialized.");
		}

		//ScopedDbConnection c(m_host);

		//c.done();
	}

	_Store::~_Store ()
	{
	}

	string const& _Store::Host () const
	{
		return m_host;
	}

	map<string, _Collection*> _Store::Get (list<string> names)
	{
		map<string, _Collection*> collections;

		ScopedDbConnection c(Host());

		if (names.size() == 0)
		{
			auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", BSONObjBuilder().obj());

			while (cur->more())
			{
				_Collection* collection = new _Collection(this, cur->next());
				collections[collection->Name()] = collection;
			}
		}
		else if (names.size() == 1)
		{
			auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", BSONObjBuilder().append("Name", names.front()).obj(), 1);

			if (cur->more())
			{
				_Collection* collection = new _Collection(this, cur->next());
				collections[collection->Name()] = collection;
			}
		}
		else
		{
			BSONObjBuilder ob;
			list<string>::iterator it;

			for (it = names.begin(); it != names.end(); ++it)
			{
				ob.append("Name", *it);
			}

			auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", BSONObjBuilder().append("$or", ob.obj()).obj());

			while (cur->more())
			{
				_Collection* collection = new _Collection(this, cur->next());
				collections[collection->Name()] = collection;
			}
		}

		c.done();

		return collections;
	}

	void _Store::Create (list<Collection> collections)
	{
		vector<BSONObj> obj;

		if (collections.size() == 0)
		{
			return;
		}

		//ScopedDbConnection c(Host());

		list<Collection>::iterator it;

		for (it = collections.begin(); it != collections.end(); ++it)
		{
			(*it)->m_store = this;
			obj.push_back((*it)->Serialize());
		}

//		c->insert("JULEA.Collections", obj);

//		c.done();
	}
}
