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

		ScopedDbConnection c(m_host);

		c.done();
	}

	_Store::~_Store ()
	{
	}

	string const& _Store::Host () const
	{
		return m_host;
	}

	list<Collection> _Store::Get (list<string> names)
	{
		BSONObjBuilder ob;
		int n = 0;

		if (names.size() == 1)
		{
			ob.append("Name", names.front());
			n = 1;
		}
		else
		{
			BSONObjBuilder obv;
			list<string>::iterator it;

			for (it = names.begin(); it != names.end(); ++it)
			{
				obv.append("Name", *it);
			}

			ob.append("$or", obv.obj());
		}

		list<Collection> collections;
		ScopedDbConnection c(Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", ob.obj(), n);

		while (cur->more())
		{
			collections.push_back(Collection(this, cur->next()));
		}

		c.done();

		return collections;
	}

	void _Store::Create (list<Collection> collections)
	{
		if (collections.size() == 0)
		{
			return;
		}

		BSONObj o;
		vector<BSONObj> obj;
		list<Collection>::iterator it;

		o = BSONObjBuilder()
			.append("Name", 1)
			.obj();

		for (it = collections.begin(); it != collections.end(); ++it)
		{
			(*it)->Associate(this);
			obj.push_back((*it)->Serialize());
		}

		ScopedDbConnection c(Host());

		c->ensureIndex("JULEA.Collections", o, true);
		c->insert("JULEA.Collections", obj);
		c.done();
	}
}
