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

	void _Collection::Associate (_Store* store)
	{
		m_store = store;
	}

	_Collection::_Collection (string const& name)
		: m_name(name), m_store(0)
	{
		m_id.clear();
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

	list<Item> _Collection::Get (list<string> names)
	{
		BSONObjBuilder ob;
		int n = 0;

		ob.append("Collection", m_id);

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

		list<Item> items;
		ScopedDbConnection c(m_store->Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Items", ob.obj(), n);

		while (cur->more())
		{
			items.push_back(Item(this, cur->next()));
		}

		c.done();

		return items;
	}

	void _Collection::Create (list<Item> items)
	{
		if (items.size() == 0)
		{
			return;
		}

		vector<BSONObj> obj;
		list<Item>::iterator it;

		for (it = items.begin(); it != items.end(); ++it)
		{
			(*it)->Associate(this);
			obj.push_back((*it)->Serialize());
		}

		ScopedDbConnection c(m_store->Host());

		c->insert("JULEA.Items", obj);
		c.done();
	}
}
