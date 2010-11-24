#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "collection.h"

#include "exception.h"
#include "store.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	_Collection::_Collection (string const& name)
		: m_initialized(false),
		  m_name(name),
		  m_semantics(0),
		  m_store(0),
		  m_itemsCollection("")
	{
		m_id.init();
	}

	_Collection::_Collection (_Store* store, BSONObj const& obj)
		: m_initialized(true),
		  m_name(""),
		  m_semantics(0),
		  m_store(store->Ref()),
		  m_itemsCollection("")
	{
		m_id.init();

		Deserialize(obj);
	}

	_Collection::~_Collection ()
	{
		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}

		if (m_store != 0)
		{
			m_store->Unref();
		}
	}

	void _Collection::IsInitialized (bool check) const
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Collection not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Collection already initialized.");
			}
		}
	}

	BSONObj _Collection::Serialize ()
	{
		BSONObj o;

		o = BSONObjBuilder()
			.append("_id", m_id)
			.append("Name", m_name)
			.append("User", m_owner.User())
			.append("Group", m_owner.Group())
			.obj();

		return o;
	}

	void _Collection::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		m_name = o.getField("Name").String();

		m_owner.m_user = o.getField("User").Int();
		m_owner.m_group = o.getField("Group").Int();
	}

	string const& _Collection::ItemsCollection ()
	{
		IsInitialized(true);

		if (m_itemsCollection.empty())
		{
			m_itemsCollection = m_store->Name() + ".Items";
		}

		return m_itemsCollection;
	}

	mongo::OID const& _Collection::ID () const
	{
		return m_id;
	}

	void _Collection::Associate (_Store* store)
	{
		IsInitialized(false);

		m_store = store->Ref();
		m_initialized = true;
	}

	string const& _Collection::Name () const
	{
		return m_name;
	}

	list<Item> _Collection::Get (list<string> names)
	{
		BSONObjBuilder ob;
		int n = 0;

		IsInitialized(true);

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
		ScopedDbConnection c(m_store->Connection()->GetServersString());

		auto_ptr<DBClientCursor> cur = c->query(ItemsCollection(), ob.obj(), n);

		while (cur->more())
		{
			items.push_back(Item(this, cur->next()));
		}

		c.done();

		return items;
	}

	void _Collection::Create (list<Item> items)
	{
		IsInitialized(true);

		if (items.size() == 0)
		{
			return;
		}

		BSONObj o;
		vector<BSONObj> obj;
		list<Item>::iterator it;

		o = BSONObjBuilder()
			.append("Collection", 1)
			.append("Name", 1)
			.obj();

		for (it = items.begin(); it != items.end(); ++it)
		{
			(*it)->Associate(this);
			obj.push_back((*it)->Serialize());
		}

		ScopedDbConnection c(m_store->Connection()->GetServersString());

		c->ensureIndex(ItemsCollection(), o, true);
		c->insert(ItemsCollection(), obj);

		if (GetSemantics()->GetPersistency() == Persistency::Strict)
		{
			BSONObj ores;

			c->runCommand("admin", BSONObjBuilder().append("fsync", 1).obj(), ores);
			//cout << ores << endl;
		}

		c.done();
	}

	_Semantics const* _Collection::GetSemantics ()
	{
		if (m_semantics != 0)
		{
			return m_semantics;
		}

		return m_store->GetSemantics();
	}

	void _Collection::SetSemantics (Semantics const& semantics)
	{
		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}

		m_semantics = semantics->Ref();
	}
}
