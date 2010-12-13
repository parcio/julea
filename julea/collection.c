#include <glib.h>

#include "collection.h"

#include "semantics.h"

struct JCollection
{
};

JCollection*
j_collection_new (const gchar* name)
{
	/*
	: m_initialized(false),
	m_name(name),
	m_semantics(0),
	m_store(0),
	m_itemsCollection("")

	m_id.init();
	*/

	return g_new(JCollection, 1);
}

void
j_collection_set_semantics (JCollection* collection, JSemantics* semantics)
{
	/*
	if (m_semantics != 0)
	{
		m_semantics->Unref();
	}

	m_semantics = semantics->Ref();
	*/
}


/*
#include "exception.h"
#include "store.h"

namespace JULEA
{
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
		ScopedDbConnection* c = m_store->Connection()->GetMongoDB();
		DBClientBase* b = c->get();

		auto_ptr<DBClientCursor> cur = b->query(ItemsCollection(), ob.obj(), n);

		while (cur->more())
		{
			items.push_back(Item(this, cur->next()));
		}

		c->done();
		delete c;

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

		ScopedDbConnection* c = m_store->Connection()->GetMongoDB();
		DBClientBase* b = c->get();

		b->ensureIndex(ItemsCollection(), o, true);
		b->insert(ItemsCollection(), obj);

		if (GetSemantics()->GetPersistency() == Persistency::Strict)
		{
			BSONObj ores;

			b->runCommand("admin", BSONObjBuilder().append("fsync", 1).obj(), ores);
			//cout << ores << endl;
		}

		c->done();
		delete c;
	}

	_Semantics const* _Collection::GetSemantics ()
	{
		if (m_semantics != 0)
		{
			return m_semantics;
		}

		return m_store->GetSemantics();
	}

	Collection::Iterator::Iterator (Collection const& collection)
		: m_connection(collection->m_store->Connection()->GetMongoDB()),
		  m_collection(collection->Ref())
	{
		DBClientBase* b = m_connection->get();
		BSONObjBuilder ob;

		ob.append("Collection", m_collection->ID());
		m_cursor = b->query(m_collection->ItemsCollection(), ob.obj());
	}

	Collection::Iterator::~Iterator ()
	{
		m_connection->done();
		delete m_connection;

		m_collection->Unref();
	}

	bool Collection::Iterator::More ()
	{
		return m_cursor->more();
	}

	Item Collection::Iterator::Next ()
	{
		return Item(m_collection, m_cursor->next());
	}
}
*/
