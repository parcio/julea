#include <glib.h>

#include <bson.h>

#include "store.h"

#include "bson.h"
#include "collection.h"
#include "connection.h"

struct JStore
{
	gchar* name;

	JConnection* connection;
	JSemantics* semantics;

	guint ref_count;
};

JStore*
j_store_new (JConnection* connection, const gchar* name)
{
	JStore* store;
	/*
	: m_initialized(true),
	m_collectionsCollection("")
	*/

	store = g_new(JStore, 1);
	store->name = g_strdup(name);
	store->connection = j_connection_ref(connection);
	store->semantics = j_semantics_new();
	store->ref_count = 1;

	return store;
}

void
j_store_unref (JStore* store)
{
	store->ref_count--;

	if (store->ref_count == 0)
	{
		j_connection_unref(store->connection);
		j_semantics_unref(store->semantics);

		g_free(store->name);
		g_free(store);
	}
}

void
j_store_create (JStore* store, GList* collections)
{
	/*
	IsInitialized(true);

	if (collections.size() == 0)
	{
		return;
	}
	*/

	JBSON* jbson;
	bson* index;

	jbson = j_bson_new();
	j_bson_append_int(jbson, "Name", 1);
	index = j_bson_get(jbson);
	j_bson_free(jbson);

	/*
	vector<BSONObj> obj;
	list<Collection>::iterator it;

	for (it = collections.begin(); it != collections.end(); ++it)
	{
		(*it)->Associate(this);
		obj.push_back((*it)->Serialize());
	}

	ScopedDbConnection* c = m_connection->GetMongoDB();
	DBClientBase* b = c->get();

	b->ensureIndex(CollectionsCollection(), o, true);
	b->insert(CollectionsCollection(), obj);
	//		cout << "error: " << c->getLastErrorDetailed() << endl;

	if (GetSemantics()->GetPersistency() == Persistency::Strict)
	{
		BSONObj ores;

		b->runCommand("admin", BSONObjBuilder().append("fsync", 1).obj(), ores);
		//cout << ores << endl;
	}

	c->done();
	delete c;
	*/
}

/*
#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	_Store::_Store (string const& name)
		: m_initialized(false),
		  m_name(name),
		  m_connection(0),
		  m_semantics(0),
		  m_collectionsCollection("")
	{
		m_semantics = new _Semantics();
	}

	_Store::~_Store ()
	{
		m_semantics->Unref();
	}

	void _Store::IsInitialized (bool check) const
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Store not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Store already initialized.");
			}
		}
	}

	string const& _Store::CollectionsCollection ()
	{
		IsInitialized(true);

		if (m_collectionsCollection.empty())
		{
			m_collectionsCollection = m_name + ".Collections";
		}

		return m_collectionsCollection;
	}

	_Connection* _Store::Connection ()
	{
		return m_connection;
	}

	string const& _Store::Name ()
	{
		return m_name;
	}

	list<Collection> _Store::Get (list<string> names)
	{
		BSONObjBuilder ob;
		int n = 0;

		IsInitialized(true);

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
		ScopedDbConnection* c = m_connection->GetMongoDB();
		DBClientBase* b = c->get();

		auto_ptr<DBClientCursor> cur = b->query(CollectionsCollection(), ob.obj(), n);

		while (cur->more())
		{
			collections.push_back(Collection(this, cur->next()));
		}

		c->done();
		delete c;

		return collections;
	}

	_Semantics const* _Store::GetSemantics ()
	{
		return m_semantics;
	}

	void _Store::SetSemantics (Semantics const& semantics)
	{
		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}

		m_semantics = semantics->Ref();
	}
}
*/
