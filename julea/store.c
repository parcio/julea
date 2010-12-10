/*
#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "store.h"

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

	_Store::_Store (_Connection* connection, string const& name)
		: m_initialized(true),
		  m_name(name),
		  m_connection(connection),
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

	void _Store::Create (list<Collection> collections)
	{
		IsInitialized(true);

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
