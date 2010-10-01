#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "store.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	Store::Store (string const& host)
		: m_host(host)
	{
		if (m_host.empty())
		{
			throw Exception("Store not initialized.");
		}

		ScopedDbConnection c(m_host);

		c.done();
	}

	Store::~Store ()
	{
	}

	string const& Store::Host () const
	{
		return m_host;
	}

	Collection* Store::Get (string const& name)
	{
		Collection* collection = new Collection(this, name);

		return collection;
	}

	Store* Store::GetAll ()
	{
		ScopedDbConnection c(Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Collections", BSONObjBuilder().obj());

		while (cur->more())
		{
			Collection* collection = new Collection(this, cur->next());
			m_collections[collection->Name()] = collection;
		}

		c.done();

		return this;
	}
}
