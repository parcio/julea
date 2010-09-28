#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "store.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	Store* Store::Ref ()
	{
		m_refCount++;

		return this;
	}

	bool Store::Unref ()
	{
		m_refCount--;

		if (m_refCount == 0)
		{
			delete this;

			return true;
		}

		return false;
	}

	Store::Store (string const& host)
		: m_host(host), m_refCount(1)
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
}
