#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "store.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
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
}
