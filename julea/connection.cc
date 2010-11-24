#include <mongo/client/connpool.h>

#include "connection.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	_Connection::_Connection ()
		: m_servers(), m_servers_string("")
	{
	}

	_Connection::~_Connection ()
	{
	}

	void _Connection::Connect ()
	{
		try
		{
			ScopedDbConnection c(m_servers_string);

			c.done();
		}
		catch (...)
		{
			throw Exception(JULEA_FILELINE ": Can not connect to “" + m_servers_string + "”.");
		}
	}

	list<string> _Connection::GetServers ()
	{
		return m_servers;
	}

	string _Connection::GetServersString ()
	{
		return m_servers_string;
	}

	_Connection* _Connection::AddServer (string const& server)
	{
		m_servers.push_back(server);

		if (!m_servers_string.empty())
		{
			m_servers_string += ",";
		}

		m_servers_string += server;

		return this;
	}

	Store _Connection::Get (string const& name)
	{
		return Store(this, name);
	}
}
