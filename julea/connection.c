#include <glib.h>

#include "connection.h"

struct JConnection
{
};

JConnection*
j_connection_new (void)
{
	/*
	: m_servers(), m_servers_string("")
	*/

	return g_new(JConnection, 1);
}

void
j_connection_connect (JConnection* connection)
{
	/*
	try
	{
		ScopedDbConnection c(m_servers_string);

		c.done();
	}
	catch (...)
	{
		throw Exception(JULEA_FILELINE ": Can not connect to “" + m_servers_string + "”.");
	}
	*/
}

void
j_connection_add_server (JConnection* connection, const gchar* server)
{
	/*
	m_servers.push_back(server);

	if (!m_servers_string.empty())
	{
		m_servers_string += ",";
	}

	m_servers_string += server;
	*/
}

JStore*
j_connection_get (JConnection* connection, const gchar* name)
{
	return j_store_new(connection, name);
}

/*
#include "exception.h"

namespace JULEA
{
	_Connection::~_Connection ()
	{
	}

	ScopedDbConnection* _Connection::GetMongoDB ()
	{
		return new ScopedDbConnection(m_servers_string);
	}

	list<string> _Connection::GetServers ()
	{
		return m_servers;
	}
}
*/
