#include <glib.h>

#include <mongo.h>

#include "connection.h"

struct JConnection
{
	mongo_connection connection;

	guint ref_count;
};

JConnection*
j_connection_new (void)
{
	JConnection* connection;

	connection = g_new(JConnection, 1);
	connection->ref_count = 1;

	return connection;
}

JConnection*
j_connection_ref (JConnection* connection)
{
	connection->ref_count++;

	return connection;
}

void
j_connection_unref (JConnection* connection)
{
	connection->ref_count--;

	if (connection->ref_count == 0)
	{
		mongo_destroy(&(connection->connection));

		g_free(connection);
	}
}

gboolean
j_connection_connect (JConnection* connection, const gchar* server)
{
	mongo_connection_options opts;
	mongo_conn_return status;

	g_strlcpy(opts.host, server, 255);
	opts.port = 27017;

	status = mongo_connect(&(connection->connection), &opts);

	return (status == mongo_conn_success);
}

JStore*
j_connection_get (JConnection* connection, const gchar* name)
{
	return j_store_new(connection, name);
}

/*
	ScopedDbConnection* _Connection::GetMongoDB ()
	{
		return new ScopedDbConnection(m_servers_string);
	}
*/
