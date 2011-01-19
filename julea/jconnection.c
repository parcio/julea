/*
 * Copyright (c) 2010-2011 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include "jconnection.h"
#include "jconnection-internal.h"

#include "jmongo-connection.h"

/**
 * \defgroup JConnection Connection
 *
 * Data structures and functions for managing connections to servers.
 *
 * @{
 **/

/**
 * A JConnection.
 **/
struct JConnection
{
	JMongoConnection* connection;
	GSocketConnection* socket;

	gboolean connected;

	guint ref_count;
};

JConnection*
j_connection_new (void)
{
	JConnection* connection;

	connection = g_slice_new(JConnection);
	connection->connection = j_mongo_connection_new();
	connection->socket = NULL;
	connection->connected = FALSE;
	connection->ref_count = 1;

	return connection;
}

/**
 * Increases a JConnection's reference count.
 **/
JConnection*
j_connection_ref (JConnection* connection)
{
	g_return_val_if_fail(connection != NULL, NULL);

	connection->ref_count++;

	return connection;
}

void
j_connection_unref (JConnection* connection)
{
	g_return_if_fail(connection != NULL);

	connection->ref_count--;

	if (connection->ref_count == 0)
	{
		j_mongo_connection_unref(connection->connection);

		if (connection->socket != NULL)
		{
			g_object_unref(connection->socket);
		}

		g_slice_free(JConnection, connection);
	}
}

gboolean
j_connection_connect (JConnection* connection, const gchar* server)
{
	gboolean is_connected;
	GSocketClient* client;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(server != NULL, FALSE);

	is_connected = j_mongo_connection_connect(connection->connection, server);

	client = g_socket_client_new();
	/* FIXME localhost for testing */
	connection->socket = g_socket_client_connect_to_host(client, "localhost", 4711, NULL, NULL);
	g_object_unref(client);

	connection->connected = is_connected && (connection->socket != NULL);

	return connection->connected;
}

JStore*
j_connection_get (JConnection* connection, const gchar* name)
{
	g_return_val_if_fail(connection != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	return j_store_new(connection, name);
}

/* Internal */

JMongoConnection*
j_connection_connection (JConnection* connection)
{
	g_return_val_if_fail(connection != NULL, NULL);

	return connection->connection;
}

/**
 * @}
 **/
