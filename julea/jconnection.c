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

#include "jcommon.h"
#include "jmessage.h"
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
	GSocketConnection** sockets;

	gboolean connected;

	guint ref_count;
};

JConnection*
j_connection_new (void)
{
	JConnection* connection;
	guint i;

	connection = g_slice_new(JConnection);
	connection->connection = j_mongo_connection_new();
	connection->sockets = g_new(GSocketConnection*, j_common()->data_len);
	connection->connected = FALSE;
	connection->ref_count = 1;

	for (i = 0; i < j_common()->data_len; i++)
	{
		connection->sockets[i] = NULL;
	}

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
	guint i;

	g_return_if_fail(connection != NULL);

	connection->ref_count--;

	if (connection->ref_count == 0)
	{
		j_mongo_connection_unref(connection->connection);

		for (i = 0; i < j_common()->data_len; i++)
		{
			if (connection->sockets[i] != NULL)
			{
				g_object_unref(connection->sockets[i]);
			}
		}

		g_free(connection->sockets);

		g_slice_free(JConnection, connection);
	}
}

gboolean
j_connection_connect (JConnection* connection)
{
	GSocketClient* client;
	gboolean is_connected;
	guint i;

	g_return_val_if_fail(connection != NULL, FALSE);

	if (connection->connected)
	{
		return FALSE;
	}

	is_connected = j_mongo_connection_connect(connection->connection, j_common()->metadata[0]);

	client = g_socket_client_new();

	for (i = 0; i < j_common()->data_len; i++)
	{
		connection->sockets[i] = g_socket_client_connect_to_host(client, j_common()->data[i], 4711, NULL, NULL);

		is_connected = is_connected && (connection->sockets[i] != NULL);
	}

	g_object_unref(client);

	connection->connected = is_connected;

	return connection->connected;
}

gboolean
j_connection_disconnect (JConnection* connection)
{
	guint i;

	g_return_val_if_fail(connection != NULL, FALSE);

	if (!connection->connected)
	{
		return FALSE;
	}

	j_mongo_connection_disconnect(connection->connection);

	for (i = 0; i < j_common()->data_len; i++)
	{
		g_io_stream_close(G_IO_STREAM(connection->sockets[i]), NULL, NULL);
	}

	return TRUE;
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

gboolean
j_connection_send (JConnection* connection, guint i, JMessage* message, gconstpointer data, gsize length)
{
	GOutputStream* output;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(i < j_common()->data_len, FALSE);
	g_return_val_if_fail(message != NULL, FALSE);

	output = g_io_stream_get_output_stream(G_IO_STREAM(connection->sockets[i]));

	g_output_stream_write_all(output, j_message_data(message), j_message_length(message), NULL, NULL, NULL);

	if (data != NULL && length > 0)
	{
		g_output_stream_write_all(output, data, length, NULL, NULL, NULL);
	}

	g_output_stream_flush(output, NULL, NULL);

	return TRUE;
}

/**
 * @}
 **/
