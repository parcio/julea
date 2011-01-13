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
#include <gio/gio.h>

#include "jmongo-connection.h"

/**
 * \defgroup JMongoConnection MongoDB Connection
 *
 * @{
 **/

/**
 * A MongoDB connection.
 **/
struct JMongoConnection
{
	GSocketConnection* connection;
	GInputStream* input;
	GOutputStream* output;

	gboolean connected;

	guint ref_count;
};

JMongoConnection*
j_mongo_connection_new (void)
{
	JMongoConnection* connection;

	connection = g_slice_new(JMongoConnection);
	connection->connection = NULL;
	connection->input = NULL;
	connection->output = NULL;
	connection->connected = FALSE;
	connection->ref_count = 1;

	return connection;
}

static void
j_mongo_connection_close (JMongoConnection* connection)
{
	if (connection->connection != NULL)
	{
		g_object_unref(connection->connection);
		connection->connection = NULL;
		connection->input = NULL;
		connection->output = NULL;
	}
}

JMongoConnection*
j_mongo_connection_ref (JMongoConnection* connection)
{
	connection->ref_count++;

	return connection;
}

void
j_mongo_connection_unref (JMongoConnection* connection)
{
	connection->ref_count--;

	if (connection->ref_count == 0)
	{
		j_mongo_connection_close(connection);

		g_slice_free(JMongoConnection, connection);
	}
}

gboolean
j_mongo_connection_connect (JMongoConnection* connection, const gchar* host)
{
	GSocketClient* client;

	if (connection->connected)
	{
		return FALSE;
	}

	client = g_socket_client_new();

	if ((connection->connection = g_socket_client_connect_to_host(client, host, 27017, NULL, NULL)) == NULL)
	{
		goto error;
	}

	g_object_unref(client);

	connection->input = g_io_stream_get_input_stream(G_IO_STREAM(connection->connection));
	connection->output = g_io_stream_get_output_stream(G_IO_STREAM(connection->connection));
	connection->connected = TRUE;

	return TRUE;

error:
	g_object_unref(client);

	return FALSE;
}

void
j_mongo_connection_disconnect (JMongoConnection* connection)
{
	if (!connection->connected)
	{
		return;
	}

	j_mongo_connection_close(connection);
	connection->connected = FALSE;
}

void
j_mongo_connection_send (JMongoConnection* connection, JMongoMessage* message)
{
	g_output_stream_write_all(connection->output, message, j_mongo_message_length(message), NULL, NULL, NULL);
	g_output_stream_flush(connection->output, NULL, NULL);
}

/**
 * @}
 **/
