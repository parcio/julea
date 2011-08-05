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

#include <mongo.h>

#include "jconfiguration.h"
#include "jconnection.h"
#include "jconnection-internal.h"

#include "jcommon.h"
#include "jmessage.h"
#include "jmessage-reply.h"

/**
 * \defgroup JConnection Connection
 *
 * Data structures and functions for managing connections to servers.
 *
 * @{
 **/

/**
 * A connection.
 **/
struct JConnection
{
	/**
	 * The configuration.
	 **/
	JConfiguration* configuration;

	/**
	 * The MongoDB connection.
	 **/
	mongo connection;

	/**
	 * The julead connections.
	 **/
	GSocketConnection** sockets;

	/**
	 * The number of julead connections.
	 **/
	guint sockets_len;

	/**
	 * The connection status.
	 **/
	gboolean connected;

	/**
	 * The reference count.
	 **/
	guint ref_count;
};

GQuark
j_connection_error_quark (void)
{
	return g_quark_from_static_string("j-connection-error-quark");
}

/**
 * Creates a new connection.
 *
 * \author Michael Kuhn
 *
 * \code
 * JConnection* c;
 *
 * c = j_connection_new();
 * \endcode
 *
 * \param configuration A configuration.
 *
 * \return A new connection. Should be freed with j_connection_unref().
 **/
JConnection*
j_connection_new (JConfiguration* configuration)
{
	JConnection* connection;
	guint i;

	connection = g_slice_new(JConnection);
	connection->configuration = j_configuration_ref(configuration);
	mongo_init(&(connection->connection));
	connection->sockets_len = j_configuration_get_data_server_number(connection->configuration);
	connection->sockets = g_new(GSocketConnection*, connection->sockets_len);
	connection->connected = FALSE;
	connection->ref_count = 1;

	for (i = 0; i < connection->sockets_len; i++)
	{
		connection->sockets[i] = NULL;
	}

	return connection;
}

/**
 * Increases a connection's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JConnection* c;
 *
 * j_connection_ref(c);
 * \endcode
 *
 * \param connection A connection.
 *
 * \return #connection.
 **/
JConnection*
j_connection_ref (JConnection* connection)
{
	g_return_val_if_fail(connection != NULL, NULL);

	connection->ref_count++;

	return connection;
}

/**
 * Decreases a connection's reference count.
 * When the reference count reaches zero, frees the memory allocated for the connection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 **/
void
j_connection_unref (JConnection* connection)
{
	guint i;

	g_return_if_fail(connection != NULL);

	connection->ref_count--;

	if (connection->ref_count == 0)
	{
		mongo_destroy(&(connection->connection));

		for (i = 0; i < connection->sockets_len; i++)
		{
			if (connection->sockets[i] != NULL)
			{
				g_object_unref(connection->sockets[i]);
			}
		}

		j_configuration_unref(connection->configuration);

		g_free(connection->sockets);

		g_slice_free(JConnection, connection);
	}
}

/**
 * Connects a connection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
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

	is_connected = (mongo_connect(&(connection->connection), j_configuration_get_metadata_server(connection->configuration, 0), 27017) == MONGO_OK);

	client = g_socket_client_new();

	for (i = 0; i < connection->sockets_len; i++)
	{
		connection->sockets[i] = g_socket_client_connect_to_host(client, j_configuration_get_data_server(connection->configuration, i), 4711, NULL, NULL);

		is_connected = is_connected && (connection->sockets[i] != NULL);
	}

	g_object_unref(client);

	connection->connected = is_connected;

	return connection->connected;
}

/**
 * Disconnects a connection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_connection_disconnect (JConnection* connection)
{
	guint i;

	g_return_val_if_fail(connection != NULL, FALSE);

	if (!connection->connected)
	{
		return FALSE;
	}

	mongo_disconnect(&(connection->connection));

	for (i = 0; i < connection->sockets_len; i++)
	{
		g_io_stream_close(G_IO_STREAM(connection->sockets[i]), NULL, NULL);
	}

	return TRUE;
}

/* Internal */

/**
 * Returns a MongoDB connection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 *
 * \return A MongoDB connection.
 **/
mongo*
j_connection_get_connection (JConnection* connection)
{
	g_return_val_if_fail(connection != NULL, NULL);

	return &(connection->connection);
}

/**
 * Sends data via the julead connections.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 * \param i          A connection index.
 * \param message    A message.
 * \param data       An optional data buffer.
 * \param length     Number of bytes to send.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_connection_send (JConnection* connection, guint i, JMessage* message, gconstpointer data, gsize length)
{
	GOutputStream* output;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(i < connection->sockets_len, FALSE);
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
 * Receives data via the julead connections.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 * \param i          A connection index.
 * \param message    A message.
 * \param data       A data buffer.
 * \param length     #data's length.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_connection_receive (JConnection* connection, guint i, JMessage* message, gpointer data, gsize length)
{
	JMessageReply* message_reply;
	GInputStream* input;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(i < connection->sockets_len, FALSE);
	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	input = g_io_stream_get_input_stream(G_IO_STREAM(connection->sockets[i]));
	message_reply = j_message_reply_new(message, 0);

	j_message_reply_read(message_reply, input);
	g_input_stream_read_all(input, data, j_message_reply_length(message_reply), NULL, NULL, NULL);

	j_message_reply_free(message_reply);

	return TRUE;
}

/**
 * @}
 **/
