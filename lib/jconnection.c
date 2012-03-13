/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#define J_USE_NODELAY 0

#if J_USE_NODELAY
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include <jconfiguration.h>
#include <jconnection.h>
#include <jconnection-internal.h>

#include <jcommon-internal.h>
#include <jmessage.h>
#include <jtrace-internal.h>

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
	gint ref_count;
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

	j_trace_enter(j_trace(), G_STRFUNC);

	connection = g_slice_new(JConnection);
	connection->configuration = j_configuration_ref(configuration);
	mongo_init(&(connection->connection));
	connection->sockets_len = j_configuration_get_data_server_count(connection->configuration);
	connection->sockets = g_new(GSocketConnection*, connection->sockets_len);
	connection->connected = FALSE;
	connection->ref_count = 1;

	for (i = 0; i < connection->sockets_len; i++)
	{
		connection->sockets[i] = NULL;
	}

	j_trace_leave(j_trace(), G_STRFUNC);

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

	j_trace_enter(j_trace(), G_STRFUNC);

	g_atomic_int_inc(&(connection->ref_count));

	j_trace_leave(j_trace(), G_STRFUNC);

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

	j_trace_enter(j_trace(), G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(connection->ref_count)))
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

	j_trace_leave(j_trace(), G_STRFUNC);
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
	gboolean ret;
	GSocketClient* client;
	guint i;

	g_return_val_if_fail(connection != NULL, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (connection->connected)
	{
		ret = FALSE;
		goto end;
	}

	ret = (mongo_connect(&(connection->connection), j_configuration_get_metadata_server(connection->configuration, 0), 27017) == MONGO_OK);

	if (!ret)
	{
		g_critical("%s: Can not connect to MongoDB.", G_STRLOC);
	}

	client = g_socket_client_new();

	for (i = 0; i < connection->sockets_len; i++)
	{
#if J_USE_NODELAY
		GSocket* socket_;
		gint fd;
		gint flag = 1;
#endif

		connection->sockets[i] = g_socket_client_connect_to_host(client, j_configuration_get_data_server(connection->configuration, i), 4711, NULL, NULL);

		if (connection->sockets[i] == NULL)
		{
			g_critical("%s: Can not connect to %s.", G_STRLOC, j_configuration_get_data_server(connection->configuration, i));
		}

#if J_USE_NODELAY
		socket_ = g_socket_connection_get_socket(connection->sockets[i]);
		fd = g_socket_get_fd(socket_);

		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(gint));
#endif

		ret = ret && (connection->sockets[i] != NULL);
	}

	g_object_unref(client);

	connection->connected = ret;

end:
	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
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
	gboolean ret = TRUE;
	guint i;

	g_return_val_if_fail(connection != NULL, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (!connection->connected)
	{
		ret = FALSE;
		goto end;
	}

	mongo_disconnect(&(connection->connection));

	for (i = 0; i < connection->sockets_len; i++)
	{
		g_io_stream_close(G_IO_STREAM(connection->sockets[i]), NULL, NULL);
	}

end:
	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
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

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return &(connection->connection);
}

/**
 * Sends a message via the julead connections.
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
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_connection_send (JConnection* connection, guint i, JMessage* message)
{
	GOutputStream* output;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(i < connection->sockets_len, FALSE);
	g_return_val_if_fail(message != NULL, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	output = g_io_stream_get_output_stream(G_IO_STREAM(connection->sockets[i]));
	j_message_write(message, output);

	j_trace_leave(j_trace(), G_STRFUNC);

	return TRUE;
}

/**
 * Receives a message via the julead connections.
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
 * \param reply      A reply message.
 * \param message    A message.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_connection_receive (JConnection* connection, guint i, JMessage* reply, JMessage* message)
{
	gboolean ret;
	GInputStream* input;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(i < connection->sockets_len, FALSE);
	g_return_val_if_fail(message != NULL, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	input = g_io_stream_get_input_stream(G_IO_STREAM(connection->sockets[i]));
	ret = j_message_read_reply(reply, message, input);

	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
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
 * \param data       A data buffer.
 * \param length     #data's length.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_connection_receive_data (JConnection* connection, guint i, gpointer data, gsize length)
{
	gboolean ret;
	GInputStream* input;

	g_return_val_if_fail(connection != NULL, FALSE);
	g_return_val_if_fail(i < connection->sockets_len, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(length > 0, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	input = g_io_stream_get_input_stream(G_IO_STREAM(connection->sockets[i]));
	ret = g_input_stream_read_all(input, data, length, NULL, NULL, NULL);

	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
}

/**
 * @}
 **/
