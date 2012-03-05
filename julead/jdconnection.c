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

#include "jdconnection.h"

#include <jtrace.h>

/**
 * \defgroup JDConnection DConnection
 *
 * @{
 **/

/**
 * A connection.
 **/
struct JDConnection
{
	JTrace* trace;

	struct
	{
		guint64 files_created;
		guint64 files_deleted;

		guint64 sync_count;

		guint64 bytes_read;
		guint64 bytes_written;
	}
	statistics;
};

/**
 * Creates a new connection.
 *
 * \author Michael Kuhn
 *
 * \code
 * JDConnection* c;
 *
 * c = jd_connection_new();
 * \endcode
 *
 * \return A new connection. Should be freed with jd_connection_free().
 **/
JDConnection*
jd_connection_new (JTrace* trace)
{
	JDConnection* connection;

	g_return_val_if_fail(trace != NULL, NULL);

	connection = g_slice_new(JDConnection);
	connection->trace = trace;
	connection->statistics.files_created = 0;
	connection->statistics.files_deleted = 0;
	connection->statistics.sync_count = 0;
	connection->statistics.bytes_read = 0;
	connection->statistics.bytes_written = 0;

	return connection;
}

/**
 * Frees the memory allocated for the connection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param connection A connection.
 **/
void
jd_connection_free (JDConnection* connection)
{
	g_return_if_fail(connection != NULL);

	if (connection->trace != NULL)
	{
		j_trace_thread_leave(connection->trace);
	}

	g_slice_free(JDConnection, connection);
}

void
jd_connection_set_statistics (JDConnection* connection, JDConnectionStatisticsType type, guint64 value)
{
	g_return_if_fail(connection != NULL);

	switch (type)
	{
		case JD_CONNECTION_STATISTICS_FILE_CREATED:
			connection->statistics.files_created += value;
			break;
		case JD_CONNECTION_STATISTICS_FILE_DELETED:
			connection->statistics.files_deleted += value;
			break;
		case JD_CONNECTION_STATISTICS_SYNC:
			connection->statistics.sync_count += value;
			break;
		case JD_CONNECTION_STATISTICS_BYTES_READ:
			connection->statistics.bytes_read += value;
			break;
		case JD_CONNECTION_STATISTICS_BYTES_WRITTEN:
			connection->statistics.bytes_written += value;
			break;
		default:
			g_warn_if_reached();
			break;
	}
}

/**
 * @}
 **/
