/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <jstatistics-internal.h>

#include <jtrace-internal.h>

/**
 * \defgroup JStatistics Statistics
 *
 * @{
 **/

/**
 * A statistics.
 **/
struct JStatistics
{
	/**
	 * Whether to trace.
	 **/
	gboolean trace;

	/**
	 * The number of created files.
	 **/
	guint64 files_created;

	/**
	 * The number of deleted files.
	 **/
	guint64 files_deleted;

	/**
	 * The number of sync operations.
	 **/
	guint64 sync_count;

	/**
	 * The number of read bytes.
	 **/
	guint64 bytes_read;

	/**
	 * The number of written bytes.
	 **/
	guint64 bytes_written;

	/**
	 * The number of received bytes.
	 **/
	guint64 bytes_received;

	/**
	 * The number of sent bytes.
	 **/
	guint64 bytes_sent;
};

static
gchar const*
j_statistics_get_type_name (JStatisticsType type)
{
	switch (type)
	{
		case J_STATISTICS_FILES_CREATED:
			return "files_created";
		case J_STATISTICS_FILES_DELETED:
			return "files_deleted";
		case J_STATISTICS_SYNC:
			return "sync";
		case J_STATISTICS_BYTES_READ:
			return "bytes_read";
		case J_STATISTICS_BYTES_WRITTEN:
			return "bytes_written";
		case J_STATISTICS_BYTES_RECEIVED:
			return "bytes_received";
		case J_STATISTICS_BYTES_SENT:
			return "bytes_sent";
		default:
			g_warn_if_reached();
			return NULL;
	}
}

/**
 * Creates a new statistics.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * JStatistics* s;
 *
 * s = j_statistics_new(FALSE);
 * \endcode
 *
 * \return A new statistics. Should be freed with j_statistics_free().
 **/
JStatistics*
j_statistics_new (gboolean trace)
{
	JStatistics* statistics;

	j_trace_enter(G_STRFUNC);

	statistics = g_slice_new(JStatistics);
	statistics->trace = trace;
	statistics->files_created = 0;
	statistics->files_deleted = 0;
	statistics->sync_count = 0;
	statistics->bytes_read = 0;
	statistics->bytes_written = 0;
	statistics->bytes_received = 0;
	statistics->bytes_sent = 0;

	j_trace_leave(G_STRFUNC);

	return statistics;
}

/**
 * Frees the memory allocated for the statistics.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param statistics A statistics.
 **/
void
j_statistics_free (JStatistics* statistics)
{
	g_return_if_fail(statistics != NULL);

	j_trace_enter(G_STRFUNC);

	g_slice_free(JStatistics, statistics);

	j_trace_leave(G_STRFUNC);
}

guint64
j_statistics_get (JStatistics* statistics, JStatisticsType type)
{
	guint64 value = 0;

	g_return_val_if_fail(statistics != NULL, 0);

	j_trace_enter(G_STRFUNC);

	switch (type)
	{
		case J_STATISTICS_FILES_CREATED:
			value = statistics->files_created;
			break;
		case J_STATISTICS_FILES_DELETED:
			value = statistics->files_deleted;
			break;
		case J_STATISTICS_SYNC:
			value = statistics->sync_count;
			break;
		case J_STATISTICS_BYTES_READ:
			value = statistics->bytes_read;
			break;
		case J_STATISTICS_BYTES_WRITTEN:
			value = statistics->bytes_written;
			break;
		case J_STATISTICS_BYTES_RECEIVED:
			value = statistics->bytes_received;
			break;
		case J_STATISTICS_BYTES_SENT:
			value = statistics->bytes_sent;
			break;
		default:
			g_warn_if_reached();
			break;
	}

	j_trace_leave(G_STRFUNC);

	return value;
}

void
j_statistics_add (JStatistics* statistics, JStatisticsType type, guint64 value)
{
	g_return_if_fail(statistics != NULL);

	j_trace_enter(G_STRFUNC);

	switch (type)
	{
		case J_STATISTICS_FILES_CREATED:
			statistics->files_created += value;
			break;
		case J_STATISTICS_FILES_DELETED:
			statistics->files_deleted += value;
			break;
		case J_STATISTICS_SYNC:
			statistics->sync_count += value;
			break;
		case J_STATISTICS_BYTES_READ:
			statistics->bytes_read += value;
			break;
		case J_STATISTICS_BYTES_WRITTEN:
			statistics->bytes_written += value;
			break;
		case J_STATISTICS_BYTES_RECEIVED:
			statistics->bytes_received += value;
			break;
		case J_STATISTICS_BYTES_SENT:
			statistics->bytes_sent += value;
			break;
		default:
			g_warn_if_reached();
			break;
	}

	if (statistics->trace)
	{
		j_trace_counter(j_statistics_get_type_name(type), value);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
