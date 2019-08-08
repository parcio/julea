/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <jstatistics.h>
#include <jtrace.h>

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
	 * The number of stat'ed files.
	 */
	guint64 files_stated;

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
		case J_STATISTICS_FILES_STATED:
			return "files_stated";
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
	J_TRACE_FUNCTION(NULL);

	JStatistics* statistics;

	statistics = g_slice_new(JStatistics);
	statistics->trace = trace;
	statistics->files_created = 0;
	statistics->files_deleted = 0;
	statistics->files_stated = 0;
	statistics->sync_count = 0;
	statistics->bytes_read = 0;
	statistics->bytes_written = 0;
	statistics->bytes_received = 0;
	statistics->bytes_sent = 0;

	return statistics;
}

/**
 * Frees the memory allocated for the statistics.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param statistics A statistics.
 **/
void
j_statistics_free (JStatistics* statistics)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(statistics != NULL);

	g_slice_free(JStatistics, statistics);
}

guint64
j_statistics_get (JStatistics* statistics, JStatisticsType type)
{
	J_TRACE_FUNCTION(NULL);

	guint64 value = 0;

	g_return_val_if_fail(statistics != NULL, 0);

	switch (type)
	{
		case J_STATISTICS_FILES_CREATED:
			value = statistics->files_created;
			break;
		case J_STATISTICS_FILES_DELETED:
			value = statistics->files_deleted;
			break;
		case J_STATISTICS_FILES_STATED:
			value = statistics->files_stated;
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

	return value;
}

void
j_statistics_add (JStatistics* statistics, JStatisticsType type, guint64 value)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(statistics != NULL);

	switch (type)
	{
		case J_STATISTICS_FILES_CREATED:
			statistics->files_created += value;
			break;
		case J_STATISTICS_FILES_DELETED:
			statistics->files_deleted += value;
			break;
		case J_STATISTICS_FILES_STATED:
			statistics->files_stated += value;
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
}

/**
 * @}
 **/
