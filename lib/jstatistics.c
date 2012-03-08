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

#include <jstatistics-internal.h>

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
	guint64 files_created;
	guint64 files_deleted;

	guint64 sync_count;

	guint64 bytes_read;
	guint64 bytes_written;

	guint64 bytes_received;
	guint64 bytes_sent;
};

/**
 * Creates a new statistics.
 *
 * \author Michael Kuhn
 *
 * \code
 * JStatistics* s;
 *
 * s = j_statistics_new();
 * \endcode
 *
 * \return A new statistics. Should be freed with j_statistics_free().
 **/
JStatistics*
j_statistics_new (void)
{
	JStatistics* statistics;

	statistics = g_slice_new(JStatistics);
	statistics->files_created = 0;
	statistics->files_deleted = 0;
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

	g_slice_free(JStatistics, statistics);
}

guint64
j_statistics_get (JStatistics* statistics, JStatisticsType type)
{
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
j_statistics_set (JStatistics* statistics, JStatisticsType type, guint64 value)
{
	g_return_if_fail(statistics != NULL);

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
}

/**
 * @}
 **/
