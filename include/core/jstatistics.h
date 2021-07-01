/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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

#ifndef JULEA_STATISTICS_H
#define JULEA_STATISTICS_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JStatistics Statistics
 *
 * @{
 **/

enum JStatisticsType
{
	J_STATISTICS_FILES_CREATED,
	J_STATISTICS_FILES_DELETED,
	J_STATISTICS_FILES_STATED,
	J_STATISTICS_SYNC,
	J_STATISTICS_BYTES_READ,
	J_STATISTICS_BYTES_WRITTEN,
	J_STATISTICS_BYTES_RECEIVED,
	J_STATISTICS_BYTES_SENT
};

typedef enum JStatisticsType JStatisticsType;

struct JStatistics;

typedef struct JStatistics JStatistics;

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
 * \param trace Specify wether tracing was on or off while gathering data.
 *
 * \return A new statistics. Should be freed with j_statistics_free().
 **/
JStatistics* j_statistics_new(gboolean trace);

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
void j_statistics_free(JStatistics* statistics);

/**
 * Get a value from a statistic.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param statistics A statistics.
 * \param type       A statistics type.
 * 
 * \return The requested value.
 **/
guint64 j_statistics_get(JStatistics* statistics, JStatisticsType type);

/**
 * Adds a value to a statistic.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param statistics A statistics.
 * \param type       A statistics type.
 * \param value		 A value to add to the statistics.
 * 
 **/
void j_statistics_add(JStatistics* statistics, JStatisticsType type, guint64 value);

/**
 * @}
 **/

G_END_DECLS

#endif
