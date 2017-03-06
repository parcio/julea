/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#ifndef H_STATISTICS_INTERNAL
#define H_STATISTICS_INTERNAL

#include <glib.h>

#include <julea-internal.h>

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

J_GNUC_INTERNAL JStatistics* j_statistics_new (gboolean);
J_GNUC_INTERNAL void j_statistics_free (JStatistics*);

J_GNUC_INTERNAL guint64 j_statistics_get (JStatistics*, JStatisticsType);
J_GNUC_INTERNAL void j_statistics_add (JStatistics*, JStatisticsType, guint64);

#endif
