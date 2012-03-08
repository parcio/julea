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

#ifndef H_STATISTICS_INTERNAL
#define H_STATISTICS_INTERNAL

#include <glib.h>

#include <julea-internal.h>

enum JStatisticsType
{
	J_STATISTICS_FILES_CREATED,
	J_STATISTICS_FILES_DELETED,
	J_STATISTICS_SYNC,
	J_STATISTICS_BYTES_READ,
	J_STATISTICS_BYTES_WRITTEN,
	J_STATISTICS_BYTES_RECEIVED,
	J_STATISTICS_BYTES_SENT
};

typedef enum JStatisticsType JStatisticsType;

struct JStatistics;

typedef struct JStatistics JStatistics;

J_GNUC_INTERNAL JStatistics* j_statistics_new (void);
J_GNUC_INTERNAL void j_statistics_free (JStatistics*);

J_GNUC_INTERNAL guint64 j_statistics_get (JStatistics*, JStatisticsType);
J_GNUC_INTERNAL void j_statistics_set (JStatistics*, JStatisticsType, guint64);

#endif
