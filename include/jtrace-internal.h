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

#ifndef H_TRACE
#define H_TRACE

#include <glib.h>

enum JTraceFileOperation
{
	J_TRACE_FILE_CLOSE,
	J_TRACE_FILE_CREATE,
	J_TRACE_FILE_DELETE,
	J_TRACE_FILE_OPEN,
	J_TRACE_FILE_READ,
	J_TRACE_FILE_SEEK,
	J_TRACE_FILE_STATUS,
	J_TRACE_FILE_SYNC,
	J_TRACE_FILE_WRITE
};

typedef enum JTraceFileOperation JTraceFileOperation;

struct JTrace;

typedef struct JTrace JTrace;

void j_trace_init (gchar const*);
void j_trace_fini (void);

JTrace* j_trace_get_thread_default (void);

JTrace* j_trace_new (GThread*);
JTrace* j_trace_ref (JTrace*);
void j_trace_unref (JTrace*);

void j_trace_enter (gchar const*, gchar const*, ...) G_GNUC_PRINTF(2, 3);
void j_trace_leave (gchar const*);

void j_trace_file_begin (gchar const*, JTraceFileOperation);
void j_trace_file_end (gchar const*, JTraceFileOperation, guint64, guint64);

void j_trace_counter (gchar const*, guint64);

#endif
