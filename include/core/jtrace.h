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

#ifndef JULEA_TRACE_H
#define JULEA_TRACE_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

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

JTrace* j_trace_enter (gchar const*, gchar const*, ...) G_GNUC_PRINTF(2, 3);
void j_trace_leave (JTrace*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JTrace, j_trace_leave)

#ifdef JULEA_DEBUG
#ifdef __COUNTER__
#define J_TRACE(name, ...) g_autoptr(JTrace) G_PASTE(j_trace, __COUNTER__) G_GNUC_UNUSED = j_trace_enter(name, __VA_ARGS__)
#define J_TRACE_FUNCTION(...) g_autoptr(JTrace) G_PASTE(j_trace_function, __COUNTER__) G_GNUC_UNUSED = j_trace_enter(G_STRFUNC, __VA_ARGS__)
#else
#define J_TRACE(name, ...) g_autoptr(JTrace) G_PASTE(j_trace, __LINE__) G_GNUC_UNUSED = j_trace_enter(name, __VA_ARGS__)
#define J_TRACE_FUNCTION(...) g_autoptr(JTrace) G_PASTE(j_trace_function, __LINE__) G_GNUC_UNUSED = j_trace_enter(G_STRFUNC, __VA_ARGS__)
#endif
#else
#define J_TRACE(name, ...) guint G_PASTE(j_trace, __LINE__) G_GNUC_UNUSED = 0;
#define J_TRACE_FUNCTION(...) guint G_PASTE(j_trace_function, __LINE__) G_GNUC_UNUSED = 0;
#endif

void j_trace_file_begin (gchar const*, JTraceFileOperation);
void j_trace_file_end (gchar const*, JTraceFileOperation, guint64, guint64);

void j_trace_counter (gchar const*, guint64);

G_END_DECLS

#endif
