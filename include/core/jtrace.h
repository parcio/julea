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

#ifndef JULEA_TRACE_H
#define JULEA_TRACE_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JTrace Trace
 *
 * The JTrace framework offers abstracted trace capabilities.
 * It can use normal terminal output and OTF.
 *
 * @{
 **/

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

/**
 * Initializes the trace framework.
 * Tracing is disabled by default.
 * Set the \c J_TRACE environment variable to enable it.
 * Valid values are \e echo and \e otf.
 * Multiple values can be combined with commas.
 *
 * \code
 * j_trace_init("JULEA");
 * \endcode
 *
 * \param name A trace name.
 **/
void j_trace_init(gchar const* name);

/**
 * Shuts down the trace framework.
 *
 * \code
 * j_trace_fini();
 * \endcode
 **/
void j_trace_fini(void);

/**
 * Traces the entering of a function.
 *
 * \code
 * \endcode
 *
 * \param name A function name.
 **/
JTrace* j_trace_enter(gchar const* name, gchar const* format, ...) G_GNUC_PRINTF(2, 3);

/**
 * Traces the leaving of a function.
 *
 * \code
 * \endcode
 *
 * \param trace A JTrace.
 **/
void j_trace_leave(JTrace* trace);

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
#ifdef __COUNTER__
#define J_TRACE(name, ...) g_autoptr(JTrace) G_PASTE(j_trace, __COUNTER__) G_GNUC_UNUSED = NULL
#define J_TRACE_FUNCTION(...) g_autoptr(JTrace) G_PASTE(j_trace_function, __COUNTER__) G_GNUC_UNUSED = NULL
#else
#define J_TRACE(name, ...) g_autoptr(JTrace) G_PASTE(j_trace, __LINE__) G_GNUC_UNUSED = NULL
#define J_TRACE_FUNCTION(...) g_autoptr(JTrace) G_PASTE(j_trace_function, __LINE__) G_GNUC_UNUSED = NULL
#endif
#endif

/**
 * Traces the beginning of a file operation.
 *
 * \code
 * \endcode
 *
 * \param path A file path.
 * \param op   A file operation.
 **/
void j_trace_file_begin(gchar const* path, JTraceFileOperation op);

/**
 * Traces the ending of a file operation.
 *
 * \code
 * \endcode
 *
 * \param path   A file path.
 * \param op     A file operation.
 * \param length A length.
 * \param offset An offset.
 **/
void j_trace_file_end(gchar const* path, JTraceFileOperation op, guint64 length, guint64 offset);

/**
 * Traces a counter.
 *
 * \code
 * \endcode
 *
 * \param name          A counter name.
 * \param counter_value A counter value.
 **/
void j_trace_counter(gchar const* name, guint64 counter_value);

/**
 * @}
 **/

G_END_DECLS

#endif
