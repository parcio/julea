/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <string.h>

#ifdef HAVE_OTF
#include <otf.h>
#endif

#include "jtrace.h"

/**
 * \defgroup JTrace Trace
 *
 * @{
 **/

struct JTrace
{
	gchar* name;

	gchar* function_name;

#ifdef HAVE_OTF
	guint32 process_id;
#endif
};

enum JTraceFlags
{
	J_TRACE_OFF     = 0,
	J_TRACE_ECHO    = 1 << 0,
	J_TRACE_HDTRACE = 1 << 1,
	J_TRACE_OTF     = 1 << 2
};

typedef enum JTraceFlags JTraceFlags;

static JTraceFlags j_trace_flags = J_TRACE_OFF;

#ifdef HAVE_OTF
static OTF_FileManager* otf_manager = NULL;
static OTF_Writer* otf_writer = NULL;

static guint32 otf_process_id = 1;
static guint32 otf_function_id = 1;
static guint32 otf_file_id = 1;
static guint32 otf_counter_id = 1;

static GHashTable* otf_function_table = NULL;
static GHashTable* otf_process_table = NULL;
static GHashTable* otf_file_table = NULL;
static GHashTable* otf_counter_table = NULL;
#endif

static
guint64
j_trace_get_time (void)
{
	GTimeVal timeval;
	guint64 timestamp;

	g_get_current_time(&timeval);
	timestamp = (timeval.tv_sec * G_USEC_PER_SEC) + timeval.tv_usec;

	return timestamp;
}

static
gchar const*
j_trace_file_operation_name (JTraceFileOperation op)
{
	switch (op)
	{
		case J_TRACE_FILE_OPEN:
			return "open";
		case J_TRACE_FILE_CLOSE:
			return "close";
		case J_TRACE_FILE_READ:
			return "read";
		case J_TRACE_FILE_WRITE:
			return "write";
		case J_TRACE_FILE_SEEK:
			return "seek";
		default:
			return NULL;
	}
}

void
j_trace_init (gchar const* name)
{
	gchar const* j_trace;

	g_return_if_fail(name != NULL);
	g_return_if_fail(j_trace_flags == J_TRACE_OFF);

	if ((j_trace = g_getenv("J_TRACE")) == NULL)
	{
		g_printerr("Tracing is disabled. Set the J_TRACE environment variable to enable it. Valid values are echo, hdtrace or otf.\n");
		return;
	}

	if (g_strcmp0(j_trace, "echo") == 0)
	{
		j_trace_flags = J_TRACE_ECHO;
	}
	else if (g_strcmp0(j_trace, "hdtrace") == 0)
	{
		j_trace_flags = J_TRACE_HDTRACE;
	}
	else if (g_strcmp0(j_trace, "otf") == 0)
	{
		j_trace_flags = J_TRACE_OTF;
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		otf_manager = OTF_FileManager_open(1);
		g_assert(otf_manager != NULL);

		otf_writer = OTF_Writer_open(name, 1, otf_manager);
		g_assert(otf_writer != NULL);

		otf_function_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		otf_process_table = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
		otf_file_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		otf_counter_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	}
#endif
}

void
j_trace_deinit (void)
{
	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	j_trace_flags = FALSE;

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		g_hash_table_unref(otf_function_table);
		otf_function_table = NULL;

		g_hash_table_unref(otf_process_table);
		otf_process_table = NULL;

		g_hash_table_unref(otf_file_table);
		otf_file_table = NULL;

		g_hash_table_unref(otf_counter_table);
		otf_counter_table = NULL;

		OTF_Writer_close(otf_writer);
		otf_writer = NULL;

		OTF_FileManager_close(otf_manager);
		otf_manager = NULL;
	}
#endif
}

void
j_trace_enter (JTrace* trace, gchar const* name)
{
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	g_return_if_fail(trace != NULL);
	g_return_if_fail(name != NULL);

	timestamp = j_trace_get_time();

	if (j_trace_flags == J_TRACE_ECHO)
	{
		g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s: ENTER %s\n", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, trace->name, name);
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		gpointer value;
		guint32 function_id;

		if ((value = g_hash_table_lookup(otf_function_table, name)) == NULL)
		{
			function_id = otf_function_id;
			otf_function_id++;

			OTF_Writer_writeDefFunction(otf_writer, 1, function_id, name, 0, 0);
			g_hash_table_insert(otf_function_table, g_strdup(name), GUINT_TO_POINTER(function_id));
		}
		else
		{
			function_id = GPOINTER_TO_UINT(value);
		}

		OTF_Writer_writeEnter(otf_writer, timestamp, function_id, trace->process_id, 0);
	}
#endif
}

void
j_trace_leave (JTrace* trace, gchar const* name)
{
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	g_return_if_fail(trace != NULL);
	g_return_if_fail(name != NULL);

	timestamp = j_trace_get_time();

	if (j_trace_flags == J_TRACE_ECHO)
	{
		g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s: LEAVE %s\n", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, trace->name, name);
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		gpointer value;
		guint32 function_id;

		value = g_hash_table_lookup(otf_function_table, name);
		g_assert(value != NULL);
		function_id = GPOINTER_TO_UINT(value);

		OTF_Writer_writeLeave(otf_writer, timestamp, function_id, trace->process_id, 0);
	}
#endif
}

void
j_trace_file_begin (JTrace* trace, gchar const* path, JTraceFileOperation op)
{
	guint64 timestamp;

	g_return_if_fail(path != NULL);

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	timestamp = j_trace_get_time();

	if (j_trace_flags == J_TRACE_ECHO)
	{
		g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s: BEGIN %s %s\n", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, trace->name, j_trace_file_operation_name(op), path);
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		gpointer value;
		guint32 file_id;

		if ((value = g_hash_table_lookup(otf_file_table, path)) == NULL)
		{
			file_id = otf_file_id;
			otf_file_id++;

			OTF_Writer_writeDefFile(otf_writer, 1, file_id, path, 0);
			g_hash_table_insert(otf_file_table, g_strdup(path), GUINT_TO_POINTER(file_id));
		}
		else
		{
			file_id = GPOINTER_TO_UINT(value);
		}

		OTF_Writer_writeBeginFileOperation(otf_writer, timestamp, trace->process_id, 1, 0);
	}
#endif

	return;
}

void
j_trace_file_end (JTrace* trace, gchar const* path, JTraceFileOperation op, guint64 length)
{
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	timestamp = j_trace_get_time();

	if (j_trace_flags == J_TRACE_ECHO)
	{
		if (op == J_TRACE_FILE_READ || op == J_TRACE_FILE_WRITE)
		{
			g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s: END %s %s (length=%" G_GUINT64_FORMAT ")\n", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, trace->name, j_trace_file_operation_name(op), path, length);
		}
		else
		{
			g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s: END %s %s\n", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, trace->name, j_trace_file_operation_name(op), path);
		}
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		gpointer value;
		guint32 file_id;
		guint32 otf_op;

		switch (op)
		{
			case J_TRACE_FILE_OPEN:
				otf_op = OTF_FILEOP_OPEN;
				break;
			case J_TRACE_FILE_CLOSE:
				otf_op = OTF_FILEOP_CLOSE;
				break;
			case J_TRACE_FILE_READ:
				otf_op = OTF_FILEOP_READ;
				break;
			case J_TRACE_FILE_WRITE:
				otf_op = OTF_FILEOP_WRITE;
				break;
			case J_TRACE_FILE_SEEK:
				otf_op = OTF_FILEOP_SEEK;
				break;
			default:
				g_return_if_reached();
		}

		value = g_hash_table_lookup(otf_file_table, path);
		g_assert(value != NULL);
		file_id = GPOINTER_TO_UINT(value);

		OTF_Writer_writeEndFileOperation(otf_writer, timestamp, trace->process_id, file_id, 1, 0, otf_op, length, 0);
	}
#endif
}

JTrace*
j_trace_thread_enter (GThread* thread, gchar const* function_name)
{
	JTrace* trace;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return NULL;
	}

	g_return_val_if_fail(function_name != NULL, NULL);

	trace = g_slice_new(JTrace);
	trace->function_name = g_strdup(function_name);

	if (thread == NULL)
	{
		trace->name = g_strdup("Main process");
	}
	else
	{
		trace->name = g_strdup_printf("Thread %p", (gconstpointer)thread);
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		gpointer value;
		guint32 process_id;

		if (!g_hash_table_lookup_extended(otf_process_table, thread, NULL, &value))
		{
			process_id = otf_process_id;
			otf_process_id++;

			OTF_Writer_writeDefProcess(otf_writer, 1, process_id, trace->name, 0);
			g_hash_table_insert(otf_process_table, thread, GUINT_TO_POINTER(process_id));
		}
		else
		{
			process_id = GPOINTER_TO_UINT(value);
		}

		trace->process_id = process_id;
	}
#endif

	j_trace_enter(trace, function_name);

	return trace;
}

void
j_trace_thread_leave (JTrace* trace)
{
	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	g_return_if_fail(trace != NULL);

	j_trace_leave(trace, trace->function_name);

	g_free(trace->name);
	g_free(trace->function_name);

	g_slice_free(JTrace, trace);
}

void
j_trace_counter (JTrace* trace, gchar const* name, guint64 counter_value)
{
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	g_return_if_fail(trace != NULL);
	g_return_if_fail(name != NULL);

	timestamp = j_trace_get_time();

	if (j_trace_flags == J_TRACE_ECHO)
	{
		g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s: COUNTER %s %" G_GUINT64_FORMAT "\n", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, trace->name, name, counter_value);
	}

#ifdef HAVE_OTF
	if (j_trace_flags == J_TRACE_OTF)
	{
		gpointer value;
		guint32 counter_id;

		if ((value = g_hash_table_lookup(otf_counter_table, name)) == NULL)
		{
			counter_id = otf_counter_id;
			otf_counter_id++;

			OTF_Writer_writeDefCounter(otf_writer, 1, counter_id, name, OTF_COUNTER_TYPE_ACC + OTF_COUNTER_SCOPE_START, 0, NULL);
			g_hash_table_insert(otf_counter_table, g_strdup(name), GUINT_TO_POINTER(counter_id));
		}
		else
		{
			counter_id = GPOINTER_TO_UINT(value);
		}

		OTF_Writer_writeCounter(otf_writer, timestamp, trace->process_id, counter_id, counter_value);
	}
#endif
}

/**
 * @}
 **/
