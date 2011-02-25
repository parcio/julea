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

#ifdef HAVE_OTF
static OTF_FileManager* otf_manager = NULL;
static OTF_Writer* otf_writer = NULL;

static guint32 otf_process_id = 1;
static guint32 otf_function_id = 1;
static guint32 otf_file_id = 1;

static GHashTable* otf_function_table = NULL;
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

void
j_trace_init (gchar const* name)
{
	g_return_if_fail(name != NULL);

#ifdef HAVE_OTF
	g_return_if_fail(otf_manager == NULL);
	g_return_if_fail(otf_writer == NULL);
	g_return_if_fail(otf_function_table == NULL);

	otf_manager = OTF_FileManager_open(1);
	g_assert(otf_manager != NULL);

	otf_writer = OTF_Writer_open(name, 1, otf_manager);
	g_assert(otf_writer != NULL);

	otf_function_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
#endif
}

void
j_trace_deinit (void)
{
#ifdef HAVE_OTF
	g_return_if_fail(otf_manager != NULL);
	g_return_if_fail(otf_writer != NULL);
	g_return_if_fail(otf_function_table != NULL);

	g_hash_table_unref(otf_function_table);
	otf_function_table = NULL;

	OTF_Writer_close(otf_writer);
	otf_writer = NULL;

	OTF_FileManager_close(otf_manager);
	otf_manager = NULL;
#endif
}

void
j_trace_define_process (gchar const* name)
{
	g_return_if_fail(name != NULL);

#ifdef HAVE_OTF
	OTF_Writer_writeDefProcess(otf_writer, 1, otf_process_id, name, 0);
	otf_process_id++;
#endif
}

void
j_trace_define_file (gchar const* name)
{
	g_return_if_fail(name != NULL);

#ifdef HAVE_OTF
	OTF_Writer_writeDefFile(otf_writer, 1, otf_file_id, name, 0);
	otf_file_id++;
#endif
}

void
j_trace_enter (gchar const* name)
{
#ifdef HAVE_OTF
	gpointer value;
	guint32 function_id;
#endif

	g_return_if_fail(name != NULL);

#ifdef HAVE_OTF
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

	OTF_Writer_writeEnter(otf_writer, j_trace_get_time(), 1, 1, 0);
#endif
}

void
j_trace_leave (gchar const* name)
{
#ifdef HAVE_OTF
	gpointer value;
	guint32 function_id;
#endif

	g_return_if_fail(name != NULL);

#ifdef HAVE_OTF
	value = g_hash_table_lookup(otf_function_table, name);
	g_assert(value != NULL);
	function_id = GPOINTER_TO_UINT(value);

	OTF_Writer_writeLeave(otf_writer, j_trace_get_time(), function_id, 1, 0);
#endif
}

void
j_trace_file_begin (gchar const* path)
{
	g_return_if_fail(path != NULL);

#ifdef HAVE_OTF
	OTF_Writer_writeBeginFileOperation(otf_writer, j_trace_get_time(), 1, 1, 0);
#endif
}

void
j_trace_file_end (gchar const* path, JTraceFileOp op, guint64 length)
{
#ifdef HAVE_OTF
	guint32 otf_op;
#endif

	g_return_if_fail(path != NULL);

#ifdef HAVE_OTF
	switch (op)
	{
		case J_TRACE_FILE_READ:
			otf_op = OTF_FILEOP_READ;
			break;
		case J_TRACE_FILE_WRITE:
			otf_op = OTF_FILEOP_WRITE;
			break;
		default:
			g_return_if_reached();
	}

	OTF_Writer_writeEndFileOperation(otf_writer, j_trace_get_time(), 1, otf_file_id, 1, 0, otf_op, length, 0);
#endif
}

/**
 * @}
 **/
