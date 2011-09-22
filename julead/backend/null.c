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

#include <glib.h>
#include <gmodule.h>

#include <jtrace.h>

#include "backend.h"
#include "backend-internal.h"

G_MODULE_EXPORT
void
backend_open (JBackendFile* bf, gchar const* store, gchar const* collection, gchar const* item, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	bf->path = g_strdup_printf("%s.%s.%s", store, collection, item);
	bf->user_data = NULL;

	j_trace_file_begin(trace, bf->path, J_TRACE_FILE_OPEN);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_OPEN, 0, 0);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
void
backend_close (JBackendFile* bf, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(bf->path);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
void
backend_sync (JBackendFile* bf, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path, J_TRACE_FILE_SYNC);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_SYNC, 0, 0);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
guint64
backend_read (JBackendFile* bf, gpointer buffer, guint64 length, guint64 offset, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path, J_TRACE_FILE_READ);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_READ, length, offset);

	j_trace_leave(trace, G_STRFUNC);

	return length;
}

G_MODULE_EXPORT
guint64
backend_write (JBackendFile* bf, gconstpointer buffer, guint64 length, guint64 offset, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path, J_TRACE_FILE_WRITE);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_WRITE, length, offset);

	j_trace_leave(trace, G_STRFUNC);

	return length;
}

G_MODULE_EXPORT
void
backend_init (gchar const* path, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);
	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
void
backend_fini (JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);
	j_trace_leave(trace, G_STRFUNC);
}
