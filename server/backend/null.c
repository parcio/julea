/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <jtrace-internal.h>

#include "backend.h"

static
gboolean
backend_create (JBackendItem* bf, gchar const* path, gpointer data)
{
	(void)data;

	j_trace_enter(G_STRFUNC);

	bf->path = g_strdup(path);
	bf->user_data = NULL;

	j_trace_file_begin(bf->path, J_TRACE_FILE_CREATE);
	j_trace_file_end(bf->path, J_TRACE_FILE_CREATE, 0, 0);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_delete (JBackendItem* bf, gpointer data)
{
	(void)data;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_open (JBackendItem* bf, gchar const* path, gpointer data)
{
	(void)data;

	j_trace_enter(G_STRFUNC);

	bf->path = g_strdup(path);
	bf->user_data = NULL;

	j_trace_file_begin(bf->path, J_TRACE_FILE_OPEN);
	j_trace_file_end(bf->path, J_TRACE_FILE_OPEN, 0, 0);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_close (JBackendItem* bf, gpointer data)
{
	(void)data;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(bf->path);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size, gpointer data)
{
	(void)flags;
	(void)data;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
	j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);

	*modification_time = 0;
	*size = 0;

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_sync (JBackendItem* bf, gpointer data)
{
	(void)data;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
	j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read, gpointer data)
{
	(void)buffer;
	(void)data;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
	j_trace_file_end(bf->path, J_TRACE_FILE_READ, length, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = length;
	}

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written, gpointer data)
{
	(void)buffer;
	(void)data;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
	j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, length, offset);

	if (bytes_written != NULL)
	{
		*bytes_written = length;
	}

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_init (gchar const* path)
{
	(void)path;

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);
}

static
JBackend null_backend = {
	.component = J_BACKEND_COMPONENT_SERVER,
	.type = J_BACKEND_TYPE_DATA,
	.u.data = {
		.init = backend_init,
		.fini = backend_fini,
		.thread_init = NULL,
		.thread_fini = NULL,
		.create = backend_create,
		.delete = backend_delete,
		.open = backend_open,
		.close = backend_close,
		.status = backend_status,
		.sync = backend_sync,
		.read = backend_read,
		.write = backend_write
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	j_trace_enter(G_STRFUNC);

	j_trace_leave(G_STRFUNC);

	return &null_backend;
}
