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

#define _XOPEN_SOURCE 500

#include <glib.h>
#include <glib/gstdio.h>
#include <gmodule.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <jtrace.h>

#include "backend.h"
#include "backend-internal.h"

static gchar* jd_backend_path = NULL;

G_MODULE_EXPORT
void
backend_open (JBackendFile* bf, gchar const* store, gchar const* collection, gchar const* item, JTrace* trace)
{
	gchar* path;
	gint fd;

	j_trace_enter(trace, G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);

	j_trace_file_begin(trace, path);
	fd = open(path, O_RDWR);

	if (fd == -1)
	{
		gchar* parent;

		parent = g_path_get_dirname(path);
		g_mkdir_with_parents(parent, 0700);
		g_free(parent);

		fd = open(path, O_RDWR | O_CREAT);
	}

	j_trace_file_end(trace, path, J_TRACE_FILE_OPEN, 0);

	bf->path = path;
	bf->user_data = GINT_TO_POINTER(fd);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
void
backend_close (JBackendFile* bf, JTrace* trace)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path);
	close(fd);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_CLOSE, 0);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
guint64
backend_read (JBackendFile* bf, gpointer buffer, guint64 length, guint64 offset, JTrace* trace)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);
	gsize bytes_read;

	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path);
	bytes_read = pread(fd, buffer, length, offset);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_READ, bytes_read);

	j_trace_leave(trace, G_STRFUNC);

	return bytes_read;
}

G_MODULE_EXPORT
guint64
backend_write (JBackendFile* bf, gconstpointer buffer, guint64 length, guint64 offset, JTrace* trace)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);
	gsize bytes_written;

	j_trace_enter(trace, G_STRFUNC);

	j_trace_file_begin(trace, bf->path);
	bytes_written = pwrite(fd, buffer, length, offset);
	j_trace_file_end(trace, bf->path, J_TRACE_FILE_WRITE, bytes_written);

	j_trace_leave(trace, G_STRFUNC);

	return bytes_written;
}

G_MODULE_EXPORT
void
backend_init (gchar const* path, JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	jd_backend_path = g_strdup(path);

	g_mkdir_with_parents(path, 0700);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
void
backend_deinit (JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	g_free(jd_backend_path);

	j_trace_leave(trace, G_STRFUNC);
}
