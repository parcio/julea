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

void init (JBackendVTable*, gchar const*);
void deinit (void);

static gchar* jd_backend_path = NULL;

static
gpointer
jd_backend_open (gchar const* store, gchar const* collection, gchar const* item)
{
	gchar* path;
	gint fd;

	j_trace_enter(G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);

	fd = open(path, O_RDWR);

	if (fd == -1)
	{
		gchar* parent;

		parent = g_path_get_dirname(path);
		g_mkdir_with_parents(parent, 0700);
		g_free(parent);

		fd = open(path, O_RDWR | O_CREAT);
	}

	g_free(path);

	j_trace_leave(G_STRFUNC);

	return GINT_TO_POINTER(fd);
}

static
void
jd_backend_close (gpointer item)
{
	gint fd = GPOINTER_TO_INT(item);

	j_trace_enter(G_STRFUNC);

	close(fd);

	j_trace_leave(G_STRFUNC);
}

static
guint64
jd_backend_read (gpointer item, gpointer buffer, guint64 length, guint64 offset)
{
	gint fd = GPOINTER_TO_INT(item);
	gsize bytes_read;

	j_trace_enter(G_STRFUNC);

	bytes_read = pread(fd, buffer, length, offset);

	j_trace_leave(G_STRFUNC);

	return bytes_read;
}

static
guint64
jd_backend_write (gpointer item, gconstpointer buffer, guint64 length, guint64 offset)
{
	gint fd = GPOINTER_TO_INT(item);
	gsize bytes_written;

	j_trace_enter(G_STRFUNC);

	bytes_written = pwrite(fd, buffer, length, offset);

	j_trace_leave(G_STRFUNC);

	return bytes_written;
}

G_MODULE_EXPORT
void
init (JBackendVTable* vtable, gchar const* path)
{
	j_trace_enter(G_STRFUNC);

	vtable->open = jd_backend_open;
	vtable->close = jd_backend_close;
	vtable->read = jd_backend_read;
	vtable->write = jd_backend_write;

	jd_backend_path = g_strdup(path);

	g_mkdir_with_parents(path, 0700);

	j_trace_leave(G_STRFUNC);
}

G_MODULE_EXPORT
void
deinit (void)
{
	j_trace_enter(G_STRFUNC);

	g_free(jd_backend_path);

	j_trace_leave(G_STRFUNC);
}
