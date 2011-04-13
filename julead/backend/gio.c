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
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <jtrace.h>

#include "backend.h"

void init (JBackendVTable*, gchar const*, JTrace*);
void deinit (JTrace*);

static gchar* jd_backend_path = NULL;

static
gpointer
jd_backend_open (gchar const* store, gchar const* collection, gchar const* item, JTrace* trace)
{
	GFile* file;
	GFileIOStream* stream;
	gchar* path;

	j_trace_enter(trace, G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);
	file = g_file_new_for_path(path);

	stream = g_file_open_readwrite(file, NULL, NULL);

	if (stream == NULL)
	{
		GFile* parent;

		parent = g_file_get_parent(file);
		g_file_make_directory_with_parents(parent, NULL, NULL);
		g_object_unref(parent);

		stream = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, NULL);
	}

	g_object_unref(file);
	g_free(path);

	j_trace_leave(trace, G_STRFUNC);

	return stream;
}

static
void
jd_backend_close (gpointer item, JTrace* trace)
{
	GFileIOStream* stream = item;

	j_trace_enter(trace, G_STRFUNC);

	g_io_stream_close(G_IO_STREAM(stream), NULL, NULL);

	j_trace_leave(trace, G_STRFUNC);
}

static
guint64
jd_backend_read (gpointer item, gpointer buffer, guint64 length, guint64 offset, JTrace* trace)
{
	GFileIOStream* stream = item;
	GInputStream* input;
	gsize bytes_read;

	j_trace_enter(trace, G_STRFUNC);

	input = g_io_stream_get_input_stream(G_IO_STREAM(stream));
	g_seekable_seek(G_SEEKABLE(stream), offset, G_SEEK_SET, NULL, NULL);
	g_input_stream_read_all(input, buffer, length, &bytes_read, NULL, NULL);

	j_trace_leave(trace, G_STRFUNC);

	return bytes_read;
}

static
guint64
jd_backend_write (gpointer item, gconstpointer buffer, guint64 length, guint64 offset, JTrace* trace)
{
	GFileIOStream* stream = item;
	GOutputStream* output;
	gsize bytes_written;

	j_trace_enter(trace, G_STRFUNC);

	output = g_io_stream_get_output_stream(G_IO_STREAM(stream));
	g_seekable_seek(G_SEEKABLE(stream), offset, G_SEEK_SET, NULL, NULL);
	g_output_stream_write_all(output, buffer, length, &bytes_written, NULL, NULL);

	j_trace_leave(trace, G_STRFUNC);

	return bytes_written;
}

G_MODULE_EXPORT
void
init (JBackendVTable* vtable, gchar const* path, JTrace* trace)
{
	GFile* file;

	j_trace_enter(trace, G_STRFUNC);

	vtable->open = jd_backend_open;
	vtable->close = jd_backend_close;
	vtable->read = jd_backend_read;
	vtable->write = jd_backend_write;

	jd_backend_path = g_strdup(path);

	file = g_file_new_for_path(path);
	g_file_make_directory_with_parents(file, NULL, NULL);
	g_object_unref(file);

	j_trace_leave(trace, G_STRFUNC);
}

G_MODULE_EXPORT
void
deinit (JTrace* trace)
{
	j_trace_enter(trace, G_STRFUNC);

	g_free(jd_backend_path);

	j_trace_leave(trace, G_STRFUNC);
}
