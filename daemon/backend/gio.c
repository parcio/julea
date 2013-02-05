/*
 * Copyright (c) 2010-2013 Michael Kuhn
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
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <jtrace-internal.h>

#include "backend.h"
#include "backend-internal.h"

static gchar* jd_backend_path = NULL;

G_MODULE_EXPORT
gboolean
backend_create (JBackendFile* bf, gchar const* store, gchar const* collection, gchar const* item)
{
	GFile* file;
	GFile* parent;
	GFileIOStream* stream;
	gchar* path;

	j_trace_enter(G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);
	file = g_file_new_for_path(path);

	j_trace_file_begin(path, J_TRACE_FILE_CREATE);

	parent = g_file_get_parent(file);
	g_file_make_directory_with_parents(parent, NULL, NULL);
	g_object_unref(parent);

	stream = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, NULL);

	j_trace_file_end(path, J_TRACE_FILE_CREATE, 0, 0);

	bf->path = path;
	bf->user_data = stream;

	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
gboolean
backend_delete (JBackendFile* bf)
{
	gboolean ret = FALSE;
	GFile* file;

	j_trace_enter(G_STRFUNC);

	file = g_file_new_for_path(bf->path);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = g_file_delete(file, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return ret;
}

G_MODULE_EXPORT
gboolean
backend_open (JBackendFile* bf, gchar const* store, gchar const* collection, gchar const* item)
{
	GFile* file;
	GFileIOStream* stream;
	gchar* path;

	j_trace_enter(G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);
	file = g_file_new_for_path(path);

	j_trace_file_begin(path, J_TRACE_FILE_OPEN);
	stream = g_file_open_readwrite(file, NULL, NULL);
	j_trace_file_end(path, J_TRACE_FILE_OPEN, 0, 0);

	bf->path = path;
	bf->user_data = stream;

	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
gboolean
backend_close (JBackendFile* bf)
{
	GFileIOStream* stream = bf->user_data;

	j_trace_enter(G_STRFUNC);

	if (stream != NULL)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
		g_io_stream_close(G_IO_STREAM(stream), NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);
	}

	g_object_unref(stream);

	g_free(bf->path);

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
gboolean
backend_status (JBackendFile* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size)
{
	GFileIOStream* stream = bf->user_data;
	//GOutputStream* output;

	j_trace_enter(G_STRFUNC);

	if (stream != NULL)
	{
		//output = g_io_stream_get_output_stream(G_IO_STREAM(stream));

		j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
		// FIXME
		//g_output_stream_flush(output, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);

		// FIXME
		*modification_time = 0;
		*size = 0;
	}

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
gboolean
backend_sync (JBackendFile* bf)
{
	GFileIOStream* stream = bf->user_data;
	GOutputStream* output;

	j_trace_enter(G_STRFUNC);

	if (stream != NULL)
	{
		output = g_io_stream_get_output_stream(G_IO_STREAM(stream));

		j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
		g_output_stream_flush(output, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);
	}

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
gboolean
backend_read (JBackendFile* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	GFileIOStream* stream = bf->user_data;
	GInputStream* input;
	gsize nbytes;

	j_trace_enter(G_STRFUNC);

	if (stream != NULL)
	{
		input = g_io_stream_get_input_stream(G_IO_STREAM(stream));

		j_trace_file_begin(bf->path, J_TRACE_FILE_SEEK);
		g_seekable_seek(G_SEEKABLE(stream), offset, G_SEEK_SET, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_SEEK, 0, offset);

		j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
		g_input_stream_read_all(input, buffer, length, &nbytes, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_READ, nbytes, offset);

		if (bytes_read != NULL)
		{
			*bytes_read = nbytes;
		}
	}

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
gboolean
backend_write (JBackendFile* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	GFileIOStream* stream = bf->user_data;
	GOutputStream* output;
	gsize nbytes;

	j_trace_enter(G_STRFUNC);

	if (stream != NULL)
	{
		output = g_io_stream_get_output_stream(G_IO_STREAM(stream));

		j_trace_file_begin(bf->path, J_TRACE_FILE_SEEK);
		g_seekable_seek(G_SEEKABLE(stream), offset, G_SEEK_SET, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_SEEK, 0, offset);

		j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
		g_output_stream_write_all(output, buffer, length, &nbytes, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, nbytes, offset);

		if (bytes_written != NULL)
		{
			*bytes_written = nbytes;
		}
	}

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

G_MODULE_EXPORT
void
backend_init (gchar const* path)
{
	GFile* file;

	j_trace_enter(G_STRFUNC);

	jd_backend_path = g_strdup(path);

	file = g_file_new_for_path(path);
	g_file_make_directory_with_parents(file, NULL, NULL);
	g_object_unref(file);

	j_trace_leave(G_STRFUNC);
}

G_MODULE_EXPORT
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);

	g_free(jd_backend_path);

	j_trace_leave(G_STRFUNC);
}
