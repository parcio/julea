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
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <jbackend.h>
#include <jtrace-internal.h>

static gchar* jd_backend_path = NULL;

static
gboolean
backend_create (JBackendItem* bf, gchar const* path, gpointer data)
{
	GFile* file;
	GFile* parent;
	GFileIOStream* stream;
	gchar* full_path;

	(void)data;

	j_trace_enter(G_STRFUNC);

	full_path = g_build_filename(jd_backend_path, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);

	parent = g_file_get_parent(file);
	g_file_make_directory_with_parents(parent, NULL, NULL);
	g_object_unref(parent);

	stream = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, NULL);

	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	bf->path = full_path;
	bf->user_data = stream;

	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

static
gboolean
backend_delete (JBackendItem* bf, gpointer data)
{
	gboolean ret = FALSE;
	GFile* file;

	(void)data;

	j_trace_enter(G_STRFUNC);

	file = g_file_new_for_path(bf->path);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = g_file_delete(file, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_open (JBackendItem* bf, gchar const* path, gpointer data)
{
	GFile* file;
	GFileIOStream* stream;
	gchar* full_path;

	(void)data;

	j_trace_enter(G_STRFUNC);

	full_path = g_build_filename(jd_backend_path, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	stream = g_file_open_readwrite(file, NULL, NULL);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	bf->path = full_path;
	bf->user_data = stream;

	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return (stream != NULL);
}

static
gboolean
backend_close (JBackendItem* bf, gpointer data)
{
	GFileIOStream* stream = bf->user_data;

	(void)data;

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

static
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size, gpointer data)
{
	GFileIOStream* stream = bf->user_data;
	//GOutputStream* output;

	(void)flags;
	(void)data;

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

static
gboolean
backend_sync (JBackendItem* bf, gpointer data)
{
	GFileIOStream* stream = bf->user_data;
	GOutputStream* output;

	(void)data;

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

static
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read, gpointer data)
{
	GFileIOStream* stream = bf->user_data;
	GInputStream* input;
	gsize nbytes;

	(void)data;

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

static
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written, gpointer data)
{
	GFileIOStream* stream = bf->user_data;
	GOutputStream* output;
	gsize nbytes;

	(void)data;

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

static
gboolean
backend_init (gchar const* path)
{
	GFile* file;

	j_trace_enter(G_STRFUNC);

	jd_backend_path = g_strdup(path);

	file = g_file_new_for_path(path);
	g_file_make_directory_with_parents(file, NULL, NULL);
	g_object_unref(file);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);

	g_free(jd_backend_path);

	j_trace_leave(G_STRFUNC);
}

static
JBackend gio_backend = {
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

	return &gio_backend;
}
