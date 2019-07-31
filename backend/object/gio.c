/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <julea.h>
#include <jtrace-internal.h>

struct JBackendFile
{
	gchar* path;
	GFileIOStream* stream;
};

typedef struct JBackendFile JBackendFile;

static gchar* jd_backend_path = NULL;

static
gboolean
backend_create (gchar const* namespace, gchar const* path, gpointer* data)
{
	JBackendFile* bf;
	GFile* file;
	GFile* parent;
	GFileIOStream* stream;
	gchar* full_path;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);

	parent = g_file_get_parent(file);
	g_file_make_directory_with_parents(parent, NULL, NULL);
	g_object_unref(parent);

	stream = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, NULL);

	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	bf = g_slice_new(JBackendFile);
	bf->path = full_path;
	bf->stream = stream;

	*data = bf;

	g_object_unref(file);

	return (stream != NULL);
}

static
gboolean
backend_open (gchar const* namespace, gchar const* path, gpointer* data)
{
	JBackendFile* bf;
	GFile* file;
	GFileIOStream* stream;
	gchar* full_path;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	stream = g_file_open_readwrite(file, NULL, NULL);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	bf = g_slice_new(JBackendFile);
	bf->path = full_path;
	bf->stream = stream;

	*data = bf;

	g_object_unref(file);

	return (stream != NULL);
}

static
gboolean
backend_delete (gpointer data)
{
	JBackendFile* bf = data;
	gboolean ret;

	GFile* file;

	file = g_file_new_for_path(bf->path);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = g_file_delete(file, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_object_unref(file);

	g_object_unref(bf->stream);
	g_free(bf->path);
	g_slice_free(JBackendFile, bf);

	return ret;
}

static
gboolean
backend_close (gpointer data)
{
	JBackendFile* bf = data;
	gboolean ret;

	j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
	ret = g_io_stream_close(G_IO_STREAM(bf->stream), NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_object_unref(bf->stream);
	g_free(bf->path);
	g_slice_free(JBackendFile, bf);

	return ret;
}

static
gboolean
backend_status (gpointer data, gint64* modification_time, guint64* size)
{
	JBackendFile* bf = data;
	gboolean ret;

	//output = g_io_stream_get_output_stream(G_IO_STREAM(stream));

	j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
	// FIXME
	//g_output_stream_flush(output, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);

	// FIXME
	ret = TRUE;

	if (modification_time != NULL)
	{
		*modification_time = 0;
	}

	if (size != NULL)
	{
		*size = 0;
	}

	return ret;
}

static
gboolean
backend_sync (gpointer data)
{
	JBackendFile* bf = data;
	gboolean ret;

	GOutputStream* output;

	output = g_io_stream_get_output_stream(G_IO_STREAM(bf->stream));

	j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
	ret = g_output_stream_flush(output, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);

	return ret;
}

static
gboolean
backend_read (gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	JBackendFile* bf = data;
	gboolean ret;

	GInputStream* input;
	gsize nbytes;

	input = g_io_stream_get_input_stream(G_IO_STREAM(bf->stream));

	j_trace_file_begin(bf->path, J_TRACE_FILE_SEEK);
	g_seekable_seek(G_SEEKABLE(bf->stream), offset, G_SEEK_SET, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_SEEK, 0, offset);

	j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
	ret = g_input_stream_read_all(input, buffer, length, &nbytes, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_READ, nbytes, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = nbytes;
	}

	return ret;
}

static
gboolean
backend_write (gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	JBackendFile* bf = data;
	gboolean ret;

	GOutputStream* output;
	gsize nbytes;

	output = g_io_stream_get_output_stream(G_IO_STREAM(bf->stream));

	j_trace_file_begin(bf->path, J_TRACE_FILE_SEEK);
	g_seekable_seek(G_SEEKABLE(bf->stream), offset, G_SEEK_SET, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_SEEK, 0, offset);

	j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
	ret = g_output_stream_write_all(output, buffer, length, &nbytes, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, nbytes, offset);

	if (bytes_written != NULL)
	{
		*bytes_written = nbytes;
	}

	return ret;
}

static
gboolean
backend_init (gchar const* path)
{
	GFile* file;

	jd_backend_path = g_strdup(path);

	file = g_file_new_for_path(path);
	g_file_make_directory_with_parents(file, NULL, NULL);
	g_object_unref(file);

	return TRUE;
}

static
void
backend_fini (void)
{
	g_free(jd_backend_path);
}

static
JBackend gio_backend = {
	.type = J_BACKEND_TYPE_OBJECT,
	.component = J_BACKEND_COMPONENT_SERVER,
	.object = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_create = backend_create,
		.backend_delete = backend_delete,
		.backend_open = backend_open,
		.backend_close = backend_close,
		.backend_status = backend_status,
		.backend_sync = backend_sync,
		.backend_read = backend_read,
		.backend_write = backend_write
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &gio_backend;
}
