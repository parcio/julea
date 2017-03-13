/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include <jbackend.h>
#include <jtrace-internal.h>

static gchar* jd_backend_path = NULL;

static
gboolean
backend_create (JBackendItem* bf, gchar const* namespace, gchar const* path)
{
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

	bf->path = full_path;
	bf->user_data = stream;

	g_object_unref(file);

	return (stream != NULL);
}

static
gboolean
backend_delete (JBackendItem* bf)
{
	gboolean ret = FALSE;
	GFile* file;

	file = g_file_new_for_path(bf->path);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = g_file_delete(file, NULL, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_object_unref(file);

	return ret;
}

static
gboolean
backend_open (JBackendItem* bf, gchar const* namespace, gchar const* path)
{
	GFile* file;
	GFileIOStream* stream;
	gchar* full_path;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	stream = g_file_open_readwrite(file, NULL, NULL);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	bf->path = full_path;
	bf->user_data = stream;

	g_object_unref(file);

	return (stream != NULL);
}

static
gboolean
backend_close (JBackendItem* bf)
{
	GFileIOStream* stream = bf->user_data;

	if (stream != NULL)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
		g_io_stream_close(G_IO_STREAM(stream), NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);
	}

	g_object_unref(stream);

	g_free(bf->path);

	return (stream != NULL);
}

static
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size)
{
	GFileIOStream* stream = bf->user_data;
	//GOutputStream* output;

	(void)flags;

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

	return (stream != NULL);
}

static
gboolean
backend_sync (JBackendItem* bf)
{
	GFileIOStream* stream = bf->user_data;
	GOutputStream* output;

	if (stream != NULL)
	{
		output = g_io_stream_get_output_stream(G_IO_STREAM(stream));

		j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
		g_output_stream_flush(output, NULL, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);
	}

	return (stream != NULL);
}

static
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	GFileIOStream* stream = bf->user_data;
	GInputStream* input;
	gsize nbytes;

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

	return (stream != NULL);
}

static
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	GFileIOStream* stream = bf->user_data;
	GOutputStream* output;
	gsize nbytes;

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

	return (stream != NULL);
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
	.type = J_BACKEND_TYPE_DATA,
	.u.data = {
		.init = backend_init,
		.fini = backend_fini,
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
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	if (type == J_BACKEND_TYPE_DATA)
	{
		backend = &gio_backend;
	}

	return backend;
}
