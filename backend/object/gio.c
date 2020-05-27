/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

struct JBackendData
{
	gchar* path;
};

typedef struct JBackendData JBackendData;

struct JBackendObject
{
	gchar* path;
	GFileIOStream* stream;
};

typedef struct JBackendObject JBackendObject;

static gboolean
backend_create(gpointer backend_data, gchar const* namespace, gchar const* path, gpointer* backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo;
	GFile* file;
	GFile* parent;
	GFileIOStream* stream;
	gchar* full_path;

	full_path = g_build_filename(bd->path, namespace, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);

	parent = g_file_get_parent(file);
	g_file_make_directory_with_parents(parent, NULL, NULL);
	g_object_unref(parent);

	stream = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, NULL);

	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	bo = g_slice_new(JBackendObject);
	bo->path = full_path;
	bo->stream = stream;

	*backend_object = bo;

	g_object_unref(file);

	return (stream != NULL);
}

static gboolean
backend_open(gpointer backend_data, gchar const* namespace, gchar const* path, gpointer* backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo;
	GFile* file;
	GFileIOStream* stream;
	gchar* full_path;

	full_path = g_build_filename(bd->path, namespace, path, NULL);
	file = g_file_new_for_path(full_path);

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	stream = g_file_open_readwrite(file, NULL, NULL);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	bo = g_slice_new(JBackendObject);
	bo->path = full_path;
	bo->stream = stream;

	*backend_object = bo;

	g_object_unref(file);

	return (stream != NULL);
}

static gboolean
backend_delete(gpointer backend_data, gpointer backend_object)
{
	JBackendObject* bo = backend_object;
	gboolean ret;
	GFile* file;

	(void)backend_data;

	file = g_file_new_for_path(bo->path);

	j_trace_file_begin(bo->path, J_TRACE_FILE_DELETE);
	ret = g_file_delete(file, NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_DELETE, 0, 0);

	g_object_unref(file);

	g_object_unref(bo->stream);
	g_free(bo->path);
	g_slice_free(JBackendObject, bo);

	return ret;
}

static gboolean
backend_close(gpointer backend_data, gpointer backend_object)
{
	JBackendObject* bo = backend_object;
	gboolean ret;

	(void)backend_data;

	j_trace_file_begin(bo->path, J_TRACE_FILE_CLOSE);
	ret = g_io_stream_close(G_IO_STREAM(bo->stream), NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_object_unref(bo->stream);
	g_free(bo->path);
	g_slice_free(JBackendObject, bo);

	return ret;
}

static gboolean
backend_status(gpointer backend_data, gpointer backend_object, gint64* modification_time, guint64* size)
{
	JBackendObject* bo = backend_object;
	gboolean ret = TRUE;

	(void)backend_data;

	if (modification_time != NULL || size != NULL)
	{
		GFile* file;
		GFileInfo* file_info;

		file = g_file_new_for_path(bo->path);

		j_trace_file_begin(bo->path, J_TRACE_FILE_STATUS);
		file_info = g_file_query_info(file, G_FILE_ATTRIBUTE_STANDARD_SIZE "," G_FILE_ATTRIBUTE_TIME_MODIFIED "," G_FILE_ATTRIBUTE_TIME_MODIFIED_USEC, G_FILE_QUERY_INFO_NONE, NULL, NULL);
		j_trace_file_end(bo->path, J_TRACE_FILE_STATUS, 0, 0);

		ret = (file_info != NULL);

		if (modification_time != NULL)
		{
#if GLIB_CHECK_VERSION(2, 62, 0)
			GDateTime* date_time;

			date_time = g_file_info_get_modification_date_time(file_info);

			if (date_time != NULL)
			{
				*modification_time = g_date_time_to_unix(date_time) * G_USEC_PER_SEC + g_date_time_get_microsecond(date_time);
				g_date_time_unref(date_time);
			}
#else
			GTimeVal time_val;

			g_file_info_get_modification_time(file_info, &time_val);
			*modification_time = time_val.tv_sec * G_USEC_PER_SEC + time_val.tv_usec;
#endif
		}

		if (size != NULL)
		{
			*size = g_file_info_get_size(file_info);
		}

		g_object_unref(file_info);
		g_object_unref(file);
	}

	return ret;
}

static gboolean
backend_sync(gpointer backend_data, gpointer backend_object)
{
	JBackendObject* bo = backend_object;
	gboolean ret;
	GOutputStream* output;

	(void)backend_data;

	output = g_io_stream_get_output_stream(G_IO_STREAM(bo->stream));

	j_trace_file_begin(bo->path, J_TRACE_FILE_SYNC);
	ret = g_output_stream_flush(output, NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_SYNC, 0, 0);

	return ret;
}

static gboolean
backend_read(gpointer backend_data, gpointer backend_object, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	JBackendObject* bo = backend_object;
	gboolean ret;

	GInputStream* input;
	gsize nbytes;

	(void)backend_data;

	input = g_io_stream_get_input_stream(G_IO_STREAM(bo->stream));

	j_trace_file_begin(bo->path, J_TRACE_FILE_SEEK);
	g_seekable_seek(G_SEEKABLE(bo->stream), offset, G_SEEK_SET, NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_SEEK, 0, offset);

	j_trace_file_begin(bo->path, J_TRACE_FILE_READ);
	ret = g_input_stream_read_all(input, buffer, length, &nbytes, NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_READ, nbytes, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = nbytes;
	}

	return ret;
}

static gboolean
backend_write(gpointer backend_data, gpointer backend_object, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	JBackendObject* bo = backend_object;
	gboolean ret;

	GOutputStream* output;
	gsize nbytes;

	(void)backend_data;

	output = g_io_stream_get_output_stream(G_IO_STREAM(bo->stream));

	j_trace_file_begin(bo->path, J_TRACE_FILE_SEEK);
	g_seekable_seek(G_SEEKABLE(bo->stream), offset, G_SEEK_SET, NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_SEEK, 0, offset);

	j_trace_file_begin(bo->path, J_TRACE_FILE_WRITE);
	ret = g_output_stream_write_all(output, buffer, length, &nbytes, NULL, NULL);
	j_trace_file_end(bo->path, J_TRACE_FILE_WRITE, nbytes, offset);

	if (bytes_written != NULL)
	{
		*bytes_written = nbytes;
	}

	return ret;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	JBackendData* bd;
	GFile* file;

	bd = g_slice_new(JBackendData);
	bd->path = g_strdup(path);

	file = g_file_new_for_path(path);
	g_file_make_directory_with_parents(file, NULL, NULL);
	g_object_unref(file);

	*backend_data = bd;

	return TRUE;
}

static void
backend_fini(gpointer backend_data)
{
	JBackendData* bd = backend_data;

	g_free(bd->path);
	g_slice_free(JBackendData, bd);
}

static JBackend gio_backend = {
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
		.backend_write = backend_write }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &gio_backend;
}
