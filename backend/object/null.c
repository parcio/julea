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
#include <gmodule.h>

#include <julea.h>

static
gboolean
backend_create (gchar const* namespace, gchar const* path, gpointer* data)
{
	gchar* full_path;

	full_path = g_build_filename(namespace, path, NULL);

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);
	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	*data = full_path;

	return TRUE;
}

static
gboolean
backend_open (gchar const* namespace, gchar const* path, gpointer* data)
{
	gchar* full_path;

	full_path = g_build_filename(namespace, path, NULL);

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	*data = full_path;

	return TRUE;
}

static
gboolean
backend_delete (gpointer data)
{
	gchar* full_path = data;

	j_trace_file_begin(full_path, J_TRACE_FILE_DELETE);
	j_trace_file_end(full_path, J_TRACE_FILE_DELETE, 0, 0);

	g_free(full_path);

	return TRUE;
}

static
gboolean
backend_close (gpointer data)
{
	gchar* full_path = data;

	j_trace_file_begin(full_path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(full_path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(full_path);

	return TRUE;
}

static
gboolean
backend_status (gpointer data, gint64* modification_time, guint64* size)
{
	gchar const* full_path = data;

	j_trace_file_begin(full_path, J_TRACE_FILE_STATUS);
	j_trace_file_end(full_path, J_TRACE_FILE_STATUS, 0, 0);

	if (modification_time != NULL)
	{
		*modification_time = 0;
	}

	if (size != NULL)
	{
		*size = 0;
	}

	return TRUE;
}

static
gboolean
backend_sync (gpointer data)
{
	gchar const* full_path = data;

	j_trace_file_begin(full_path, J_TRACE_FILE_SYNC);
	j_trace_file_end(full_path, J_TRACE_FILE_SYNC, 0, 0);

	return TRUE;
}

static
gboolean
backend_read (gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	gchar const* full_path = data;

	(void)buffer;

	j_trace_file_begin(full_path, J_TRACE_FILE_READ);
	j_trace_file_end(full_path, J_TRACE_FILE_READ, length, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = length;
	}

	return TRUE;
}

static
gboolean
backend_write (gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	gchar const* full_path = data;

	(void)buffer;

	j_trace_file_begin(full_path, J_TRACE_FILE_WRITE);
	j_trace_file_end(full_path, J_TRACE_FILE_WRITE, length, offset);

	if (bytes_written != NULL)
	{
		*bytes_written = length;
	}

	return TRUE;
}

static
gboolean
backend_init (gchar const* path)
{
	(void)path;

	return TRUE;
}

static
void
backend_fini (void)
{
}

static
JBackend null_backend = {
	.type = J_BACKEND_TYPE_OBJECT,
	.component = J_BACKEND_COMPONENT_CLIENT | J_BACKEND_COMPONENT_SERVER,
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
	return &null_backend;
}
