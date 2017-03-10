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
#include <gmodule.h>

#include <jbackend.h>
#include <jtrace-internal.h>

static
gboolean
backend_create (JBackendItem* bf, gchar const* path, gpointer data)
{
	(void)data;

	bf->path = g_strdup(path);
	bf->user_data = NULL;

	j_trace_file_begin(bf->path, J_TRACE_FILE_CREATE);
	j_trace_file_end(bf->path, J_TRACE_FILE_CREATE, 0, 0);

	return TRUE;
}

static
gboolean
backend_delete (JBackendItem* bf, gpointer data)
{
	(void)data;

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	return TRUE;
}

static
gboolean
backend_open (JBackendItem* bf, gchar const* path, gpointer data)
{
	(void)data;

	bf->path = g_strdup(path);
	bf->user_data = NULL;

	j_trace_file_begin(bf->path, J_TRACE_FILE_OPEN);
	j_trace_file_end(bf->path, J_TRACE_FILE_OPEN, 0, 0);

	return TRUE;
}

static
gboolean
backend_close (JBackendItem* bf, gpointer data)
{
	(void)data;

	j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(bf->path);

	return TRUE;
}

static
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size, gpointer data)
{
	(void)flags;
	(void)data;

	j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
	j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);

	*modification_time = 0;
	*size = 0;

	return TRUE;
}

static
gboolean
backend_sync (JBackendItem* bf, gpointer data)
{
	(void)data;

	j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
	j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);

	return TRUE;
}

static
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read, gpointer data)
{
	(void)buffer;
	(void)data;

	j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
	j_trace_file_end(bf->path, J_TRACE_FILE_READ, length, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = length;
	}

	return TRUE;
}

static
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written, gpointer data)
{
	(void)buffer;
	(void)data;

	j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
	j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, length, offset);

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
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	if (type == J_BACKEND_TYPE_DATA)
	{
		backend = &null_backend;
	}

	return backend;
}
