/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Lars Thoms
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

#include <rados/librados.h>

#include <julea.h>

struct JBackendData
{
	/* Initialize cluster and config variables */
	rados_t backend_connection;
	rados_ioctx_t backend_io;

	gchar* backend_pool;
	gchar* backend_config;
};

typedef struct JBackendData JBackendData;



struct JBackendFile
{
	gchar* path;
};

typedef struct JBackendFile JBackendFile;

static
gboolean
backend_create (gpointer bData, gchar const* namespace, gchar const* path, gpointer* data)
{
	JBackendFile* bf;
	JBackendData* backendData = bData;
	gchar* full_path = g_strconcat(namespace, path, NULL);
	gint ret = 0;

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);
	ret = rados_write_full(backendData->backend_io, full_path, "", 0);
	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	g_return_val_if_fail(ret == 0, FALSE);

	bf = g_slice_new(JBackendFile);
	bf->path = full_path;

	*data = bf;

	return TRUE;
}

static
gboolean
backend_open (gpointer bData, gchar const* namespace, gchar const* path, gpointer* data)
{
	JBackendFile* bf;
	gchar* full_path = g_strconcat(namespace, path, NULL);
	gint ret = 0;

	(void)bData;

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	g_return_val_if_fail(ret == 0, FALSE);

	bf = g_slice_new(JBackendFile);
	bf->path = full_path;

	*data = bf;

	return TRUE;
}

static
gboolean
backend_delete (gpointer bData, gpointer data)
{
	JBackendData* backendData = bData;
	JBackendFile* bf = data;
	gint ret = 0;

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = rados_remove(backendData->backend_io, bf->path);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_free(bf->path);
	g_slice_free(JBackendFile, bf);

	return (ret == 0 ? TRUE : FALSE);
}

static
gboolean
backend_close (gpointer bData, gpointer data)
{
	JBackendFile* bf = data;

	(void)bData;

	j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(bf->path);
	g_slice_free(JBackendFile, bf);

	return TRUE;
}

static
gboolean
backend_status (gpointer bData, gpointer data, gint64* modification_time, guint64* size)
{
	JBackendData* backendData = bData;
	JBackendFile* bf = data;
	gboolean ret = TRUE;
	gint64 modification_time_ = 0;
	guint64 size_ = 0;

	if (modification_time != NULL || size != NULL)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
		ret = (rados_stat(backendData->backend_io, bf->path, &size_, &modification_time_) == 0);
		j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);

		if (ret && modification_time != NULL)
		{
			*modification_time = modification_time_;
		}

		if (ret && size != NULL)
		{
			*size = size_;
		}
	}

	return ret;
}

/* Not implemented */
static
gboolean
backend_sync (gpointer bData, gpointer data)
{
	JBackendFile* bf = data;

	(void)bData;

	j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
	j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);

	return TRUE;
}

static
gboolean
backend_read (gpointer bData, gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	JBackendData* backendData = bData;
	JBackendFile* bf = data;
	gint ret = 0;

	j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
	ret = rados_read(backendData->backend_io, bf->path, buffer, length, offset);
	j_trace_file_end(bf->path, J_TRACE_FILE_READ, length, offset);

	g_return_val_if_fail(ret >= 0, FALSE);

	if (bytes_read != NULL)
	{
		*bytes_read = ret;
	}

	return TRUE;
}

static
gboolean
backend_write (gpointer bData, gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	JBackendData* backendData = bData;
	JBackendFile* bf = data;
	gint ret = 0;

	j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
	ret = rados_write(backendData->backend_io, bf->path, buffer, length, offset);
	j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, length, offset);

	g_return_val_if_fail(ret == 0, FALSE);

	if (bytes_written != NULL)
	{
		*bytes_written = length;
	}

	return TRUE;
}

static
gboolean
backend_init (gpointer* bData, gchar const* path)
{
	JBackendData* backendData = g_slice_new(JBackendData);
	g_auto(GStrv) split = NULL;

	g_return_val_if_fail(path != NULL, FALSE);

	/* Path syntax: [config-path]:[pool]
	   e.g.: /etc/ceph/ceph.conf:data */
	split = g_strsplit(path, ":", 0);
	backendData->backend_config = g_strdup(split[0]);
	backendData->backend_pool = g_strdup(split[1]);

	g_return_val_if_fail(backendData->backend_pool != NULL, FALSE);
	g_return_val_if_fail(backendData->backend_config != NULL, FALSE);

	/* Create cluster handle */
	if (rados_create(&backendData->backend_connection, NULL) != 0)
	{
		g_critical("Can not create a RADOS cluster handle.");
	}

	/* Read config file */
	if (rados_conf_read_file(backendData->backend_connection, backendData->backend_config) != 0)
	{
		g_critical("Can not read RADOS config file %s.", backendData->backend_config);
	}

	/* Connect to cluster */
	if (rados_connect(backendData->backend_connection) != 0)
	{
		g_critical("Can not connect to RADOS. Cluster online, config up-to-date and keyring correct linked?");
	}

	/* Initialize IO and select pool */
	if (rados_ioctx_create(backendData->backend_connection, backendData->backend_pool, &backendData->backend_io) != 0)
	{
		rados_shutdown(backendData->backend_connection);
		g_critical("Can not connect to RADOS pool %s.", backendData->backend_pool);
	}

	*bData = backendData;

	return TRUE;
}

static
void
backend_fini (gpointer bData)
{
	JBackendData* backendData = bData;

	/* Close connection to cluster */
	rados_ioctx_destroy(backendData->backend_io);
	rados_shutdown(backendData->backend_connection);

	/* Free memory */
	g_free(backendData->backend_config);
	g_free(backendData->backend_pool);
	g_free(backendData);
}

static
JBackend rados_backend = {
	.type = J_BACKEND_TYPE_OBJECT,
	.component = J_BACKEND_COMPONENT_CLIENT,
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
	return &rados_backend;
}
