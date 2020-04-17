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
	rados_t backend_connection;
	rados_ioctx_t backend_io;

	gchar* backend_pool;
	gchar* backend_config;
};

typedef struct JBackendData JBackendData;

struct JBackendObject
{
	gchar* path;
};

typedef struct JBackendObject JBackendObject;

static gboolean
backend_create(gpointer backend_data, gchar const* namespace, gchar const* path, gpointer* backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo;
	gchar* full_path = g_strconcat(namespace, path, NULL);
	gint ret = 0;

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);
	ret = rados_write_full(bd->backend_io, full_path, "", 0);
	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	g_return_val_if_fail(ret == 0, FALSE);

	bo = g_slice_new(JBackendObject);
	bo->path = full_path;

	*backend_object = bo;

	return TRUE;
}

static gboolean
backend_open(gpointer backend_data, gchar const* namespace, gchar const* path, gpointer* backend_object)
{
	JBackendObject* bo;
	gchar* full_path = g_strconcat(namespace, path, NULL);
	gint ret = 0;

	(void)backend_data;

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	g_return_val_if_fail(ret == 0, FALSE);

	bo = g_slice_new(JBackendObject);
	bo->path = full_path;

	*backend_object = bo;

	return TRUE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo = backend_object;
	gint ret = 0;

	j_trace_file_begin(bo->path, J_TRACE_FILE_DELETE);
	ret = rados_remove(bd->backend_io, bo->path);
	j_trace_file_end(bo->path, J_TRACE_FILE_DELETE, 0, 0);

	g_free(bo->path);
	g_slice_free(JBackendObject, bo);

	return (ret == 0 ? TRUE : FALSE);
}

static gboolean
backend_close(gpointer backend_data, gpointer backend_object)
{
	JBackendObject* bo = backend_object;

	(void)backend_data;

	j_trace_file_begin(bo->path, J_TRACE_FILE_CLOSE);
	j_trace_file_end(bo->path, J_TRACE_FILE_CLOSE, 0, 0);

	g_free(bo->path);
	g_slice_free(JBackendObject, bo);

	return TRUE;
}

static gboolean
backend_status(gpointer backend_data, gpointer backend_object, gint64* modification_time, guint64* size)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo = backend_object;
	gboolean ret = TRUE;
	gint64 modification_time_ = 0;
	guint64 size_ = 0;

	if (modification_time != NULL || size != NULL)
	{
		j_trace_file_begin(bo->path, J_TRACE_FILE_STATUS);
		ret = (rados_stat(bd->backend_io, bo->path, &size_, &modification_time_) == 0);
		j_trace_file_end(bo->path, J_TRACE_FILE_STATUS, 0, 0);

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
static gboolean
backend_sync(gpointer backend_data, gpointer backend_object)
{
	JBackendObject* bo = backend_object;

	(void)backend_data;

	j_trace_file_begin(bo->path, J_TRACE_FILE_SYNC);
	j_trace_file_end(bo->path, J_TRACE_FILE_SYNC, 0, 0);

	return TRUE;
}

static gboolean
backend_read(gpointer backend_data, gpointer backend_object, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo = backend_object;
	gint ret = 0;

	j_trace_file_begin(bo->path, J_TRACE_FILE_READ);
	ret = rados_read(bd->backend_io, bo->path, buffer, length, offset);
	j_trace_file_end(bo->path, J_TRACE_FILE_READ, length, offset);

	g_return_val_if_fail(ret >= 0, FALSE);

	if (bytes_read != NULL)
	{
		*bytes_read = ret;
	}

	return TRUE;
}

static gboolean
backend_write(gpointer backend_data, gpointer backend_object, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo = backend_object;
	gint ret = 0;

	j_trace_file_begin(bo->path, J_TRACE_FILE_WRITE);
	ret = rados_write(bd->backend_io, bo->path, buffer, length, offset);
	j_trace_file_end(bo->path, J_TRACE_FILE_WRITE, length, offset);

	g_return_val_if_fail(ret == 0, FALSE);

	if (bytes_written != NULL)
	{
		*bytes_written = length;
	}

	return TRUE;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	JBackendData* bd;
	g_auto(GStrv) split = NULL;

	g_return_val_if_fail(path != NULL, FALSE);

	/* Path syntax: [config-path]:[pool]
	   e.g.: /etc/ceph/ceph.conf:data */
	split = g_strsplit(path, ":", 0);

	bd = g_slice_new(JBackendData);
	bd->backend_config = g_strdup(split[0]);
	bd->backend_pool = g_strdup(split[1]);
	bd->backend_connection = NULL;
	bd->backend_io = NULL;

	g_return_val_if_fail(bd->backend_pool != NULL, FALSE);
	g_return_val_if_fail(bd->backend_config != NULL, FALSE);

	/* Create cluster handle */
	if (rados_create(&(bd->backend_connection), NULL) != 0)
	{
		g_critical("Can not create a RADOS cluster handle.");
	}

	/* Read config file */
	if (rados_conf_read_file(bd->backend_connection, bd->backend_config) != 0)
	{
		g_critical("Can not read RADOS config file %s.", bd->backend_config);
	}

	/* Connect to cluster */
	if (rados_connect(bd->backend_connection) != 0)
	{
		g_critical("Can not connect to RADOS. Cluster online, config up-to-date and keyring correct linked?");
	}

	/* Initialize IO and select pool */
	if (rados_ioctx_create(bd->backend_connection, bd->backend_pool, &(bd->backend_io)) != 0)
	{
		rados_shutdown(bd->backend_connection);
		g_critical("Can not connect to RADOS pool %s.", bd->backend_pool);
	}

	*backend_data = bd;

	return TRUE;
}

static void
backend_fini(gpointer backend_data)
{
	JBackendData* bd = backend_data;

	/* Close connection to cluster */
	rados_ioctx_destroy(bd->backend_io);
	rados_shutdown(bd->backend_connection);

	/* Free memory */
	g_free(bd->backend_config);
	g_free(bd->backend_pool);
	g_slice_free(JBackendData, bd);
}

static JBackend rados_backend = {
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
		.backend_write = backend_write }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &rados_backend;
}
