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

struct JBackendIterator
{
	rados_list_ctx_t rados_list;
	gchar* prefix;
};

typedef struct JBackendIterator JBackendIterator;

struct JBackendObject
{
	// This is a concatenation of the namespace and name of an object separated by a whitespace.
	// Ideally, these would get stored as two separate strings and used with rados's `set_namespace` function.
	// However, this can cause problems when multiple threads are attempting to set the namespace at the same time.
	// This could be managed by having an independent `rados_ioctx_t` for each thread but for now this implementation will do.
	gchar* namespace_and_name;
};

typedef struct JBackendObject JBackendObject;

static gboolean
backend_create(const gpointer backend_data, gchar const* namespace, gchar const* name, gpointer* backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo;
	gint ret = 0;
	gchar* namespace_and_name;

	g_return_val_if_fail(backend_data != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);

	namespace_and_name = g_strconcat(namespace, " ", name, NULL);

	j_trace_file_begin(namespace_and_name, J_TRACE_FILE_CREATE);
	ret = rados_write_full(bd->backend_io, namespace_and_name, "", 0);
	j_trace_file_end(namespace_and_name, J_TRACE_FILE_CREATE, 0, 0);

	g_return_val_if_fail(ret == 0, FALSE);

	bo = g_new(JBackendObject, 1);
	bo->namespace_and_name = namespace_and_name;

	*backend_object = bo;

	return TRUE;
}

static gboolean
backend_open(gpointer backend_data, gchar const* namespace, gchar const* name, gpointer* backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo;
	gint ret = 0;
	gchar* namespace_and_name;

	g_return_val_if_fail(bd != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);

	namespace_and_name = g_strconcat(namespace, " ", name, NULL);

	j_trace_file_begin(namespace_and_name, J_TRACE_FILE_OPEN);
	ret = rados_stat(bd->backend_io, namespace_and_name, NULL, NULL);
	j_trace_file_end(namespace_and_name, J_TRACE_FILE_OPEN, 0, 0);

	if (ret != 0)
	{
		// ENOENT (Error NO ENTry)
		// It is an expected "error" here because we are using the stat command to check if the object exists.
		// This is done to ensure that the object was created before opening it, just as it has to be done with other APIs.
		// As such, we only print other error kinds.
		if (ret != -ENOENT)
		{
			g_critical("rados_stat() failed: %s", strerror(-ret));
		}
		// In either case, we return false as we obviously weren't able to `open` to object :)
		return FALSE;
	}

	bo = g_new(JBackendObject, 1);
	bo->namespace_and_name = namespace_and_name;

	*backend_object = bo;

	return TRUE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer backend_object)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo = backend_object;
	gint ret = 0;

	g_return_val_if_fail(bd != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);
	g_return_val_if_fail(bo != NULL, FALSE);
	g_return_val_if_fail(bo->namespace_and_name != NULL, FALSE);

	j_trace_file_begin(bo->namespace_and_name, J_TRACE_FILE_DELETE);
	ret = rados_remove(bd->backend_io, bo->namespace_and_name);
	j_trace_file_end(bo->namespace_and_name, J_TRACE_FILE_DELETE, 0, 0);

	g_clear_pointer(&bo->namespace_and_name, g_free);
	g_free(bo);

	return (ret == 0 ? TRUE : FALSE);
}

static gboolean
backend_close(gpointer backend_data, gpointer backend_object)
{
	JBackendObject* bo = backend_object;

	(void)backend_data;

	j_trace_file_begin(bo->namespace_and_name, J_TRACE_FILE_CLOSE);
	j_trace_file_end(bo->namespace_and_name, J_TRACE_FILE_CLOSE, 0, 0);

	g_clear_pointer(&bo->namespace_and_name, g_free);
	g_free(bo);

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

	g_return_val_if_fail(bd != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);
	g_return_val_if_fail(bo != NULL, FALSE);
	g_return_val_if_fail(bo->namespace_and_name != NULL, FALSE);

	if (modification_time != NULL || size != NULL)
	{
		j_trace_file_begin(bo->namespace_and_name, J_TRACE_FILE_STATUS);
		ret = (rados_stat(bd->backend_io, bo->namespace_and_name, &size_, &modification_time_) == 0);
		j_trace_file_end(bo->namespace_and_name, J_TRACE_FILE_STATUS, 0, 0);

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

	j_trace_file_begin(bo->namespace_and_name, J_TRACE_FILE_SYNC);
	j_trace_file_end(bo->namespace_and_name, J_TRACE_FILE_SYNC, 0, 0);

	return TRUE;
}

static gboolean
backend_read(gpointer backend_data, gpointer backend_object, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	JBackendData* bd = backend_data;
	JBackendObject* bo = backend_object;
	gint ret = 0;

	g_return_val_if_fail(bd != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);
	g_return_val_if_fail(bo != NULL, FALSE);
	g_return_val_if_fail(bo->namespace_and_name != NULL, FALSE);

	j_trace_file_begin(bo->namespace_and_name, J_TRACE_FILE_READ);
	ret = rados_read(bd->backend_io, bo->namespace_and_name, buffer, length, offset);
	j_trace_file_end(bo->namespace_and_name, J_TRACE_FILE_READ, length, offset);

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

	g_return_val_if_fail(bd != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);
	g_return_val_if_fail(bo != NULL, FALSE);
	g_return_val_if_fail(bo->namespace_and_name != NULL, FALSE);

	if (length == 0)
	{
		if (bytes_written != NULL)
		{
			*bytes_written = 0;
		}
		return TRUE;
	}

	g_return_val_if_fail(buffer != NULL, FALSE);

	j_trace_file_begin(bo->namespace_and_name, J_TRACE_FILE_WRITE);
	ret = rados_write(bd->backend_io, bo->namespace_and_name, buffer, length, offset);
	j_trace_file_end(bo->namespace_and_name, J_TRACE_FILE_WRITE, length, offset);

	g_return_val_if_fail(ret == 0, FALSE);

	if (bytes_written != NULL)
	{
		*bytes_written = length;
	}

	return TRUE;
}

static gboolean
backend_get_all(gpointer backend_data, gchar const* namespace, gpointer* backend_iterator)
{
	JBackendData* bd = backend_data;
	JBackendIterator* iterator = g_new(JBackendIterator, 1);
	int err;

	g_return_val_if_fail(backend_data != NULL, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(bd->backend_io != NULL, FALSE);

	iterator->prefix = g_strdup(namespace);

	err = rados_nobjects_list_open(bd->backend_io, &iterator->rados_list);
	g_return_val_if_fail(err == 0, FALSE);

	*backend_iterator = iterator;
	return TRUE;
}

static gboolean
backend_get_by_prefix(gpointer backend_data, gchar const* namespace, gchar const* prefix, gpointer* backend_iterator)
{
	const JBackendData* bd = backend_data;
	JBackendIterator* iterator = g_new(JBackendIterator, 1);
	int err;

	g_return_val_if_fail(backend_data != NULL, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);

	iterator->prefix = g_strconcat(namespace, " ", prefix, NULL);

	err = rados_nobjects_list_open(bd->backend_io, &iterator->rados_list);
	g_return_val_if_fail(err == 0, FALSE);

	*backend_iterator = iterator;
	return TRUE;
}

static gboolean
backend_iterate(gpointer backend_data, gpointer backend_iterator, gchar const** name)
{
	JBackendIterator* iterator = backend_iterator;
	gchar const* current_name;
	gchar** split_namespace_and_name;

	// Avoid unused parameter warning :)
	(void)backend_data;

	// Loop until we have an object that matches the prefix
	do
	{
		const int err = rados_nobjects_list_next(iterator->rados_list, &current_name, NULL, NULL);

		if (err == -ENOENT)
		{
			goto end; // No more objects
		}
		if (err != 0) // Something went wrong
		{
			g_error("rados_nobjects_list_next() failed: %s", strerror(-err));
		}
	} while (!g_str_has_prefix(current_name, iterator->prefix));

	// Separate the current_name and only return the name part.
	split_namespace_and_name = g_strsplit(current_name, " ", 2);
	*name = g_strdup(split_namespace_and_name[1]);
	g_strfreev(split_namespace_and_name);
	return TRUE;
end:
	rados_nobjects_list_close(iterator->rados_list);
	g_free(iterator->prefix);
	g_free(iterator);
	return FALSE;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	JBackendData* bd = NULL;
	g_auto(GStrv) split = NULL;
	int err = 0;

	g_return_val_if_fail(path != NULL, FALSE);

	/* Path syntax: [config-path]:[pool]
	   e.g.: /etc/ceph/ceph.conf:data */
	split = g_strsplit(path, ":", 0);
	/* Expect at least two parts */
	if (split == NULL || split[0] == NULL || split[1] == NULL)
	{
		g_critical("Invalid RADOS backend path '%s'. Expected format: <config>:<pool>", path);
		return FALSE;
	}

	bd = g_new0(JBackendData, 1);
	bd->backend_config = g_strdup(split[0]);
	bd->backend_pool = g_strdup(split[1]);

	g_return_val_if_fail(bd->backend_pool != NULL, FALSE);
	g_return_val_if_fail(bd->backend_config != NULL, FALSE);

	/* Create cluster handle */
	err = rados_create(&(bd->backend_connection), NULL);
	if (err != 0)
	{
		g_critical("Can not create a RADOS cluster handle. Error message:\n%s\n", strerror(-err));
		return FALSE;
	}

	/* Read config file */
	err = rados_conf_read_file(bd->backend_connection, bd->backend_config);
	if (err != 0)
	{
		g_critical("Can not read RADOS config file `%s`. Error message:\n%s\n", bd->backend_config, strerror(-err));
		return FALSE;
	}

	/* Connect to cluster */
	err = rados_connect(bd->backend_connection);
	if (err != 0)
	{
		g_critical("Can not connect to RADOS. Cluster online, config up-to-date and keyring correct linked? Error message:\n%s\n", strerror(-err));
		return FALSE;
	}

	/* Initialize IO and select pool */
	err = rados_ioctx_create(bd->backend_connection, bd->backend_pool, &(bd->backend_io));
	if (err != 0)
	{
		g_critical("Can not connect to RADOS pool %s. Error message:\n%s\n", bd->backend_pool, strerror(-err));
		rados_shutdown(bd->backend_connection);
		return FALSE;
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
	g_free(bd);
}

static JBackend rados_backend = {
	.type = J_BACKEND_TYPE_OBJECT,
	.component = J_BACKEND_COMPONENT_CLIENT,
	.flags = 0,
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
		.backend_write = backend_write,
		.backend_get_all = backend_get_all,
		.backend_get_by_prefix = backend_get_by_prefix,
		.backend_iterate = backend_iterate,
	}
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &rados_backend;
}
