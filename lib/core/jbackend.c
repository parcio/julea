/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <jbackend.h>

#include <jtrace-internal.h>

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

GQuark
j_backend_db_error_quark (void)
{
	return g_quark_from_static_string("j-backend-db-error-quark");
}

static
GModule*
j_backend_load (gchar const* name, JBackendComponent component, JBackendType type, JBackend** backend)
{
	JBackend* (*module_backend_info) (void) = NULL;

	JBackend* tmp_backend = NULL;
	GModule* module = NULL;
	gchar* path = NULL;
	gchar* tpath = NULL;
	gchar const* type_str = NULL;

	switch(type)
	{
		case J_BACKEND_TYPE_OBJECT:
			type_str = "object";
			break;
		case J_BACKEND_TYPE_KV:
			type_str = "kv";
			break;
		case J_BACKEND_TYPE_DB:
			type_str = "db";
			break;
		default:
			g_warn_if_reached();
	}

#ifdef JULEA_BACKEND_PATH_BUILD
	tpath = g_build_filename(JULEA_BACKEND_PATH_BUILD, type_str, NULL);
	path = g_module_build_path(tpath, name);
	module = g_module_open(path, G_MODULE_BIND_LOCAL);
	g_free(tpath);
	g_free(path);
#endif

	if (module == NULL)
	{
		tpath = g_build_filename(JULEA_BACKEND_PATH, type_str, NULL);
		path = g_module_build_path(tpath, name);
		module = g_module_open(path, G_MODULE_BIND_LOCAL);
		g_free(tpath);
		g_free(path);
	}

	if (module == NULL)
	{
		goto error;
	}

	g_module_symbol(module, "backend_info", (gpointer*)&module_backend_info);

	if (module_backend_info == NULL)
	{
		goto error;
	}

	j_trace_enter("backend_info", NULL);
	tmp_backend = module_backend_info();
	j_trace_leave("backend_info");

	if (tmp_backend == NULL)
	{
		goto error;
	}

	if (tmp_backend->type != type || !(tmp_backend->component & component))
	{
		goto error;
	}

	if (type == J_BACKEND_TYPE_OBJECT)
	{
		if (tmp_backend->object.backend_init == NULL
		    || tmp_backend->object.backend_fini == NULL
		    || tmp_backend->object.backend_create == NULL
		    || tmp_backend->object.backend_delete == NULL
		    || tmp_backend->object.backend_open == NULL
		    || tmp_backend->object.backend_close == NULL
		    || tmp_backend->object.backend_status == NULL
		    || tmp_backend->object.backend_sync == NULL
		    || tmp_backend->object.backend_read == NULL
		    || tmp_backend->object.backend_write == NULL)
		{
			goto error;
		}
	}

	if (type == J_BACKEND_TYPE_KV)
	{
		if (tmp_backend->kv.backend_init == NULL
		    || tmp_backend->kv.backend_fini == NULL
		    || tmp_backend->kv.backend_batch_start == NULL
		    || tmp_backend->kv.backend_batch_execute == NULL
		    || tmp_backend->kv.backend_put == NULL
		    || tmp_backend->kv.backend_delete == NULL
		    || tmp_backend->kv.backend_get == NULL
		    || tmp_backend->kv.backend_get_all == NULL
		    || tmp_backend->kv.backend_get_by_prefix == NULL
		    || tmp_backend->kv.backend_iterate == NULL)
		{
			goto error;
		}
	}

	if (type == J_BACKEND_TYPE_DB)
	{
		if (tmp_backend->db.backend_init == NULL
		    || tmp_backend->db.backend_fini == NULL
		    || tmp_backend->db.backend_batch_start == NULL
		    || tmp_backend->db.backend_batch_execute == NULL
		    || tmp_backend->db.backend_schema_create == NULL
		    || tmp_backend->db.backend_schema_get == NULL
		    || tmp_backend->db.backend_schema_delete == NULL
		    || tmp_backend->db.backend_insert == NULL
		    || tmp_backend->db.backend_update == NULL
		    || tmp_backend->db.backend_delete == NULL
		    || tmp_backend->db.backend_query == NULL
		    || tmp_backend->db.backend_iterate == NULL)
		{
			goto error;
		}
	}

	*backend = tmp_backend;

	return module;

error:
	if (module != NULL)
	{
		g_module_close(module);
	}

	*backend = NULL;

	return NULL;
}

gboolean
j_backend_load_client (gchar const* name, gchar const* component, JBackendType type, GModule** module, JBackend** backend)
{
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(component != NULL, FALSE);
	g_return_val_if_fail(type == J_BACKEND_TYPE_OBJECT || type == J_BACKEND_TYPE_KV || type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(module != NULL, FALSE);
	g_return_val_if_fail(backend != NULL, FALSE);

	*module = NULL;
	*backend = NULL;

	if (g_strcmp0(component, "client") == 0)
	{
		*module = j_backend_load(name, J_BACKEND_COMPONENT_CLIENT, type, backend);

		return TRUE;
	}

	return FALSE;
}

gboolean
j_backend_load_server (gchar const* name, gchar const* component, JBackendType type, GModule** module, JBackend** backend)
{
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(component != NULL, FALSE);
	g_return_val_if_fail(type == J_BACKEND_TYPE_OBJECT || type == J_BACKEND_TYPE_KV || type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(module != NULL, FALSE);
	g_return_val_if_fail(backend != NULL, FALSE);

	*module = NULL;
	*backend = NULL;

	if (g_strcmp0(component, "server") == 0)
	{
		*module = j_backend_load(name, J_BACKEND_COMPONENT_SERVER, type, backend);

		return TRUE;
	}

	return FALSE;
}

gboolean
j_backend_object_init (JBackend* backend, gchar const* path)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	j_trace_enter("backend_init", "%s", path);
	ret = backend->object.backend_init(path);
	j_trace_leave("backend_init");

	return ret;
}

void
j_backend_object_fini (JBackend* backend)
{
	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_OBJECT);

	j_trace_enter("backend_fini", NULL);
	backend->object.backend_fini();
	j_trace_leave("backend_fini");
}

gboolean
j_backend_object_create (JBackend* backend, gchar const* namespace, gchar const* path, gpointer* data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_create", "%s, %s, %p", namespace, path, (gpointer)data);
	ret = backend->object.backend_create(namespace, path, data);
	j_trace_leave("backend_create");

	return ret;
}

gboolean
j_backend_object_open (JBackend* backend, gchar const* namespace, gchar const* path, gpointer* data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_open", "%s, %s, %p", namespace, path, (gpointer)data);
	ret = backend->object.backend_open(namespace, path, data);
	j_trace_leave("backend_open");

	return ret;
}

gboolean
j_backend_object_delete (JBackend* backend, gpointer data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_delete", "%p", data);
	ret = backend->object.backend_delete(data);
	j_trace_leave("backend_delete");

	return ret;
}

gboolean
j_backend_object_close (JBackend* backend, gpointer data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_close", "%p", data);
	ret = backend->object.backend_close(data);
	j_trace_leave("backend_close");

	return ret;
}

gboolean
j_backend_object_status (JBackend* backend, gpointer data, gint64* modification_time, guint64* size)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(modification_time != NULL, FALSE);
	g_return_val_if_fail(size != NULL, FALSE);

	j_trace_enter("backend_status", "%p, %p, %p", data, (gpointer)modification_time, (gpointer)size);
	ret = backend->object.backend_status(data, modification_time, size);
	j_trace_leave("backend_status");

	return ret;
}

gboolean
j_backend_object_sync (JBackend* backend, gpointer data)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter("backend_sync", "%p", data);
	ret = backend->object.backend_sync(data);
	j_trace_leave("backend_sync");

	return ret;
}

gboolean
j_backend_object_read (JBackend* backend, gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(buffer != NULL, FALSE);
	g_return_val_if_fail(bytes_read != NULL, FALSE);

	j_trace_enter("backend_read", "%p, %p, %" G_GUINT64_FORMAT ", %" G_GUINT64_FORMAT ", %p", data, buffer, length, offset, (gpointer)bytes_read);
	ret = backend->object.backend_read(data, buffer, length, offset, bytes_read);
	j_trace_leave("backend_read");

	return ret;
}

gboolean
j_backend_object_write (JBackend* backend, gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(buffer != NULL, FALSE);
	g_return_val_if_fail(bytes_written != NULL, FALSE);

	j_trace_enter("backend_write", "%p, %p, %" G_GUINT64_FORMAT ", %" G_GUINT64_FORMAT ", %p", data, buffer, length, offset, (gpointer)bytes_written);
	ret = backend->object.backend_write(data, buffer, length, offset, bytes_written);
	j_trace_leave("backend_write");

	return ret;
}

gboolean
j_backend_kv_init (JBackend* backend, gchar const* path)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	j_trace_enter("backend_init", "%s", path);
	ret = backend->kv.backend_init(path);
	j_trace_leave("backend_init");

	return ret;
}

void
j_backend_kv_fini (JBackend* backend)
{
	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_KV);

	j_trace_enter("backend_fini", NULL);
	backend->kv.backend_fini();
	j_trace_leave("backend_fini");
}

gboolean
j_backend_kv_batch_start (JBackend* backend, gchar const* namespace, JSemanticsSafety safety, gpointer* batch)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	j_trace_enter("backend_batch_start", "%s, %d, %p", namespace, safety, (gpointer)batch);
	ret = backend->kv.backend_batch_start(namespace, safety, batch);
	j_trace_leave("backend_batch_start");

	return ret;
}

gboolean
j_backend_kv_batch_execute (JBackend* backend, gpointer batch)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	j_trace_enter("backend_batch_execute", "%p", batch);
	ret = backend->kv.backend_batch_execute(batch);
	j_trace_leave("backend_batch_execute");

	return ret;
}

gboolean
j_backend_kv_put (JBackend* backend, gpointer batch, gchar const* key, gconstpointer value, guint32 value_len)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	j_trace_enter("backend_put", "%p, %s, %p, %u", batch, key, (gconstpointer)value, value_len);
	ret = backend->kv.backend_put(batch, key, value, value_len);
	j_trace_leave("backend_put");

	return ret;
}

gboolean
j_backend_kv_delete (JBackend* backend, gpointer batch, gchar const* key)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	j_trace_enter("backend_delete", "%p, %s", batch, key);
	ret = backend->kv.backend_delete(batch, key);
	j_trace_leave("backend_delete");

	return ret;
}

gboolean
j_backend_kv_get (JBackend* backend, gpointer batch, gchar const* key, gpointer* value, guint32* value_len)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(value_len != NULL, FALSE);

	j_trace_enter("backend_get", "%p, %s, %p, %p", batch, key, (gpointer)value, (gpointer)value_len);
	ret = backend->kv.backend_get(batch, key, value, value_len);
	j_trace_leave("backend_get");

	return ret;
}

gboolean
j_backend_kv_get_all (JBackend* backend, gchar const* namespace, gpointer* iterator)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);

	j_trace_enter("backend_get_all", "%s, %p", namespace, (gpointer)iterator);
	ret = backend->kv.backend_get_all(namespace, iterator);
	j_trace_leave("backend_get_all");

	return ret;
}

gboolean
j_backend_kv_get_by_prefix (JBackend* backend, gchar const* namespace, gchar const* prefix, gpointer* iterator)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);

	j_trace_enter("backend_get_by_prefix", "%s, %s, %p", namespace, prefix, (gpointer)iterator);
	ret = backend->kv.backend_get_by_prefix(namespace, prefix, iterator);
	j_trace_leave("backend_get_by_prefix");

	return ret;
}
gboolean
j_backend_kv_iterate (JBackend* backend, gpointer iterator, gchar const** key, gconstpointer* value, guint32* value_len)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(value_len != NULL, FALSE);

	j_trace_enter("backend_iterate", "%p, %p, %p, %p", iterator, (gpointer)key, (gpointer)value, (gpointer)value_len);
	ret = backend->kv.backend_iterate(iterator, key, value, value_len);
	j_trace_leave("backend_iterate");

	return ret;
}

gboolean
j_backend_db_init (JBackend* backend, gchar const* path)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	j_trace_enter("backend_init", "%s", path);
	ret = backend->db.backend_init(path);
	j_trace_leave("backend_init");

	return ret;
}

void
j_backend_db_fini (JBackend* backend)
{
	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_DB);

	j_trace_enter("backend_fini", NULL);
	backend->db.backend_fini();
	j_trace_leave("backend_fini");
}

gboolean
j_backend_db_batch_start (JBackend* backend, gchar const* namespace, JSemanticsSafety safety, gpointer* batch, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_batch_start", "%s, %d, %p, %p", namespace, safety, (gpointer)batch, (gpointer)error);
	ret = backend->db.backend_batch_start(namespace, safety, batch, error);
	j_trace_leave("backend_batch_start");

	return ret;
}

gboolean
j_backend_db_batch_execute (JBackend* backend, gpointer batch, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_batch_execute", "%p, %p", batch, (gpointer)error);
	ret = backend->db.backend_batch_execute(batch, error);
	j_trace_leave("backend_batch_execute");

	return ret;
}

gboolean
j_backend_db_schema_create (JBackend* backend, gpointer batch, gchar const* name, bson_t const* schema, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_schema_create", "%p, %s, %p, %p", batch, name, (gconstpointer)schema, (gpointer)error);
	ret = backend->db.backend_schema_create(batch, name, schema, error);
	j_trace_leave("backend_schema_create");

	return ret;
}

gboolean
j_backend_db_schema_get (JBackend* backend, gpointer batch, gchar const* name, bson_t* schema, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_schema_get", "%p, %s, %p, %p", batch, name, (gpointer)schema, (gpointer)error);
	ret = backend->db.backend_schema_get(batch, name, schema, error);
	j_trace_leave("backend_schema_get");

	return ret;
}

gboolean
j_backend_db_schema_delete (JBackend* backend, gpointer batch, gchar const* name, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_schema_delete", "%p, %s, %p", batch, name, (gpointer)error);
	ret = backend->db.backend_schema_delete(batch, name, error);
	j_trace_leave("backend_schema_delete");

	return ret;
}

gboolean
j_backend_db_insert (JBackend* backend, gpointer batch, gchar const* name, bson_t const* metadata, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_insert", "%p, %s, %p, %p", batch, name, (gconstpointer)metadata, (gpointer)error);
	ret = backend->db.backend_insert(batch, name, metadata, error);
	j_trace_leave("backend_insert");

	return ret;
}

gboolean
j_backend_db_update (JBackend* backend, gpointer batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_update", "%p, %s, %p, %p, %p", batch, name, (gconstpointer)selector, (gconstpointer)metadata, (gpointer)error);
	ret = backend->db.backend_update(batch, name, selector, metadata, error);
	j_trace_leave("backend_update");

	return ret;
}

gboolean
j_backend_db_delete (JBackend* backend, gpointer batch, gchar const* name, bson_t const* selector, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_delete", "%p, %s, %p, %p", batch, name, (gconstpointer)selector, (gpointer)error);
	ret = backend->db.backend_delete(batch, name, selector, error);
	j_trace_leave("backend_delete");

	return ret;
}

gboolean
j_backend_db_query (JBackend* backend, gpointer batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_query", "%p, %s, %p, %p, %p", batch, name, (gconstpointer)selector, (gpointer)iterator, (gpointer)error);
	ret = backend->db.backend_query(batch, name, selector, iterator, error);
	j_trace_leave("backend_query");

	return ret;
}

gboolean
j_backend_db_iterate (JBackend* backend, gpointer iterator, bson_t* metadata, GError** error)
{
	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);

	j_trace_enter("backend_iterate", "%p, %p, %p", iterator, (gpointer)metadata, (gpointer)error);
	ret = backend->db.backend_iterate(iterator, metadata, error);
	j_trace_leave("backend_iterate");

	return ret;
}

/**
 * @}
 **/
