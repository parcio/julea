/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
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

#include <jtrace.h>

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

GQuark
j_backend_bson_error_quark (void)
{
	J_TRACE_FUNCTION(NULL);

	return g_quark_from_static_string("j-backend-bson-error-quark");
}

GQuark
j_backend_db_error_quark (void)
{
	J_TRACE_FUNCTION(NULL);

	return g_quark_from_static_string("j-backend-db-error-quark");
}

GQuark
j_backend_sql_error_quark (void)
{
	J_TRACE_FUNCTION(NULL);

	return g_quark_from_static_string("j-backend-sql-error-quark");
}

static
GModule*
j_backend_load (gchar const* name, JBackendComponent component, JBackendType type, JBackend** backend)
{
	J_TRACE_FUNCTION(NULL);

	JBackend* (*module_backend_info) (void) = NULL;

	JBackend* tmp_backend = NULL;
	GModule* module = NULL;
	gchar const* backend_path = NULL;
	gchar* path = NULL;
	gchar* type_path = NULL;
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

#ifdef JULEA_DEBUG
	if ((backend_path = g_getenv("JULEA_BACKEND_PATH")) != NULL)
	{
		type_path = g_build_filename(backend_path, type_str, NULL);
		path = g_module_build_path(type_path, name);
		module = g_module_open(path, G_MODULE_BIND_LOCAL);
		g_free(type_path);
		g_free(path);
	}
#endif

	if (module == NULL)
	{
		type_path = g_build_filename(JULEA_BACKEND_PATH, type_str, NULL);
		path = g_module_build_path(type_path, name);
		module = g_module_open(path, G_MODULE_BIND_LOCAL);
		g_free(type_path);
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

	{
		J_TRACE("backend_info", NULL);
		tmp_backend = module_backend_info();
	}

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
	J_TRACE_FUNCTION(NULL);

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
	J_TRACE_FUNCTION(NULL);

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
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	{
		J_TRACE("backend_init", "%s", path);
		ret = backend->object.backend_init(path);
	}

	return ret;
}

void
j_backend_object_fini (JBackend* backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_OBJECT);

	{
		J_TRACE("backend_fini", NULL);
		backend->object.backend_fini();
	}
}

gboolean
j_backend_object_create (JBackend* backend, gchar const* namespace, gchar const* path, gpointer* data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	{
		J_TRACE("backend_create", "%s, %s, %p", namespace, path, (gpointer)data);
		ret = backend->object.backend_create(namespace, path, data);
	}

	return ret;
}

gboolean
j_backend_object_open (JBackend* backend, gchar const* namespace, gchar const* path, gpointer* data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	{
		J_TRACE("backend_open", "%s, %s, %p", namespace, path, (gpointer)data);
		ret = backend->object.backend_open(namespace, path, data);
	}

	return ret;
}

gboolean
j_backend_object_delete (JBackend* backend, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	{
		J_TRACE("backend_delete", "%p", data);
		ret = backend->object.backend_delete(data);
	}

	return ret;
}

gboolean
j_backend_object_close (JBackend* backend, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	{
		J_TRACE("backend_close", "%p", data);
		ret = backend->object.backend_close(data);
	}

	return ret;
}

gboolean
j_backend_object_status (JBackend* backend, gpointer data, gint64* modification_time, guint64* size)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(modification_time != NULL, FALSE);
	g_return_val_if_fail(size != NULL, FALSE);

	{
		J_TRACE("backend_status", "%p, %p, %p", data, (gpointer)modification_time, (gpointer)size);
		ret = backend->object.backend_status(data, modification_time, size);
	}

	return ret;
}

gboolean
j_backend_object_sync (JBackend* backend, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	{
		J_TRACE("backend_sync", "%p", data);
		ret = backend->object.backend_sync(data);
	}

	return ret;
}

gboolean
j_backend_object_read (JBackend* backend, gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(buffer != NULL, FALSE);
	g_return_val_if_fail(bytes_read != NULL, FALSE);

	{
		J_TRACE("backend_read", "%p, %p, %" G_GUINT64_FORMAT ", %" G_GUINT64_FORMAT ", %p", data, buffer, length, offset, (gpointer)bytes_read);
		ret = backend->object.backend_read(data, buffer, length, offset, bytes_read);
	}

	return ret;
}

gboolean
j_backend_object_write (JBackend* backend, gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_OBJECT, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(buffer != NULL, FALSE);
	g_return_val_if_fail(bytes_written != NULL, FALSE);

	{
		J_TRACE("backend_write", "%p, %p, %" G_GUINT64_FORMAT ", %" G_GUINT64_FORMAT ", %p", data, buffer, length, offset, (gpointer)bytes_written);
		ret = backend->object.backend_write(data, buffer, length, offset, bytes_written);
	}

	return ret;
}

gboolean
j_backend_kv_init (JBackend* backend, gchar const* path)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	{
		J_TRACE("backend_init", "%s", path);
		ret = backend->kv.backend_init(path);
	}

	return ret;
}

void
j_backend_kv_fini (JBackend* backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_KV);

	{
		J_TRACE("backend_fini", NULL);
		backend->kv.backend_fini();
	}
}

gboolean
j_backend_kv_batch_start (JBackend* backend, gchar const* namespace, JSemantics* semantics, gpointer* batch)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	{
		J_TRACE("backend_batch_start", "%s, %p, %p", namespace, (gpointer)semantics, (gpointer)batch);
		ret = backend->kv.backend_batch_start(namespace, semantics, batch);
	}

	return ret;
}

gboolean
j_backend_kv_batch_execute (JBackend* backend, gpointer batch)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	{
		J_TRACE("backend_batch_execute", "%p", batch);
		ret = backend->kv.backend_batch_execute(batch);
	}

	return ret;
}

gboolean
j_backend_kv_put (JBackend* backend, gpointer batch, gchar const* key, gconstpointer value, guint32 value_len)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	{
		J_TRACE("backend_put", "%p, %s, %p, %u", batch, key, (gconstpointer)value, value_len);
		ret = backend->kv.backend_put(batch, key, value, value_len);
	}

	return ret;
}

gboolean
j_backend_kv_delete (JBackend* backend, gpointer batch, gchar const* key)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	{
		J_TRACE("backend_delete", "%p, %s", batch, key);
		ret = backend->kv.backend_delete(batch, key);
	}

	return ret;
}

gboolean
j_backend_kv_get (JBackend* backend, gpointer batch, gchar const* key, gpointer* value, guint32* value_len)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(value_len != NULL, FALSE);

	{
		J_TRACE("backend_get", "%p, %s, %p, %p", batch, key, (gpointer)value, (gpointer)value_len);
		ret = backend->kv.backend_get(batch, key, value, value_len);
	}

	return ret;
}

gboolean
j_backend_kv_get_all (JBackend* backend, gchar const* namespace, gpointer* iterator)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);

	{
		J_TRACE("backend_get_all", "%s, %p", namespace, (gpointer)iterator);
		ret = backend->kv.backend_get_all(namespace, iterator);
	}

	return ret;
}

gboolean
j_backend_kv_get_by_prefix (JBackend* backend, gchar const* namespace, gchar const* prefix, gpointer* iterator)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);

	{
		J_TRACE("backend_get_by_prefix", "%s, %s, %p", namespace, prefix, (gpointer)iterator);
		ret = backend->kv.backend_get_by_prefix(namespace, prefix, iterator);
	}

	return ret;
}
gboolean
j_backend_kv_iterate (JBackend* backend, gpointer iterator, gchar const** key, gconstpointer* value, guint32* value_len)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_KV, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(value_len != NULL, FALSE);

	{
		J_TRACE("backend_iterate", "%p, %p, %p, %p", iterator, (gpointer)key, (gpointer)value, (gpointer)value_len);
		ret = backend->kv.backend_iterate(iterator, key, value, value_len);
	}

	return ret;
}

gboolean
j_backend_db_init (JBackend* backend, gchar const* path)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);

	{
		J_TRACE("backend_init", "%s", path);
		ret = backend->db.backend_init(path);
	}

	return ret;
}

void
j_backend_db_fini (JBackend* backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(backend != NULL);
	g_return_if_fail(backend->type == J_BACKEND_TYPE_DB);

	{
		J_TRACE("backend_fini", NULL);
		backend->db.backend_fini();
	}
}

gboolean
j_backend_db_batch_start (JBackend* backend, gchar const* namespace, JSemantics* semantics, gpointer* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_batch_start", "%s, %p, %p, %p", namespace, (gpointer)semantics, (gpointer)batch, (gpointer)error);
		ret = backend->db.backend_batch_start(namespace, semantics, batch, error);
	}

	return ret;
}

gboolean
j_backend_db_batch_execute (JBackend* backend, gpointer batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_batch_execute", "%p, %p", batch, (gpointer)error);
		ret = backend->db.backend_batch_execute(batch, error);
	}

	return ret;
}

gboolean
j_backend_db_schema_create (JBackend* backend, gpointer batch, gchar const* name, bson_t const* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(schema != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_schema_create", "%p, %s, %p, %p", batch, name, (gconstpointer)schema, (gpointer)error);
		ret = backend->db.backend_schema_create(batch, name, schema, error);
	}

	return ret;
}

gboolean
j_backend_db_schema_get (JBackend* backend, gpointer batch, gchar const* name, bson_t* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_schema_get", "%p, %s, %p, %p", batch, name, (gpointer)schema, (gpointer)error);
		ret = backend->db.backend_schema_get(batch, name, schema, error);
	}

	return ret;
}

gboolean
j_backend_db_schema_delete (JBackend* backend, gpointer batch, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_schema_delete", "%p, %s, %p", batch, name, (gpointer)error);
		ret = backend->db.backend_schema_delete(batch, name, error);
	}

	return ret;
}

gboolean
j_backend_db_insert (JBackend* backend, gpointer batch, gchar const* name, bson_t const* metadata, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(metadata != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_insert", "%p, %s, %p, %p", batch, name, (gconstpointer)metadata, (gpointer)error);
		ret = backend->db.backend_insert(batch, name, metadata, error);
	}

	return ret;
}

gboolean
j_backend_db_update (JBackend* backend, gpointer batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(metadata != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_update", "%p, %s, %p, %p, %p", batch, name, (gconstpointer)selector, (gconstpointer)metadata, (gpointer)error);
		ret = backend->db.backend_update(batch, name, selector, metadata, error);
	}

	return ret;
}

gboolean
j_backend_db_delete (JBackend* backend, gpointer batch, gchar const* name, bson_t const* selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_delete", "%p, %s, %p, %p", batch, name, (gconstpointer)selector, (gpointer)error);
		ret = backend->db.backend_delete(batch, name, selector, error);
	}

	return ret;
}

gboolean
j_backend_db_query (JBackend* backend, gpointer batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_query", "%p, %s, %p, %p, %p", batch, name, (gconstpointer)selector, (gpointer)iterator, (gpointer)error);
		ret = backend->db.backend_query(batch, name, selector, iterator, error);
	}

	return ret;
}

gboolean
j_backend_db_iterate (JBackend* backend, gpointer iterator, bson_t* metadata, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(backend->type == J_BACKEND_TYPE_DB, FALSE);
	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(metadata != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	{
		J_TRACE("backend_iterate", "%p, %p, %p", iterator, (gpointer)metadata, (gpointer)error);
		ret = backend->db.backend_iterate(iterator, metadata, error);
	}

	return ret;
}

/**
 * @}
 **/
