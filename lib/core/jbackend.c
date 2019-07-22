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
#include <julea-internal.h>

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
j_backend_db_message_from_data(JMessage* message, JBackend_db_operation* data, guint arrlen)
{
	JBackend_db_operation* element;
	guint i;
	guint len = 0;
	guint tmp;
	GError** error;
	for (i = 0; i < arrlen; i++)
	{
		len += 4;
		element = &data[i];
		switch (element->type)
		{
		case J_DB_PARAM_TYPE_STR:
			if (element->ptr)
				element->len = strlen(element->ptr) + 1;
			else
				element->len = 0;
			break;
		case J_DB_PARAM_TYPE_BLOB:
			break;
		case J_DB_PARAM_TYPE_BSON:
			if (element->bson_initialized && element->ptr)
				element->len = ((bson_t*)element->ptr)->len;
			else
				element->len = 0;
			break;
		case J_DB_PARAM_TYPE_ERROR:
			element->len = 4;
			error = (GError**)element->ptr;
			if (error)
			{
				element->len += 4;
				if (*error)
				{
					element->len += 4 + 4;
					element->len += strlen((*error)->message) + 1;
				}
			}
			break;
		case _J_DB_PARAM_TYPE_COUNT:
		default:
			abort();
		}
		len += element->len;
	}
	j_message_add_operation(message, len);
	for (i = 0; i < arrlen; i++)
	{
		element = &data[i];
		j_message_append_4(message, &element->len);
		if (element->len)
		{
			switch (element->type)
			{
			case J_DB_PARAM_TYPE_STR:
			case J_DB_PARAM_TYPE_BLOB:
				if (element->ptr)
					j_message_append_n(message, element->ptr, element->len);
				break;
			case J_DB_PARAM_TYPE_BSON:
				if (element->bson_initialized && element->ptr)
				{
					j_message_append_n(message, bson_get_data(element->ptr), element->len);
					element->bson_initialized = FALSE;
				}
				break;
			case J_DB_PARAM_TYPE_ERROR:
				error = (GError**)element->ptr;
				tmp = error != NULL;
				j_message_append_4(message, &tmp);
				if (error)
				{
					tmp = *error != NULL;
					j_message_append_4(message, &tmp);
					if (*error)
					{
						tmp = (*error)->code;
						j_message_append_4(message, &tmp);
						tmp = strlen((*error)->message) + 1;
						j_message_append_4(message, &tmp);
						j_message_append_n(message, (*error)->message, tmp);
						g_error_free(*error);
						*error = NULL;
					}
				}
				break;
			case _J_DB_PARAM_TYPE_COUNT:
			default:
				abort();
			}
		}
	}
	return TRUE;
}
/*
*this function is called only on the client side of the backend
 * the return value of this function is the same as the return value of the original function call
*/
gboolean
j_backend_db_message_to_data(JMessage* message, JBackend_db_operation* data, guint arrlen)
{
	JBackend_db_operation* element;
	guint i;
	guint len;
	gint error_code;
	gint error_message_len;
	GError** error;
	gboolean ret = TRUE;
	for (i = 0; i < arrlen; i++)
	{
		len = j_message_get_4(message);
		element = &data[i];
		element->len = len;
		if (len)
		{
			switch (element->type)
			{
			case J_DB_PARAM_TYPE_STR:
			case J_DB_PARAM_TYPE_BLOB:
				*(gchar**)element->ptr = g_strdup(j_message_get_n(message, len));
				break;
			case J_DB_PARAM_TYPE_BSON:
				ret = bson_init_static(&element->bson, j_message_get_n(message, len), len) && ret;
				if (element->ptr)
					bson_copy_to(&element->bson, element->ptr);
				break;
			case J_DB_PARAM_TYPE_ERROR:
				error = (GError**)element->ptr;
				if (error)
				{
					if (j_message_get_4(message))
					{
						if (j_message_get_4(message))
						{
							ret = FALSE;
							error_code = j_message_get_4(message);
							error_message_len = j_message_get_4(message);
							g_set_error_literal(error, JULEA_BACKEND_ERROR, error_code, j_message_get_n(message, error_message_len));
						}
					}
				}
				else
				{
					if (j_message_get_4(message))
					{
						if (j_message_get_4(message))
						{
							ret = FALSE;
							j_message_get_4(message);
							error_message_len = j_message_get_4(message);
							j_message_get_n(message, error_message_len);
						}
					}
				}
				break;
			case _J_DB_PARAM_TYPE_COUNT:
			default:
				abort();
			}
		}
	}
	return ret;
}
/*
*this function is called server side. This assumes 'message' is valid as long as the returned array is used
 * the return value of this function is the same as the return value of the original function call
*/
gboolean
j_backend_db_message_to_data_static(JMessage* message, JBackend_db_operation* data, guint arrlen)
{
	JBackend_db_operation* element;
	guint i;
	guint len;
	guint error_message_len;
	gboolean ret = TRUE;
	for (i = 0; i < arrlen; i++)
	{
		len = j_message_get_4(message);
		element = &data[i];
		element->ptr = NULL;
		element->len = len;
		if (len)
		{
			switch (element->type)
			{
			case J_DB_PARAM_TYPE_BLOB:
			case J_DB_PARAM_TYPE_STR:
				element->ptr = j_message_get_n(message, len);
				break;
			case J_DB_PARAM_TYPE_BSON:
				element->ptr = &element->bson;
				ret = bson_init_static(element->ptr, j_message_get_n(message, len), len) && ret;
				break;
			case J_DB_PARAM_TYPE_ERROR:
				if (j_message_get_4(message))
				{
					element->ptr = &element->error_ptr;
					element->error_ptr = NULL;
					if (j_message_get_4(message))
					{
						ret = FALSE;
						element->error_ptr = &element->error;
						element->error.code = j_message_get_4(message);
						error_message_len = j_message_get_4(message);
						element->error.message = j_message_get_n(message, error_message_len);
					}
				}
				break;
			case _J_DB_PARAM_TYPE_COUNT:
			default:
				abort();
			}
		}
	}
	return ret;
}
gboolean
j_backend_db_init(JBackend* backend, gchar const* path)
{
	gboolean ret;
	g_return_val_if_fail(backend != NULL, FALSE);
	g_return_val_if_fail(path != NULL, FALSE);
	ret = backend->db.backend_init(path);
	return ret;
}
void
j_backend_db_fini(JBackend* backend)
{
	g_return_if_fail(backend != NULL);
	backend->db.backend_fini();
}
static gboolean
j_backend_db_schema_create(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	return backend->db.backend_schema_create( //
		batch, //
		data->in_param[1].ptr,
		data->in_param[2].ptr, data->out_param[0].ptr);
}
const JBackend_db_operation_data j_db_schema_create_params = {
	.backend_func = j_backend_db_schema_create,
	.in_param_count = 3,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{
			.type = J_DB_PARAM_TYPE_ERROR,
		},
	},
};
static gboolean
j_backend_db_schema_get(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	return backend->db.backend_schema_get( //
		batch, //
		data->in_param[1].ptr, //
		data->out_param[0].ptr, data->out_param[1].ptr);
}
const JBackend_db_operation_data j_db_schema_get_params = {
	.backend_func = j_backend_db_schema_get,
	.in_param_count = 2,
	.out_param_count = 2,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
	},
	.out_param = {
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
		{ .type = J_DB_PARAM_TYPE_ERROR },
	},
};
static gboolean
j_backend_db_schema_delete(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	return backend->db.backend_schema_delete( //
		batch, //
		data->in_param[1].ptr, data->out_param[0].ptr);
}
const JBackend_db_operation_data j_db_schema_delete_params = {
	.backend_func = j_backend_db_schema_delete,
	.in_param_count = 2,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
	},
	.out_param = {
		{ .type = J_DB_PARAM_TYPE_ERROR },
	},
};
static gboolean
j_backend_db_insert(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	return backend->db.backend_insert( //
		batch, //
		data->in_param[1].ptr, //
		data->in_param[2].ptr, data->out_param[0].ptr);
}
const JBackend_db_operation_data j_db_insert_params = {
	.backend_func = j_backend_db_insert,
	.in_param_count = 3,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{ .type = J_DB_PARAM_TYPE_ERROR },
	},
};
static gboolean
j_backend_db_update(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	return backend->db.backend_update( //
		batch, //
		data->in_param[1].ptr, //
		data->in_param[2].ptr, //
		data->in_param[3].ptr, data->out_param[0].ptr);
}
const JBackend_db_operation_data j_db_update_params = {
	.backend_func = j_backend_db_update,
	.in_param_count = 4,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{ .type = J_DB_PARAM_TYPE_ERROR },
	},
};
static gboolean
j_backend_db_delete(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	return backend->db.backend_delete( //
		batch, //
		data->in_param[1].ptr, //
		data->in_param[2].ptr, data->out_param[0].ptr);
}
const JBackend_db_operation_data j_db_delete_params = {
	.backend_func = j_backend_db_delete,
	.in_param_count = 3,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{ .type = J_DB_PARAM_TYPE_ERROR },
	},
};
static gboolean
j_backend_db_get_all(JBackend* backend, gpointer batch, JBackend_db_operation_data* data)
{
	GError** error;
	gboolean ret;
	gpointer iter;
	guint i;
	char str_buf[16];
	const char* key;
	bson_t* bson = data->out_param[0].ptr;
	bson_t* tmp;
	bson_init(bson);
	ret = backend->db.backend_query( //
		batch, //
		data->in_param[1].ptr, //
		data->in_param[2].ptr, //
		&iter, //
		data->out_param[1].ptr);
	if (!ret)
		return FALSE;
	i = 0;
	do
	{
		bson_uint32_to_string(i, &key, str_buf, sizeof(str_buf));
		tmp = bson_new();
		ret = backend->db.backend_iterate(iter, tmp, data->out_param[1].ptr);
		i++;
		if (ret)
			bson_append_document(bson, key, -1, tmp);
		bson_destroy(tmp);
	} while (ret); //TODO handle the no more elements error here
	error = data->out_param[1].ptr;
	if (error && (*error)->code == JULEA_BACKEND_ERROR_ITERATOR_NO_MORE_ELEMENTS)
	{
		g_error_free(*error);
		*error = NULL;
	}
	return TRUE;
}
const JBackend_db_operation_data j_db_get_all_params = {
	.backend_func = j_backend_db_get_all,
	.in_param_count = 3,
	.out_param_count = 2,
	.in_param = {
		{ .type = J_DB_PARAM_TYPE_STR },
		{ .type = J_DB_PARAM_TYPE_STR },
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{
			.type = J_DB_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
		{ .type = J_DB_PARAM_TYPE_ERROR },
	},
};

/**
 * @}
 **/
