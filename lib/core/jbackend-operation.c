/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019 Michael Kuhn
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

#include <jbackend-operation.h>

#include <jtrace-internal.h>

/**
 * \defgroup JBackendOperation Backend Operation
 *
 * Backend operation data structures and functions.
 *
 * @{
 **/

gboolean
j_backend_operation_unwrap_db_schema_create (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	return j_backend_db_schema_create(backend, batch, data->in_param[1].ptr, data->in_param[2].ptr, data->out_param[0].ptr);
}

gboolean
j_backend_operation_unwrap_db_schema_get (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	return j_backend_db_schema_get(backend, batch, data->in_param[1].ptr, data->out_param[0].ptr, data->out_param[1].ptr);
}

gboolean
j_backend_operation_unwrap_db_schema_delete (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	return j_backend_db_schema_delete(backend, batch, data->in_param[1].ptr, data->out_param[0].ptr);
}

gboolean
j_backend_operation_unwrap_db_insert (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	return j_backend_db_insert(backend, batch, data->in_param[1].ptr, data->in_param[2].ptr, data->out_param[0].ptr);
}

gboolean
j_backend_operation_unwrap_db_update (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	return j_backend_db_update(backend, batch, data->in_param[1].ptr, data->in_param[2].ptr, data->in_param[3].ptr, data->out_param[0].ptr);
}

gboolean
j_backend_operation_unwrap_db_delete (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	return j_backend_db_delete(backend, batch, data->in_param[1].ptr, data->in_param[2].ptr, data->out_param[0].ptr);
}

// FIXME clean up
gboolean
j_backend_operation_unwrap_db_query (JBackend* backend, gpointer batch, JBackendOperation* data)
{
	GError** error;
	gboolean ret;
	gpointer iter;
	guint i;
	char str_buf[16];
	const char* key;
	bson_t* bson = data->out_param[0].ptr;
	bson_t* tmp;

	j_trace_enter(G_STRFUNC, NULL);
	bson_init(bson);
	ret = j_backend_db_query(backend, batch, data->in_param[1].ptr, data->in_param[2].ptr, &iter, data->out_param[1].ptr);
	if (!ret)
	{
		goto _error;
	}
	i = 0;
	do
	{
		bson_uint32_to_string(i, &key, str_buf, sizeof(str_buf));
		tmp = bson_new();
		ret = j_backend_db_iterate(backend, iter, tmp, data->out_param[1].ptr);
		i++;
		if (ret)
		{
			bson_append_document(bson, key, -1, tmp);
		}
		bson_destroy(tmp);
	} while (ret); //TODO handle the no more elements error here
	error = data->out_param[1].ptr;
	if (error && (*error)->code == J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS)
	{
		g_error_free(*error);
		*error = NULL;
	}
	j_trace_leave(G_STRFUNC);
	return TRUE;
_error:
	j_trace_leave(G_STRFUNC);
	return FALSE;
}

gboolean
j_backend_operation_to_message(JMessage* message, JBackendOperationParam* data, guint arrlen)
{
	JBackendOperationParam* element;
	guint i;
	guint len = 0;
	guint error_message_len;
	guint error_domain_len;
	guint tmp;
	GError** error;

	j_trace_enter(G_STRFUNC, NULL);
	for (i = 0; i < arrlen; i++)
	{
		len += 4;
		element = &data[i];
		switch (element->type)
		{
		case J_BACKEND_OPERATION_PARAM_TYPE_STR:
			if (element->ptr)
			{
				element->len = strlen(element->ptr) + 1;
			}
			else
			{
				element->len = 0;
			}
			break;
		case J_BACKEND_OPERATION_PARAM_TYPE_BLOB:
			break;
		case J_BACKEND_OPERATION_PARAM_TYPE_BSON:
			if (element->bson_initialized && element->ptr)
			{
				element->len = ((bson_t*)element->ptr)->len;
			}
			else
			{
				element->len = 0;
			}
			break;
		case J_BACKEND_OPERATION_PARAM_TYPE_ERROR:
			element->len = 4;
			error = (GError**)element->ptr;
			if (error)
			{
				element->len += 4;
				if (*error)
				{
					element->error_quark_string = g_quark_to_string((*error)->domain);
					element->len += 4 + 4 + 4;
					element->len += strlen(element->error_quark_string) + 1;
					element->len += strlen((*error)->message) + 1;
				}
			}
			break;
		case _J_BACKEND_OPERATION_PARAM_TYPE_COUNT:
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
			case J_BACKEND_OPERATION_PARAM_TYPE_STR:
			case J_BACKEND_OPERATION_PARAM_TYPE_BLOB:
				if (element->ptr)
				{
					j_message_append_n(message, element->ptr, element->len);
				}
				break;
			case J_BACKEND_OPERATION_PARAM_TYPE_BSON:
				if (element->bson_initialized && element->ptr)
				{
					j_message_append_n(message, bson_get_data(element->ptr), element->len);
					element->bson_initialized = FALSE;
				}
				break;
			case J_BACKEND_OPERATION_PARAM_TYPE_ERROR:
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
						error_domain_len = strlen(element->error_quark_string) + 1;
						error_message_len = strlen((*error)->message) + 1;
						j_message_append_4(message, &tmp);
						j_message_append_4(message, &error_domain_len);
						j_message_append_4(message, &error_message_len);
						j_message_append_n(message, element->error_quark_string, error_domain_len);
						j_message_append_n(message, (*error)->message, error_message_len);
						g_error_free(*error);
						*error = NULL;
					}
				}
				break;
			case _J_BACKEND_OPERATION_PARAM_TYPE_COUNT:
			default:
				abort();
			}
		}
	}
	j_trace_leave(G_STRFUNC);
	return TRUE;
}

/*
*this function is called only on the client side of the backend
 * the return value of this function is the same as the return value of the original function call
*/
gboolean
j_backend_operation_from_message(JMessage* message, JBackendOperationParam* data, guint arrlen)
{
	JBackendOperationParam* element;
	guint i;
	guint len;
	guint error_code;
	guint error_message_len;
	guint error_domain_len;
	GQuark error_quark;
	GError** error;
	gboolean ret = TRUE;

	j_trace_enter(G_STRFUNC, NULL);
	for (i = 0; i < arrlen; i++)
	{
		len = j_message_get_4(message);
		element = &data[i];
		element->len = len;
		if (len)
		{
			switch (element->type)
			{
			case J_BACKEND_OPERATION_PARAM_TYPE_STR:
			case J_BACKEND_OPERATION_PARAM_TYPE_BLOB:
				*(gchar**)element->ptr = g_strdup(j_message_get_n(message, len));
				break;
			case J_BACKEND_OPERATION_PARAM_TYPE_BSON:
				ret = bson_init_static(&element->bson, j_message_get_n(message, len), len) && ret;
				if (element->ptr)
				{
					bson_copy_to(&element->bson, element->ptr);
				}
				break;
			case J_BACKEND_OPERATION_PARAM_TYPE_ERROR:
				error = (GError**)element->ptr;
				if (error)
				{
					if (j_message_get_4(message))
					{
						if (j_message_get_4(message))
						{
							ret = FALSE;
							error_code = j_message_get_4(message);
							error_domain_len = j_message_get_4(message);
							error_message_len = j_message_get_4(message);
							error_quark = g_quark_from_string(j_message_get_n(message, error_domain_len));
							g_set_error_literal(error, error_quark, error_code, j_message_get_n(message, error_message_len));
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
			case _J_BACKEND_OPERATION_PARAM_TYPE_COUNT:
			default:
				abort();
			}
		}
	}
	j_trace_leave(G_STRFUNC);
	return ret;
}

/*
 * this function is called server side. This assumes 'message' is valid as long as the returned array is used
 * the return value of this function is the same as the return value of the original function call
 */
gboolean
j_backend_operation_from_message_static(JMessage* message, JBackendOperationParam* data, guint arrlen)
{
	JBackendOperationParam* element;
	guint i;
	guint len;
	guint error_message_len;
	guint error_domain_len;
	gboolean ret = TRUE;

	j_trace_enter(G_STRFUNC, NULL);
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
			case J_BACKEND_OPERATION_PARAM_TYPE_BLOB:
			case J_BACKEND_OPERATION_PARAM_TYPE_STR:
				element->ptr = j_message_get_n(message, len);
				break;
			case J_BACKEND_OPERATION_PARAM_TYPE_BSON:
				element->ptr = &element->bson;
				ret = bson_init_static(element->ptr, j_message_get_n(message, len), len) && ret;
				break;
			case J_BACKEND_OPERATION_PARAM_TYPE_ERROR:
				if (j_message_get_4(message))
				{
					element->ptr = &element->error_ptr;
					element->error_ptr = NULL;
					if (j_message_get_4(message))
					{
						ret = FALSE;
						element->error_ptr = &element->error;
						element->error.code = j_message_get_4(message);
						error_domain_len = j_message_get_4(message);
						error_message_len = j_message_get_4(message);
						element->error.domain = g_quark_from_string(j_message_get_n(message, error_domain_len));
						element->error.message = j_message_get_n(message, error_message_len);
					}
				}
				break;
			case _J_BACKEND_OPERATION_PARAM_TYPE_COUNT:
			default:
				abort();
			}
		}
	}
	j_trace_leave(G_STRFUNC);
	return ret;
}

/**
 * @}
 **/
