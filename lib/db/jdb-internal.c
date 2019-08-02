/*
 * JULEA - Flexible storage framework
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

#include <string.h>

#include <bson.h>

#include <db/jdb-internal.h>

#include <julea.h>
#include <jtrace-internal.h>
#include <core/jbson-wrapper.h>

struct J_db_iterator_helper
{
	bson_t bson;
	bson_iter_t iter;
	gboolean initialized;
};
typedef struct J_db_iterator_helper J_db_iterator_helper;

GQuark
j_frontend_db_error_quark(void)
{
	return g_quark_from_static_string("j-frontend-db-error-quark");
}

static gboolean
j_backend_db_func_exec(JList* operations, JSemantics* semantics, JMessageType type)
{
	JBackendOperation* data = NULL;
	gboolean ret = TRUE;
	GSocketConnection* db_connection;
	g_autoptr(JListIterator) iter_send = NULL;
	g_autoptr(JListIterator) iter_recieve = NULL;
	g_autoptr(JMessage) message = NULL;
	g_autoptr(JMessage) reply = NULL;
	JBackend* db_backend = j_db_backend();
	gpointer batch = NULL;
	GError* error = NULL;

	j_trace_enter(G_STRFUNC, NULL);
	if (db_backend == NULL)
	{
		message = j_message_new(type, 0);
	}
	iter_send = j_list_iterator_new(operations);
	while (j_list_iterator_next(iter_send))
	{
		data = j_list_iterator_get(iter_send);
		if (db_backend != NULL)
		{
			if (!batch)
			{
				ret = db_backend->db.backend_batch_start( //
					      data->in_param[0].ptr, //
					      j_semantics_get(semantics, J_SEMANTICS_SAFETY), //
					      &batch, &error) &&
					ret;
			}
			if (data->out_param[data->out_param_count - 1].ptr && error)
			{
				*((void**)data->out_param[data->out_param_count - 1].ptr) = g_error_copy(error);
			}
			else
			{
				ret = data->backend_func(db_backend, batch, data) && ret;
			}
		}
		else
		{
			ret = j_backend_operation_to_message(message, data->in_param, data->in_param_count) && ret;
		}
	}
	if (db_backend != NULL)
	{
		if (data != NULL)
		{
			if (!error)
			{
				ret = db_backend->db.backend_batch_execute(batch, NULL) && ret;
			}
			else
			{
				g_error_free(error);
			}
		}
	}
	else
	{
		db_connection = j_connection_pool_pop_db(0);
		j_message_send(message, db_connection);
		reply = j_message_new_reply(message);
		j_message_receive(reply, db_connection);
		iter_recieve = j_list_iterator_new(operations);
		while (j_list_iterator_next(iter_recieve))
		{
			data = j_list_iterator_get(iter_recieve);
			ret = j_backend_operation_from_message(reply, data->out_param, data->out_param_count) && ret;
		}
		j_connection_pool_push_db(0, db_connection);
	}
	j_trace_leave(G_STRFUNC);
	return ret;
}
static void
j_backend_db_func_free(gpointer _data)
{
	JBackendOperation* data = _data;
	if (data)
	{
		g_slice_free(JBackendOperation, data);
	}
}

static gboolean
j_db_schema_create_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_SCHEMA_CREATE);
}
gboolean
j_db_internal_schema_create(gchar const* namespace, gchar const* name, bson_t const* schema, JBatch* batch, GError** error)
{
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_schema_create, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->in_param[2].ptr_const = schema;
	data->out_param[0].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_schema_create_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
}
static gboolean
j_db_schema_get_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_SCHEMA_GET);
}
gboolean
j_db_internal_schema_get(gchar const* namespace, gchar const* name, bson_t* schema, JBatch* batch, GError** error)
{
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_schema_get, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->out_param[0].ptr_const = schema;
	data->out_param[1].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_schema_get_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
}
static gboolean
j_db_schema_delete_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_SCHEMA_DELETE);
}
gboolean
j_db_internal_schema_delete(gchar const* namespace, gchar const* name, JBatch* batch, GError** error)
{
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_schema_delete, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->out_param[0].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_schema_delete_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
}
static gboolean
j_db_insert_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_INSERT);
}
gboolean
j_db_internal_insert(gchar const* namespace, gchar const* name, bson_t const* metadata, JBatch* batch, GError** error)
{
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_insert, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->in_param[2].ptr_const = metadata;
	data->out_param[0].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_insert_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
}
static gboolean
j_db_update_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_UPDATE);
}
gboolean
j_db_internal_update(gchar const* namespace, gchar const* name, bson_t const* selector, bson_t const* metadata, JBatch* batch, GError** error)
{
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_update, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->in_param[2].ptr_const = selector;
	data->in_param[3].ptr_const = metadata;
	data->out_param[0].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_update_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
}
static gboolean
j_db_delete_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_DELETE);
}
gboolean
j_db_internal_delete(gchar const* namespace, gchar const* name, bson_t const* selector, JBatch* batch, GError** error)
{
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_delete, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->in_param[2].ptr_const = selector;
	data->out_param[0].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_delete_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
}
static gboolean
j_db_query_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_QUERY);
}
gboolean
j_db_internal_query(gchar const* namespace, gchar const* name, bson_t const* selector, gpointer* iterator, JBatch* batch, GError** error)
{
	J_db_iterator_helper* helper;
	JOperation* op;
	JBackendOperation* data;

	j_trace_enter(G_STRFUNC, NULL);
	if (G_UNLIKELY(!iterator))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NULL, "iterator not set");
		goto _error;
	}
	helper = g_slice_new(J_db_iterator_helper);
	helper->initialized = FALSE;
	memset(&helper->bson, 0, sizeof(bson_t));
	*iterator = helper;
	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_query, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->in_param[2].ptr_const = selector;
	data->out_param[0].ptr_const = &helper->bson;
	data->out_param[1].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_query_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	j_trace_leave(G_STRFUNC);
	return TRUE;
_error:
	j_trace_leave(G_STRFUNC);
	return FALSE;
}
gboolean
j_db_internal_iterate(gpointer iterator, bson_t* metadata, GError** error)
{
	J_db_iterator_helper* helper = iterator;
	gboolean has_next;
	bson_t zerobson;

	j_trace_enter(G_STRFUNC, NULL);
	memset(&zerobson, 0, sizeof(bson_t));
	if (!helper->initialized)
	{
		if (G_UNLIKELY(!memcmp(&helper->bson, &zerobson, sizeof(bson_t))))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_INVALID, "iterator invalid");
			goto error2;
		}
		if (G_UNLIKELY(!j_bson_iter_init(&helper->iter, &helper->bson, error)))
		{
			goto _error;
		}
		helper->initialized = TRUE;
	}
	if (G_UNLIKELY(!j_bson_iter_next(&helper->iter, &has_next, error)))
	{
		goto _error;
	}
	if (G_UNLIKELY(!has_next))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		goto _error;
	}
	if (G_UNLIKELY(!j_bson_iter_copy_document(&helper->iter, metadata, error)))
	{
		goto _error;
	}
	j_trace_leave(G_STRFUNC);
	return TRUE;
_error:
	j_bson_destroy(&helper->bson);
error2:
	g_slice_free(J_db_iterator_helper, helper);
	j_trace_leave(G_STRFUNC);
	return FALSE;
}
