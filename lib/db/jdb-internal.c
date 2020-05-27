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

#include <db/jdb-error.h>
#include <db/jdb-internal.h>

#include <julea.h>
#include "../../backend/db/jbson.c"

struct JDBIteratorHelper
{
	bson_t bson;
	bson_iter_t iter;
	gboolean initialized;
};

typedef struct JDBIteratorHelper JDBIteratorHelper;

GQuark
j_db_error_quark(void)
{
	J_TRACE_FUNCTION(NULL);

	return g_quark_from_static_string("j-db-error-quark");
}

static gboolean
j_backend_db_func_exec(JList* operations, JSemantics* semantics, JMessageType type)
{
	J_TRACE_FUNCTION(NULL);

	JBackendOperation* data = NULL;
	gboolean ret = TRUE;
	GSocketConnection* db_connection;
	g_autoptr(JListIterator) iter_send = NULL;
	g_autoptr(JListIterator) iter_recieve = NULL;
	g_autoptr(JMessage) message = NULL;
	g_autoptr(JMessage) reply = NULL;
	JBackend* db_backend = j_db_get_backend();
	gpointer batch = NULL;
	GError* error = NULL;

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
				ret = j_backend_db_batch_start(db_backend, data->in_param[0].ptr, semantics, &batch, &error) && ret;
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
				ret = j_backend_db_batch_execute(db_backend, batch, NULL) && ret;
			}
			else
			{
				g_error_free(error);
			}
		}
	}
	else
	{
		db_connection = j_connection_pool_pop(J_BACKEND_TYPE_DB, 0);
		j_message_send(message, db_connection);
		reply = j_message_new_reply(message);
		j_message_receive(reply, db_connection);
		iter_recieve = j_list_iterator_new(operations);

		while (j_list_iterator_next(iter_recieve))
		{
			data = j_list_iterator_get(iter_recieve);
			ret = j_backend_operation_from_message(reply, data->out_param, data->out_param_count) && ret;
		}

		j_connection_pool_push(J_BACKEND_TYPE_DB, 0, db_connection);
	}

	return ret;
}

static void
j_backend_db_func_free(gpointer _data)
{
	J_TRACE_FUNCTION(NULL);

	JBackendOperation* data = _data;

	if (data)
	{
		for (guint i = 0; i < data->unref_func_count; i++)
		{
			if (data->unref_values[i])
			{
				(*data->unref_funcs[i])(data->unref_values[i]);
			}
		}

		g_slice_free(JBackendOperation, data);
	}
}

static gboolean
j_db_schema_create_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_SCHEMA_CREATE);
}

gboolean
j_db_internal_schema_create(JDBSchema* j_db_schema, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_schema_create, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_schema->namespace;
	data->in_param[1].ptr_const = j_db_schema->name;
	data->in_param[2].ptr_const = &j_db_schema->bson;
	data->out_param[0].ptr_const = error;

	data->unref_func_count = 1;
	data->unref_funcs[0] = (GDestroyNotify)j_db_schema_unref;
	data->unref_values[0] = j_db_schema_ref(j_db_schema);

	op = j_operation_new();
	op->key = j_db_schema->namespace;
	op->data = data;
	op->exec_func = j_db_schema_create_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

static gboolean
j_db_schema_get_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_SCHEMA_GET);
}

gboolean
j_db_internal_schema_get(JDBSchema* j_db_schema, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_schema_get, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_schema->namespace;
	data->in_param[1].ptr_const = j_db_schema->name;
	data->out_param[0].ptr_const = &j_db_schema->bson;
	data->out_param[1].ptr_const = error;

	data->unref_func_count = 1;
	data->unref_funcs[0] = (GDestroyNotify)j_db_schema_unref;
	data->unref_values[0] = j_db_schema_ref(j_db_schema);

	op = j_operation_new();
	op->key = j_db_schema->namespace;
	op->data = data;
	op->exec_func = j_db_schema_get_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

static gboolean
j_db_schema_delete_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_SCHEMA_DELETE);
}

gboolean
j_db_internal_schema_delete(JDBSchema* j_db_schema, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_schema_delete, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_schema->namespace;
	data->in_param[1].ptr_const = j_db_schema->name;
	data->out_param[0].ptr_const = error;

	data->unref_func_count = 1;
	data->unref_funcs[0] = (GDestroyNotify)j_db_schema_unref;
	data->unref_values[0] = j_db_schema_ref(j_db_schema);

	op = j_operation_new();
	op->key = j_db_schema->namespace;
	op->data = data;
	op->exec_func = j_db_schema_delete_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

static gboolean
j_db_insert_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_INSERT);
}

gboolean
j_db_internal_insert(JDBEntry* j_db_entry, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_insert, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_entry->schema->namespace;
	data->in_param[1].ptr_const = j_db_entry->schema->name;
	data->in_param[2].ptr_const = &j_db_entry->bson;
	data->out_param[0].ptr_const = &j_db_entry->id;
	data->out_param[1].ptr_const = error;

	data->unref_func_count = 1;
	data->unref_funcs[0] = (GDestroyNotify)j_db_entry_unref;
	data->unref_values[0] = j_db_entry_ref(j_db_entry);

	op = j_operation_new();
	op->key = j_db_entry->schema->namespace;
	op->data = data;
	op->exec_func = j_db_insert_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

static gboolean
j_db_update_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_UPDATE);
}

gboolean
j_db_internal_update(JDBEntry* j_db_entry, JDBSelector* j_db_selector, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_update, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_entry->schema->namespace;
	data->in_param[1].ptr_const = j_db_entry->schema->name;
	data->in_param[2].ptr_const = j_db_selector_get_bson(j_db_selector);
	data->in_param[3].ptr_const = &j_db_entry->bson;
	data->out_param[0].ptr_const = error;

	data->unref_func_count = 2;
	data->unref_funcs[0] = (GDestroyNotify)j_db_entry_unref;
	data->unref_funcs[1] = (GDestroyNotify)j_db_selector_unref;
	data->unref_values[0] = j_db_entry_ref(j_db_entry);
	data->unref_values[1] = j_db_selector_ref(j_db_selector);

	op = j_operation_new();
	op->key = j_db_entry->schema->namespace;
	op->data = data;
	op->exec_func = j_db_update_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

static gboolean
j_db_delete_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_DELETE);
}

gboolean
j_db_internal_delete(JDBEntry* j_db_entry, JDBSelector* j_db_selector, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_delete, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_entry->schema->namespace;
	data->in_param[1].ptr_const = j_db_entry->schema->name;
	data->in_param[2].ptr_const = j_db_selector_get_bson(j_db_selector);
	data->out_param[0].ptr_const = error;

	data->unref_func_count = 2;
	data->unref_funcs[0] = (GDestroyNotify)j_db_entry_unref;
	data->unref_funcs[1] = (GDestroyNotify)j_db_selector_unref;
	data->unref_values[0] = j_db_entry_ref(j_db_entry);
	data->unref_values[1] = j_db_selector_ref(j_db_selector);

	op = j_operation_new();
	op->key = j_db_entry->schema->namespace;
	op->data = data;
	op->exec_func = j_db_delete_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

static gboolean
j_db_query_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_QUERY);
}

gboolean
j_db_internal_query(JDBSchema* j_db_schema, JDBSelector* j_db_selector, JDBIterator* j_db_iterator, JBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBIteratorHelper* helper;
	JOperation* op;
	JBackendOperation* data;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	helper = j_helper_alloc_aligned(128, sizeof(JDBIteratorHelper));
	helper->initialized = FALSE;
	memset(&helper->bson, 0, sizeof(bson_t));
	j_db_iterator->iterator = helper;

	data = g_slice_new(JBackendOperation);
	memcpy(data, &j_backend_operation_db_query, sizeof(JBackendOperation));
	data->in_param[0].ptr_const = j_db_schema->namespace;
	data->in_param[1].ptr_const = j_db_schema->name;
	data->in_param[2].ptr_const = j_db_selector_get_bson(j_db_selector);
	data->out_param[0].ptr_const = &helper->bson;
	data->out_param[1].ptr_const = error;

	data->unref_func_count = 3;
	data->unref_funcs[0] = (GDestroyNotify)j_db_schema_unref;
	data->unref_funcs[1] = (GDestroyNotify)j_db_selector_unref;
	data->unref_funcs[2] = (GDestroyNotify)j_db_iterator_unref;
	data->unref_values[0] = j_db_schema_ref(j_db_schema);
	data->unref_values[1] = j_db_selector_ref(j_db_selector);
	data->unref_values[2] = j_db_iterator_ref(j_db_iterator);

	op = j_operation_new();
	op->key = j_db_schema->namespace;
	op->data = data;
	op->exec_func = j_db_query_exec;
	op->free_func = j_backend_db_func_free;

	j_batch_add(batch, op);

	return TRUE;
}

gboolean
j_db_internal_iterate(JDBIterator* j_db_iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBIteratorHelper* helper = j_db_iterator->iterator;
	gboolean has_next;
	bson_t zerobson;

	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

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

	if (G_UNLIKELY(!j_bson_iter_copy_document(&helper->iter, &j_db_iterator->bson, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	j_bson_destroy(&helper->bson);

error2:
	g_free(helper);

	return FALSE;
}

bson_t*
j_db_selector_get_bson(JDBSelector* selector)
{
	J_TRACE_FUNCTION(NULL);

	if (selector && selector->bson_count > 0)
	{
		return &selector->bson;
	}

	return NULL;
}
