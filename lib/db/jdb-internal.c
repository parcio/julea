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
#include <julea-internal.h>
#include <core/jerror.h>
struct J_db_iterator_helper
{
	bson_t bson;
	bson_iter_t iter;
	gboolean initialized;
};
typedef struct J_db_iterator_helper J_db_iterator_helper;
static gboolean
j_backend_db_func_exec(JList* operations, JSemantics* semantics, JMessageType type)
{
	JBackend_db_operation_data* data = NULL;
	gboolean ret = TRUE;
	GSocketConnection* db_connection;
	g_autoptr(JListIterator) iter_send = NULL;
	g_autoptr(JListIterator) iter_recieve = NULL;
	g_autoptr(JMessage) message = NULL;
	g_autoptr(JMessage) reply = NULL;
	JBackend* db_backend = j_db_backend();
	gpointer batch = NULL;
	GError* error = NULL;
	if (db_backend == NULL || JULEA_TEST_MOCKUP)
		message = j_message_new(type, 0);
	iter_send = j_list_iterator_new(operations);
	while (j_list_iterator_next(iter_send))
	{
		data = j_list_iterator_get(iter_send);
		if (db_backend != NULL && !JULEA_TEST_MOCKUP)
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
				*((void**)data->out_param[data->out_param_count - 1].ptr) = g_error_copy(error);
			else
				ret = data->backend_func(db_backend, batch, data) && ret;
		}
		else
		{
			ret = j_backend_db_message_from_data(message, data->in_param, data->in_param_count) && ret;
		}
	}
	if (db_backend != NULL && !JULEA_TEST_MOCKUP)
	{
		if (data != NULL)
		{
			if (!error)
				ret = db_backend->db.backend_batch_execute(batch, NULL) && ret;
			else
				g_error_free(error);
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
			ret = j_backend_db_message_to_data(reply, data->out_param, data->out_param_count) && ret;
		}
		j_connection_pool_push_db(0, db_connection);
	}
	return ret;
}
static void
j_backend_db_func_free(gpointer _data)
{
	JBackend_db_operation_data* data = _data;
	if (data)
		g_slice_free(JBackend_db_operation_data, data);
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
	JBackend_db_operation_data* data;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_schema_create_params, sizeof(JBackend_db_operation_data));
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
	JBackend_db_operation_data* data;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_schema_get_params, sizeof(JBackend_db_operation_data));
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
	JBackend_db_operation_data* data;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_schema_delete_params, sizeof(JBackend_db_operation_data));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->out_param[0].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_schema_delete_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
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
	JBackend_db_operation_data* data;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_insert_params, sizeof(JBackend_db_operation_data));
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
	JBackend_db_operation_data* data;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_update_params, sizeof(JBackend_db_operation_data));
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
	JBackend_db_operation_data* data;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_delete_params, sizeof(JBackend_db_operation_data));
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
	return TRUE;
}
static gboolean
j_db_get_all_exec(JList* operations, JSemantics* semantics)
{
	return j_backend_db_func_exec(operations, semantics, J_MESSAGE_DB_GET_ALL);
}
gboolean
j_db_internal_query(gchar const* namespace, gchar const* name, bson_t const* selector, gpointer* iterator, JBatch* batch, GError** error)
{
	J_db_iterator_helper* helper;
	JOperation* op;
	JBackend_db_operation_data* data;
	j_goto_error_backend(!iterator, JULEA_BACKEND_ERROR_ITERATOR_NULL, "");
	helper = g_slice_new(J_db_iterator_helper);
	helper->initialized = FALSE;
	memset(&helper->bson, 0, sizeof(bson_t));
	*iterator = helper;
	data = g_slice_new(JBackend_db_operation_data);
	memcpy(data, &j_db_get_all_params, sizeof(JBackend_db_operation_data));
	data->in_param[0].ptr_const = namespace;
	data->in_param[1].ptr_const = name;
	data->in_param[2].ptr_const = selector;
	data->out_param[0].ptr_const = &helper->bson;
	data->out_param[1].ptr_const = error;
	op = j_operation_new();
	op->key = namespace;
	op->data = data;
	op->exec_func = j_db_get_all_exec;
	op->free_func = j_backend_db_func_free;
	j_batch_add(batch, op);
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_internal_iterate(gpointer iterator, bson_t* metadata, GError** error)
{
	gint ret;
	const uint8_t* data;
	uint32_t length;
	J_db_iterator_helper* helper = iterator;
	bson_t zerobson;
	memset(&zerobson, 0, sizeof(bson_t));
	if (!helper->initialized)
	{
		if (!memcmp(&helper->bson, &zerobson, sizeof(bson_t)))
		{
			g_set_error(error, JULEA_BACKEND_ERROR, JULEA_BACKEND_ERROR_BSON_INVALID, "");
			goto error2;
		}
		bson_iter_init(&helper->iter, &helper->bson);
		helper->initialized = TRUE;
	}
	ret = bson_iter_next(&helper->iter);
	j_goto_error_backend(!ret, JULEA_BACKEND_ERROR_ITERATOR_NO_MORE_ELEMENTS, "");
	ret = BSON_ITER_HOLDS_DOCUMENT(&helper->iter);
	j_goto_error_backend(!ret, JULEA_BACKEND_ERROR_BSON_INVALID_TYPE, bson_iter_type(&helper->iter));
	bson_iter_document(&helper->iter, &length, &data);
	bson_init_static(metadata, data, length);
	return TRUE;
_error:
	bson_destroy(&helper->bson);
error2:
	g_slice_free(J_db_iterator_helper, helper);
	return FALSE;
}
