/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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

#define _POSIX_C_SOURCE 200809L

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <mongoc.h>

#include <jbackend.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

static bson_t backend_value[1];

static mongoc_client_t* backend_connection = NULL;

static gchar* backend_host = NULL;
static gchar* backend_database = NULL;

/*
static
void
backend_set_write_concern (mongoc_write_concern_t* write_concern, JSemantics* semantics)
{
	g_return_if_fail(write_concern != NULL);
	g_return_if_fail(semantics != NULL);

	j_trace_enter(G_STRFUNC);

	if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) != J_SEMANTICS_SAFETY_NONE)
	{
		mongoc_write_concern_set_w(write_concern, 1);

		if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_STORAGE)
		{
			mongoc_write_concern_set_journal(write_concern, TRUE);
		}
	}

	j_trace_leave(G_STRFUNC);
}
*/

static
gboolean
backend_batch_start (gchar const* namespace, gpointer* data)
{
	bson_t index[1];
	mongoc_bulk_operation_t* bulk_op;
	mongoc_collection_t* m_collection;
	mongoc_index_opt_t m_index_opt[1];

	j_trace_enter(G_STRFUNC);

	bson_init(index);
	bson_append_int32(index, "key", -1, 1);

	mongoc_index_opt_init(m_index_opt);
	m_index_opt->unique = TRUE;

	/* FIXME */
	//write_concern = mongoc_write_concern_new();
	//j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	/* FIXME cache */
	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);

	mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);

	bulk_op = mongoc_collection_create_bulk_operation(m_collection, FALSE, NULL /*write_concern*/);

	mongoc_collection_destroy(m_collection);

	bson_destroy(index);

	*data = bulk_op;

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_batch_execute (gpointer data)
{
	gboolean ret = FALSE;

	bson_t reply[1];
	mongoc_bulk_operation_t* bulk_op = data;

	j_trace_enter(G_STRFUNC);

	ret = mongoc_bulk_operation_execute(bulk_op, reply, NULL);

	/*
	if (!ret)
	{
		bson_t error[1];

		mongo_cmd_get_last_error(mongo_connection, store->name, error);
		bson_print(error);
		bson_destroy(error);
	}
	*/

	mongoc_bulk_operation_destroy(bulk_op);
	bson_destroy(reply);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_put (gchar const* key, bson_t const* value, gpointer data)
{
	bson_t document[1];
	bson_t selector[1];
	mongoc_bulk_operation_t* bulk_op = data;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	/* FIXME */
	//write_concern = mongoc_write_concern_new();
	//j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);
	bson_append_document(document, "value", -1, value);

	bson_init(selector);
	bson_append_utf8(selector, "key", -1, key, -1);

	/* FIXME use insert when possible */
	//mongoc_bulk_operation_insert(bulk_op, document);
	mongoc_bulk_operation_replace_one(bulk_op, selector, document, TRUE);

	/*
	if (!ret)
	{
		bson_t error[1];

		mongo_cmd_get_last_error(mongo_connection, store->name, error);
		bson_print(error);
		bson_destroy(error);
	}
	*/

	bson_destroy(selector);
	bson_destroy(document);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_delete (gchar const* key, gpointer data)
{
	bson_t document[1];
	mongoc_bulk_operation_t* bulk_op = data;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);

	/* FIXME */
	//write_concern = mongoc_write_concern_new();
	//j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	mongoc_bulk_operation_remove(bulk_op, document);

	bson_destroy(document);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_get (gchar const* namespace, gchar const* key, bson_t* result_out)
{
	gboolean ret = FALSE;

	bson_t document[1];
	bson_t opts[1];
	bson_t const* result;
	mongoc_collection_t* m_collection;
	mongoc_cursor_t* cursor;

	j_trace_enter(G_STRFUNC);

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);

	bson_init(opts);
	bson_append_int32(opts, "limit", -1, 1);

	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);
	cursor = mongoc_collection_find_with_opts(m_collection, document, opts, NULL);

	while (mongoc_cursor_next(cursor, &result))
	{
		bson_iter_t iter;

		if (bson_iter_init_find(&iter, result, "value") && bson_iter_type(&iter) == BSON_TYPE_DOCUMENT)
		{
			bson_t tmp[1];
			bson_value_t const* value;

			value = bson_iter_value(&iter);
			bson_init_static(tmp, value->value.v_doc.data, value->value.v_doc.data_len);

			ret = TRUE;
			bson_copy_to(tmp, result_out);

			break;
		}
	}

	bson_destroy(opts);
	bson_destroy(document);

	mongoc_cursor_destroy(cursor);
	mongoc_collection_destroy(m_collection);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_get_all (gchar const* namespace, gpointer* data)
{
	gboolean ret = FALSE;

	bson_t document[1];
	mongoc_collection_t* m_collection;
	mongoc_cursor_t* cursor;

	j_trace_enter(G_STRFUNC);

	bson_init(document);

	/* FIXME */
	//write_concern = mongoc_write_concern_new();
	//j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);
	cursor = mongoc_collection_find_with_opts(m_collection, document, NULL, NULL);

	if (cursor != NULL)
	{
		ret = TRUE;
		*data = cursor;
	}

	mongoc_collection_destroy(m_collection);

	bson_destroy(document);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_get_by_value (gchar const* namespace, bson_t const* value, gpointer* data)
{
	gboolean ret = FALSE;

	bson_t document[1];
	bson_iter_t iter;
	mongoc_collection_t* m_collection;
	mongoc_cursor_t* cursor;

	j_trace_enter(G_STRFUNC);

	bson_init(document);

	if (bson_iter_init(&iter, value))
	{
		while (bson_iter_next(&iter))
		{
			bson_value_t const* tmp;
			gchar* key;

			tmp = bson_iter_value(&iter);
			key = g_strdup_printf("value.%s", bson_iter_key(&iter));
			bson_append_value(document, key, -1, tmp);
			g_free(key);
		}
	}

	/* FIXME */
	//write_concern = mongoc_write_concern_new();
	//j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);
	cursor = mongoc_collection_find_with_opts(m_collection, document, NULL, NULL);

	if (cursor != NULL)
	{
		ret = TRUE;
		*data = cursor;
	}

	mongoc_collection_destroy(m_collection);

	bson_destroy(document);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_iterate (gpointer data, bson_t const** result_out)
{
	bson_t const* result;
	bson_iter_t iter;
	mongoc_cursor_t* cursor = data;

	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC);

	/* FIXME */
	if (mongoc_cursor_next(cursor, &result))
	{
		if (bson_iter_init_find(&iter, result, "value") && bson_iter_type(&iter) == BSON_TYPE_DOCUMENT)
		{
			bson_value_t const* value;

			value = bson_iter_value(&iter);
			/* FIXME global variable */
			bson_init_static(backend_value, value->value.v_doc.data, value->value.v_doc.data_len);

			ret = TRUE;
			*result_out = backend_value;
		}
	}
	else
	{
		mongoc_cursor_destroy(cursor);
	}

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_init (gchar const* path)
{
	mongoc_uri_t* uri;

	gboolean ret = FALSE;
	gchar** split;

	j_trace_enter(G_STRFUNC);

	mongoc_init();
	mongoc_log_set_handler(NULL, NULL);

	split = g_strsplit(path, ":", 0);

	/* FIXME error handling */
	backend_host = g_strdup(split[0]);
	backend_database = g_strdup(split[1]);

	g_strfreev(split);

	uri = mongoc_uri_new_for_host_port(backend_host, 27017);
	backend_connection = mongoc_client_new_from_uri(uri);
	mongoc_uri_destroy(uri);

	if (backend_connection != NULL)
	{
		ret = mongoc_client_get_server_status(backend_connection, NULL, NULL, NULL);
	}

	if (!ret)
	{
		g_critical("Can not connect to MongoDB %s.", backend_host);
	}

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);

	mongoc_client_destroy(backend_connection);

	g_free(backend_database);
	g_free(backend_host);

	mongoc_cleanup();

	j_trace_leave(G_STRFUNC);
}

static
JBackend mongodb_backend = {
	.type = J_BACKEND_TYPE_META,
	.u.meta = {
		.init = backend_init,
		.fini = backend_fini,
		.thread_init = NULL,
		.thread_fini = NULL,
		.batch_start = backend_batch_start,
		.batch_execute = backend_batch_execute,
		.put = backend_put,
		.delete = backend_delete,
		.get = backend_get,
		.get_all = backend_get_all,
		.get_by_value = backend_get_by_value,
		.iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	j_trace_enter(G_STRFUNC);

	if (type == J_BACKEND_TYPE_META)
	{
		backend = &mongodb_backend;
	}

	j_trace_leave(G_STRFUNC);

	return backend;
}
