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

#include <julea.h>

static mongoc_client_t* backend_connection = NULL;

static gchar* backend_host = NULL;
static gchar* backend_database = NULL;

static
gboolean
backend_batch_start (gchar const* namespace, JSemanticsSafety safety, gpointer* data)
{
	bson_t index[1];
	mongoc_bulk_operation_t* bulk_op;
	mongoc_collection_t* m_collection;
	mongoc_index_opt_t m_index_opt[1];
	mongoc_write_concern_t* write_concern;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	bson_init(index);
	bson_append_int32(index, "key", -1, 1);

	mongoc_index_opt_init(m_index_opt);
	m_index_opt->unique = TRUE;

	write_concern = mongoc_write_concern_new();

	if (safety != J_SEMANTICS_SAFETY_NONE)
	{
		mongoc_write_concern_set_w(write_concern, MONGOC_WRITE_CONCERN_W_DEFAULT);

		if (safety == J_SEMANTICS_SAFETY_STORAGE)
		{
			mongoc_write_concern_set_journal(write_concern, TRUE);
		}
	}
	else
	{
		mongoc_write_concern_set_w(write_concern, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
	}

	/* FIXME cache */
	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);

	mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);

	bulk_op = mongoc_collection_create_bulk_operation(m_collection, FALSE, write_concern);

	mongoc_collection_destroy(m_collection);
	mongoc_write_concern_destroy(write_concern);

	bson_destroy(index);

	*data = bulk_op;

	return TRUE;
}

static
gboolean
backend_batch_execute (gpointer data)
{
	gboolean ret = FALSE;

	bson_t reply[1];
	mongoc_bulk_operation_t* bulk_op = data;

	g_return_val_if_fail(data != NULL, FALSE);

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

	return ret;
}

static
gboolean
backend_put (gpointer data, gchar const* key, bson_t const* value)
{
	bson_t document[1];
	bson_t selector[1];
	mongoc_bulk_operation_t* bulk_op = data;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

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

	return TRUE;
}

static
gboolean
backend_delete (gpointer data, gchar const* key)
{
	bson_t document[1];
	mongoc_bulk_operation_t* bulk_op = data;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);

	mongoc_bulk_operation_remove(bulk_op, document);

	bson_destroy(document);

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

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

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

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	bson_init(document);

	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);
	cursor = mongoc_collection_find_with_opts(m_collection, document, NULL, NULL);

	if (cursor != NULL)
	{
		ret = TRUE;
		*data = cursor;
	}

	mongoc_collection_destroy(m_collection);

	bson_destroy(document);

	return ret;
}

static
gboolean
backend_get_by_prefix (gchar const* namespace, gchar const* prefix, gpointer* data)
{
	gboolean ret = FALSE;

	bson_t document[1];
	mongoc_collection_t* m_collection;
	mongoc_cursor_t* cursor;
	g_autofree gchar* escaped_prefix = NULL;
	g_autofree gchar* regex_prefix = NULL;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	escaped_prefix = g_regex_escape_string(prefix, -1);
	regex_prefix = g_strdup_printf("^%s", escaped_prefix);

	bson_init(document);
	bson_append_regex(document, "key", -1, regex_prefix, NULL);

	m_collection = mongoc_client_get_collection(backend_connection, backend_database, namespace);
	cursor = mongoc_collection_find_with_opts(m_collection, document, NULL, NULL);

	if (cursor != NULL)
	{
		ret = TRUE;
		*data = cursor;
	}

	mongoc_collection_destroy(m_collection);

	bson_destroy(document);

	return ret;
}

static
gboolean
backend_iterate (gpointer data, bson_t* result_out)
{
	bson_t const* result;
	bson_iter_t iter;
	mongoc_cursor_t* cursor = data;

	gboolean ret = FALSE;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

	/* FIXME */
	if (mongoc_cursor_next(cursor, &result))
	{
		if (bson_iter_init_find(&iter, result, "value") && bson_iter_type(&iter) == BSON_TYPE_DOCUMENT)
		{
			bson_value_t const* value;

			value = bson_iter_value(&iter);
			bson_init_static(result_out, value->value.v_doc.data, value->value.v_doc.data_len);

			ret = TRUE;
		}
	}
	else
	{
		mongoc_cursor_destroy(cursor);
	}

	return ret;
}

static
gboolean
backend_init (gchar const* path)
{
	mongoc_uri_t* uri;

	gboolean ret = FALSE;
	g_auto(GStrv) split = NULL;

	g_return_val_if_fail(path != NULL, FALSE);

	mongoc_init();
	mongoc_log_set_handler(NULL, NULL);

	split = g_strsplit(path, ":", 0);

	backend_host = g_strdup(split[0]);
	backend_database = g_strdup(split[1]);

	g_return_val_if_fail(backend_host != NULL, FALSE);
	g_return_val_if_fail(backend_database != NULL, FALSE);

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

	return TRUE;
}

static
void
backend_fini (void)
{
	mongoc_client_destroy(backend_connection);

	g_free(backend_database);
	g_free(backend_host);

	mongoc_cleanup();
}

static
JBackend mongodb_backend = {
	.type = J_BACKEND_TYPE_KV,
	.kv = {
		.init = backend_init,
		.fini = backend_fini,
		.batch_start = backend_batch_start,
		.batch_execute = backend_batch_execute,
		.put = backend_put,
		.delete = backend_delete,
		.get = backend_get,
		.get_all = backend_get_all,
		.get_by_prefix = backend_get_by_prefix,
		.iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	if (type == J_BACKEND_TYPE_KV)
	{
		backend = &mongodb_backend;
	}

	return backend;
}
