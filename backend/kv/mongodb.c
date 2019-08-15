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

#define _POSIX_C_SOURCE 200809L

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <mongoc.h>

#include <julea.h>

struct JMongoDBBatch
{
	mongoc_bulk_operation_t* bulk_op;
	gchar* namespace;
};

typedef struct JMongoDBBatch JMongoDBBatch;

static mongoc_client_t* backend_connection = NULL;

static gchar* backend_host = NULL;
static gchar* backend_database = NULL;

static
gboolean
backend_batch_start (gchar const* namespace, JSemantics* semantics, gpointer* data)
{
	JMongoDBBatch* batch;
	bson_t command[1];
	bson_t index[1];
	bson_t indexes[1];
	bson_t key[1];
	bson_t opts[1];
	bson_t reply[1];
	mongoc_bulk_operation_t* bulk_op;
	mongoc_collection_t* m_collection;
	mongoc_database_t* m_database;
	mongoc_write_concern_t* write_concern;

	gchar* index_name;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	bson_init(key);
	bson_append_int32(key, "key", -1, 1);

	index_name = mongoc_collection_keys_to_index_string(key);

	bson_init(command);
	bson_append_utf8(command, "createIndexes", -1, namespace, -1);
	bson_append_array_begin(command, "indexes", -1, indexes);
	bson_append_document_begin(indexes, "0", -1, index);
	bson_append_document(index, "key", -1, key);
	bson_append_utf8(index, "name", -1, index_name, -1);
	bson_append_bool(index, "unique", -1, TRUE);
	bson_append_document_end(indexes, index);
	bson_append_array_end(command, indexes);

	bson_destroy(key);
	bson_free(index_name);

	write_concern = mongoc_write_concern_new();

	if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) != J_SEMANTICS_SAFETY_NONE)
	{
		mongoc_write_concern_set_w(write_concern, MONGOC_WRITE_CONCERN_W_DEFAULT);

		if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_STORAGE)
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
	m_database = mongoc_client_get_database(backend_connection, backend_database);

	mongoc_database_write_command_with_opts(m_database, command, NULL, reply, NULL);

	bson_init(opts);
	mongoc_write_concern_append(write_concern, opts);

	bulk_op = mongoc_collection_create_bulk_operation_with_opts(m_collection, opts);

	bson_free(opts);

	mongoc_collection_destroy(m_collection);
	mongoc_database_destroy(m_database);
	mongoc_write_concern_destroy(write_concern);

	bson_destroy(command);
	bson_destroy(reply);

	batch = g_slice_new(JMongoDBBatch);
	batch->bulk_op = bulk_op;
	batch->namespace = g_strdup(namespace);

	*data = batch;

	return TRUE;
}

static
gboolean
backend_batch_execute (gpointer data)
{
	gboolean ret = FALSE;

	JMongoDBBatch* batch = data;

	bson_t reply[1];

	g_return_val_if_fail(data != NULL, FALSE);

	ret = mongoc_bulk_operation_execute(batch->bulk_op, reply, NULL);

	/*
	if (!ret)
	{
		bson_t error[1];

		mongo_cmd_get_last_error(mongo_connection, store->name, error);
		bson_print(error);
		bson_destroy(error);
	}
	*/

	mongoc_bulk_operation_destroy(batch->bulk_op);
	bson_destroy(reply);
	g_free(batch->namespace);
	g_slice_free(JMongoDBBatch, batch);

	return ret;
}

static
gboolean
backend_put (gpointer data, gchar const* key, gconstpointer value, guint32 len)
{
	JMongoDBBatch* batch = data;
	bson_t document[1];
	bson_t selector[1];

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);
	bson_append_binary(document, "value", -1, BSON_SUBTYPE_BINARY, value, len);

	bson_init(selector);
	bson_append_utf8(selector, "key", -1, key, -1);

	/* FIXME use insert when possible */
	//mongoc_bulk_operation_insert(batch->bulk_op, document);
	mongoc_bulk_operation_replace_one(batch->bulk_op, selector, document, TRUE);

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
	JMongoDBBatch* batch = data;
	bson_t document[1];

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);

	mongoc_bulk_operation_remove(batch->bulk_op, document);

	bson_destroy(document);

	return TRUE;
}

static
gboolean
backend_get (gpointer data, gchar const* key, gpointer* value, guint32* len)
{
	gboolean ret = FALSE;

	JMongoDBBatch* batch = data;

	bson_t document[1];
	bson_t opts[1];
	bson_t const* result;
	mongoc_collection_t* m_collection;
	mongoc_cursor_t* cursor;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	bson_init(document);
	bson_append_utf8(document, "key", -1, key, -1);

	bson_init(opts);
	bson_append_int32(opts, "limit", -1, 1);

	m_collection = mongoc_client_get_collection(backend_connection, backend_database, batch->namespace);
	cursor = mongoc_collection_find_with_opts(m_collection, document, opts, NULL);

	while (mongoc_cursor_next(cursor, &result))
	{
		bson_iter_t iter;

		if (bson_iter_init_find(&iter, result, "value") && bson_iter_type(&iter) == BSON_TYPE_BINARY)
		{
			bson_value_t const* bv;

			bv = bson_iter_value(&iter);
			*value = g_memdup(bv->value.v_binary.data, bv->value.v_binary.data_len);
			*len = bv->value.v_binary.data_len;

			ret = TRUE;

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
backend_iterate (gpointer data, gchar const** key, gconstpointer* value, guint32* len)
{
	bson_t const* result;
	bson_iter_t iter;
	mongoc_cursor_t* cursor = data;

	gboolean ret = FALSE;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	/* FIXME */
	if (mongoc_cursor_next(cursor, &result))
	{
		bson_iter_init(&iter, result);

		while (bson_iter_next(&iter))
		{
			gchar const* key_;

			key_ = bson_iter_key(&iter);

			if (g_strcmp0(key_, "key") == 0 && bson_iter_type(&iter) == BSON_TYPE_UTF8)
			{
				*key = bson_iter_utf8(&iter, NULL);
			}
			else if (g_strcmp0(key_, "value") == 0 && bson_iter_type(&iter) == BSON_TYPE_BINARY)
			{
				bson_value_t const* bv;

				bv = bson_iter_value(&iter);
				*value = bv->value.v_binary.data;
				*len = bv->value.v_binary.data_len;
			}
		}

		ret = TRUE;
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
	.component = J_BACKEND_COMPONENT_CLIENT,
	.kv = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_batch_start = backend_batch_start,
		.backend_batch_execute = backend_batch_execute,
		.backend_put = backend_put,
		.backend_delete = backend_delete,
		.backend_get = backend_get,
		.backend_get_all = backend_get_all,
		.backend_get_by_prefix = backend_get_by_prefix,
		.backend_iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &mongodb_backend;
}
