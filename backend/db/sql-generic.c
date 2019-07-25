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

#ifndef SQL_GENERIC_BACKEND_H
#define SQL_GENERIC_BACKEND_H

// FIXME clean up

/*
 * this file does not care which sql-database is actually in use, and uses only defines sql-syntax to allow fast and easy implementations for any new sql-database backend
*/
#ifndef j_sql_bind_blob
#error "j_sql_bind_blob undefined"
#endif
#ifndef j_sql_bind_double
#error "j_sql_bind_double undefined"
#endif
#ifndef j_sql_bind_int
#error "j_sql_bind_int undefined"
#endif
#ifndef j_sql_bind_int64
#error "j_sql_bind_int64 undefined"
#endif
#ifndef j_sql_bind_null
#error "j_sql_bind_null undefined"
#endif
#ifndef j_sql_bind_text
#error "j_sql_bind_text undefined"
#endif
#ifndef j_sql_check
#error "j_sql_check undefined"
#endif
#ifndef j_sql_column_float32
#error "j_sql_column_float32 undefined"
#endif
#ifndef j_sql_column_float64
#error "j_sql_column_float64 undefined"
#endif
#ifndef j_sql_column_sint32
#error "j_sql_column_sint32 undefined"
#endif
#ifndef j_sql_column_sint64
#error "j_sql_column_sint64 undefined"
#endif
#ifndef j_sql_column_text
#error "j_sql_column_text undefined"
#endif
#ifndef j_sql_column_uint32
#error "j_sql_column_uint32 undefined"
#endif
#ifndef j_sql_column_uint64
#error "j_sql_column_uint64 undefined"
#endif
#ifndef j_sql_constraint_check
#error "j_sql_constraint_check undefined"
#endif
#ifndef j_sql_done
#error "j_sql_done undefined"
#endif
#ifndef j_sql_exec_and_get_number
#error "j_sql_exec_and_get_number undefined"
#endif
#ifndef j_sql_exec_or_error
#error "j_sql_exec_or_error undefined"
#endif
#ifndef j_sql_finalize
#error "j_sql_finalize undefined"
#endif
#ifndef j_sql_loop
#error "j_sql_loop undefined"
#endif
#ifndef j_sql_prepare
#error "j_sql_prepare undefined"
#endif
#ifndef j_sql_reset
#error "j_sql_reset undefined"
#endif
#ifndef j_sql_reset_constraint
#error "j_sql_reset_constraint undefined"
#endif
#ifndef j_sql_statement_type
#error "j_sql_statement_type undefined"
#endif
#ifndef j_sql_step
#error "j_sql_step undefined"
#endif
#ifndef j_sql_step_and_reset_check_done
#error "j_sql_step_and_reset_check_done undefined"
#endif
#ifndef j_sql_step_and_reset_check_done_constraint
#error "j_sql_step_and_reset_check_done_constraint undefined"
#endif
#ifndef j_sql_step_constraint
#error "j_sql_step_constraint undefined"
#endif
struct JSqlCacheNamespaces
{
	GHashTable* namespaces;
};
typedef struct JSqlCacheNamespaces JSqlCacheNamespaces;
struct JSqlCacheNames
{
	GHashTable* names;
};
typedef struct JSqlCacheNames JSqlCacheNames;
struct JSqlCacheSQLQueries
{
	GHashTable* queries;
};
typedef struct JSqlCacheSQLQueries JSqlCacheSQLQueries;
struct JSqlCacheSQLPrepared
{
	GString* sql;
	j_sql_statement_type stmt;
	guint variables_count;
	GHashTable* variables_index;
	GHashTable* variables_type;
	gboolean initialized;
};
typedef struct JSqlCacheSQLPrepared JSqlCacheSQLPrepared;
struct JSqlBatch
{
	const gchar* namespace;
};
typedef struct JSqlBatch JSqlBatch;
struct JSqlIterator
{
	char* namespace;
	char* name;
	GArray* arr;
	guint index;
};
typedef struct JSqlIterator JSqlIterator;
static JSqlCacheNamespaces* cacheNamespaces = NULL;
static j_sql_statement_type stmt_schema_structure_create = NULL;
static j_sql_statement_type stmt_schema_structure_get = NULL;
static j_sql_statement_type stmt_schema_structure_delete = NULL;
static j_sql_statement_type stmt_transaction_abort = NULL;
static j_sql_statement_type stmt_transaction_begin = NULL;
static j_sql_statement_type stmt_transaction_commit = NULL;
#define j_sql_transaction_begin() j_sql_step_and_reset_check_done(stmt_transaction_begin)
#define j_sql_transaction_commit() j_sql_step_and_reset_check_done(stmt_transaction_commit)
#define j_sql_transaction_abort() j_sql_step_and_reset_check_done(stmt_transaction_abort)
static void
freeJSqlIterator(gpointer ptr)
{
	JSqlIterator* iter = ptr;
	if (ptr)
	{
		g_free(iter->namespace);
		g_free(iter->name);
		g_array_free(iter->arr, TRUE);
		g_free(iter);
	}
}
static void
freeJSqlCacheNamespaces(void* ptr)
{
	JSqlCacheNamespaces* p = ptr;
	if (ptr)
	{
		if (p->namespaces)
			g_hash_table_destroy(p->namespaces);
		g_free(p);
	}
}
static void
freeJSqlCacheNames(void* ptr)
{
	JSqlCacheNames* p = ptr;
	if (ptr)
	{
		if (p->names)
			g_hash_table_destroy(p->names);
		g_free(p);
	}
}
static void
freeJSqlCacheSQLQueries(void* ptr)
{
	JSqlCacheSQLQueries* p = ptr;
	if (ptr)
	{
		if (p->queries)
			g_hash_table_destroy(p->queries);
		g_free(p);
	}
}
static void
freeJSqlCacheSQLPrepared(void* ptr)
{
	JSqlCacheSQLPrepared* p = ptr;
	if (ptr)
	{
		if (p->variables_index)
			g_hash_table_destroy(p->variables_index);
		if (p->variables_type)
			g_hash_table_destroy(p->variables_type);
		if (p->sql)
			g_string_free(p->sql, TRUE);
		if (p->stmt)
			j_sql_finalize(p->stmt);
		g_free(p);
	}
}
static JSqlCacheSQLPrepared*
getCachePrepared(gchar const* namespace, gchar const* name, gchar const* query, GError** error)
{
	gint ret;
	JSqlCacheNames* cacheNames = NULL;
	JSqlCacheSQLQueries* cacheQueries = NULL;
	JSqlCacheSQLPrepared* cachePrepared = NULL;
	if (!cacheNamespaces)
	{
		cacheNamespaces = g_new0(JSqlCacheNamespaces, 1);
		cacheNamespaces->namespaces = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheNames);
	}
	cacheNames = g_hash_table_lookup(cacheNamespaces->namespaces, namespace);
	if (!cacheNames)
	{
		cacheNames = g_new0(JSqlCacheNames, 1);
		cacheNames->names = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheSQLQueries);
		ret = g_hash_table_insert(cacheNamespaces->namespaces, g_strdup(namespace), cacheNames);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_THREADING_ERROR, "");
	}
	cacheQueries = g_hash_table_lookup(cacheNames->names, name);
	if (!cacheQueries)
	{
		cacheQueries = g_new0(JSqlCacheSQLQueries, 1);
		cacheQueries->queries = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheSQLPrepared);
		ret = g_hash_table_insert(cacheNames->names, g_strdup(name), cacheQueries);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_THREADING_ERROR, "");
	}
	cachePrepared = g_hash_table_lookup(cacheQueries->queries, query);
	if (!cachePrepared)
	{
		cachePrepared = g_new0(JSqlCacheSQLPrepared, 1);
		ret = g_hash_table_insert(cacheQueries->queries, g_strdup(query), cachePrepared);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_THREADING_ERROR, "");
	}
	return cachePrepared;
_error:
	return NULL;
}
static void
deleteCachePrepared(gchar const* namespace, gchar const* name)
{
	JSqlCacheNames* cacheNames = NULL;
	if (!cacheNamespaces)
		return;
	cacheNames = g_hash_table_lookup(cacheNamespaces->namespaces, namespace);
	if (!cacheNames)
		return;
	g_hash_table_remove(cacheNames->names, name);
}
static gboolean
init_sql(void)
{
	GError** error = NULL;
	j_sql_exec_or_error(
		"CREATE TABLE IF NOT EXISTS schema_structure ("
		"namespace TEXT,"
		"name TEXT,"
		"value TEXT,"
		"PRIMARY KEY (namespace, name)"
		")",
		j_sql_done);
	j_sql_prepare("INSERT INTO schema_structure(namespace, name, value) VALUES (?1, ?2, ?3)", &stmt_schema_structure_create);
	j_sql_prepare("SELECT value FROM schema_structure WHERE namespace=?1 AND name=?2", &stmt_schema_structure_get);
	j_sql_prepare("DELETE FROM schema_structure WHERE namespace=?1 AND name=?2", &stmt_schema_structure_delete);
	j_sql_prepare("BEGIN TRANSACTION", &stmt_transaction_begin);
	j_sql_prepare("COMMIT", &stmt_transaction_commit);
	j_sql_prepare("ROLLBACK", &stmt_transaction_abort);
	return TRUE;
_error:
	return FALSE;
}
static void
fini_sql(void)
{
	freeJSqlCacheNamespaces(cacheNamespaces);
	j_sql_finalize(stmt_schema_structure_create);
	j_sql_finalize(stmt_schema_structure_get);
	j_sql_finalize(stmt_schema_structure_delete);
	j_sql_finalize(stmt_transaction_abort);
	j_sql_finalize(stmt_transaction_begin);
	j_sql_finalize(stmt_transaction_commit);
}
static gboolean
backend_batch_start(gchar const* namespace, JSemanticsSafety safety, gpointer* _batch, GError** error)
{
	JSqlBatch* batch = *_batch = g_slice_new(JSqlBatch);
	j_goto_error_backend(!namespace, J_BACKEND_DB_ERROR_NAMESPACE_NULL, "");
	batch->namespace = namespace;
	(void)safety;
	return TRUE;
_error:
	g_slice_free(JSqlBatch, batch);
	*_batch = NULL;
	return FALSE;
}
static gboolean
backend_batch_execute(gpointer batch, GError** error)
{
	(void)error;
	g_slice_free(JSqlBatch, batch);
	return TRUE;
}
static gboolean
backend_schema_create(gpointer _batch, gchar const* name, bson_t const* schema, GError** error)
{
	JSqlBatch* batch = _batch;
	bson_iter_t iter;
	bson_iter_t iter_child;
	bson_iter_t iter_child2;
	JDBType type;
	gboolean first;
	guint i;
	gint ret;
	guint counter = 0;
	gboolean found_index = FALSE;
	char* json = NULL;
	GString* sql = g_string_new(NULL);
	j_sql_transaction_begin();
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	j_goto_error_backend(!schema, J_BACKEND_DB_ERROR_SCHEMA_NULL, "");
	g_string_append_printf(sql, "CREATE TABLE %s_%s ( _id INTEGER PRIMARY KEY", batch->namespace, name);
	ret = bson_iter_init(&iter, schema);
	j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
	while (bson_iter_next(&iter))
	{
		if (!g_strcmp0(bson_iter_key(&iter), "_index"))
		{
			found_index = TRUE;
		}
		else
		{
			counter++;
			g_string_append_printf(sql, ", %s", bson_iter_key(&iter));
			ret = BSON_ITER_HOLDS_INT32(&iter);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
			type = bson_iter_int32(&iter);
			switch (type)
			{
			case J_DB_TYPE_SINT32:
				g_string_append(sql, " INTEGER");
				break;
			case J_DB_TYPE_UINT32:
				g_string_append(sql, " INTEGER");
				break;
			case J_DB_TYPE_FLOAT32:
				g_string_append(sql, " REAL");
				break;
			case J_DB_TYPE_SINT64:
				g_string_append(sql, " INTEGER");
				break;
			case J_DB_TYPE_UINT64:
				g_string_append(sql, " UNSIGNED BIGINT");
				break;
			case J_DB_TYPE_FLOAT64:
				g_string_append(sql, " REAL");
				break;
			case J_DB_TYPE_STRING:
				g_string_append(sql, " TEXT");
				break;
			case J_DB_TYPE_BLOB:
				g_string_append(sql, " BLOB");
				break;
			case _J_DB_TYPE_COUNT:
			default:
				j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_DB_TYPE_INVALID, type);
			}
		}
	}
	g_string_append(sql, " )");
	j_goto_error_backend(!counter, J_BACKEND_DB_ERROR_SCHEMA_EMPTY, "");
	json = bson_as_json(schema, NULL);
	j_sql_bind_text(stmt_schema_structure_create, 1, batch->namespace, -1);
	j_sql_bind_text(stmt_schema_structure_create, 2, name, -1);
	j_sql_bind_text(stmt_schema_structure_create, 3, json, -1);
	j_sql_step_and_reset_check_done_constraint(stmt_schema_structure_create);
	j_sql_exec_or_error(sql->str, j_sql_done);
	bson_free(json);
	g_string_free(sql, TRUE);
	if (found_index)
	{
		i = 0;
		ret = bson_iter_init(&iter, schema);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		ret = bson_iter_find(&iter, "_index");
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_KEY_NOT_FOUND, "_index");
		ret = BSON_ITER_HOLDS_ARRAY(&iter);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		ret = bson_iter_recurse(&iter, &iter_child);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
		while (bson_iter_next(&iter_child))
		{
			sql = g_string_new(NULL);
			first = TRUE;
			g_string_append_printf(sql, "CREATE INDEX %s_%s_%d ON %s_%s ( ", batch->namespace, name, i, batch->namespace, name);
			ret = BSON_ITER_HOLDS_ARRAY(&iter_child);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter_child));
			ret = bson_iter_recurse(&iter_child, &iter_child2);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
			while (bson_iter_next(&iter_child2))
			{
				if (first)
					first = FALSE;
				else
					g_string_append(sql, ", ");
				g_string_append_printf(sql, "%s", bson_iter_utf8(&iter_child2, NULL));
			}
			g_string_append(sql, " )");
			j_sql_exec_or_error(sql->str, j_sql_done);
			g_string_free(sql, TRUE);
			i++;
		}
	}
	j_sql_transaction_commit();
	return TRUE;
_error:
	j_sql_transaction_abort();
	bson_free(json);
	g_string_free(sql, TRUE);
	return FALSE;
}
static gboolean
backend_schema_get(gpointer _batch, gchar const* name, bson_t* schema, GError** error)
{
	JSqlBatch* batch = _batch;
	gint retsql;
	guint ret = FALSE;
	const char* json = NULL;
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	j_sql_bind_text(stmt_schema_structure_get, 1, batch->namespace, -1);
	j_sql_bind_text(stmt_schema_structure_get, 2, name, -1);
	j_sql_step(stmt_schema_structure_get, retsql)
	{
		if (schema)
		{
			json = j_sql_column_text(stmt_schema_structure_get, 0);
			j_goto_error_backend(json == NULL, J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND, "");
			j_goto_error_backend(!strlen(json), J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND, "");
			bson_init_from_json(schema, json, -1, NULL);
		}
		ret = TRUE;
	}
	j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND, "");
	j_sql_reset(stmt_schema_structure_get);
	return TRUE;
_error:
	j_sql_reset(stmt_schema_structure_get);
	return FALSE;
}
static gboolean
backend_schema_delete(gpointer _batch, gchar const* name, GError** error)
{
	JSqlBatch* batch = _batch;
	GString* sql = g_string_new(NULL);
	gint ret;
	j_sql_transaction_begin();
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	deleteCachePrepared(batch->namespace, name);
	ret = backend_schema_get(batch, name, NULL, error);
	j_goto_error_subcommand(!ret);
	g_string_append_printf(sql, "DROP TABLE %s_%s", batch->namespace, name);
	j_sql_bind_text(stmt_schema_structure_delete, 1, batch->namespace, -1);
	j_sql_bind_text(stmt_schema_structure_delete, 2, name, -1);
	j_sql_step_and_reset_check_done(stmt_schema_structure_delete);
	j_sql_exec_or_error(sql->str, j_sql_done);
	j_sql_transaction_commit();
	g_string_free(sql, TRUE);
	return TRUE;
_error:
	j_sql_transaction_abort();
	g_string_free(sql, TRUE);
	return false;
}
static gboolean
insert_helper(JSqlCacheSQLPrepared* prepared, bson_iter_t* iter, GError** error)
{
	uint32_t binary_len;
	const uint8_t* binary;
	bson_type_t type;
	guint i;
	guint index;
	guint count = 0;
	for (i = 0; i < prepared->variables_count; i++)
		j_sql_bind_null(prepared->stmt, i + 1);
	while (bson_iter_next(iter))
	{
		type = bson_iter_type(iter);
		index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, bson_iter_key(iter)));
		j_goto_error_backend(!index, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, bson_iter_key(iter));
		switch (type)
		{
		case BSON_TYPE_DOUBLE:
			count++;
			j_sql_bind_double(prepared->stmt, index, bson_iter_double(iter));
			break;
		case BSON_TYPE_UTF8:
			count++;
			j_sql_bind_text(prepared->stmt, index, bson_iter_utf8(iter, NULL), -1);
			break;
		case BSON_TYPE_INT32:
			count++;
			j_sql_bind_int(prepared->stmt, index, bson_iter_int32(iter));
			break;
		case BSON_TYPE_INT64:
			count++;
			j_sql_bind_int64(prepared->stmt, index, bson_iter_int64(iter));
			break;
		case BSON_TYPE_BINARY:
			count++;
			bson_iter_binary(iter, NULL, &binary_len, &binary);
			j_sql_bind_blob(prepared->stmt, index, binary, binary_len);
			break;
		case BSON_TYPE_NULL:
			j_sql_bind_null(prepared->stmt, index);
			break;
		case BSON_TYPE_EOD:
		case BSON_TYPE_DOCUMENT:
		case BSON_TYPE_ARRAY:
		case BSON_TYPE_UNDEFINED:
		case BSON_TYPE_OID:
		case BSON_TYPE_BOOL:
		case BSON_TYPE_DATE_TIME:
		case BSON_TYPE_REGEX:
		case BSON_TYPE_DBPOINTER:
		case BSON_TYPE_CODE:
		case BSON_TYPE_SYMBOL:
		case BSON_TYPE_CODEWSCOPE:
		case BSON_TYPE_TIMESTAMP:
		case BSON_TYPE_DECIMAL128:
		case BSON_TYPE_MAXKEY:
		case BSON_TYPE_MINKEY:
		default:
			j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, type);
		}
	}
	j_goto_error_backend(!count, J_BACKEND_DB_ERROR_NO_VARIABLE_SET, "");
	j_sql_step_and_reset_check_done_constraint(prepared->stmt);
	return TRUE;
_error:
	return FALSE;
}
static gboolean
backend_insert(gpointer _batch, gchar const* name, bson_t const* metadata, GError** error)
{
	JSqlBatch* batch = _batch;
	guint i;
	guint ret;
	bson_iter_t iter;
	bson_t* schema = NULL;
	gboolean schema_initialized = FALSE;
	JSqlCacheSQLPrepared* prepared = NULL;
	j_sql_transaction_begin();
	j_goto_error_backend(!metadata, J_BACKEND_DB_ERROR_METADATA_NULL, "");
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	j_goto_error_backend(!bson_count_keys(metadata), J_BACKEND_DB_ERROR_METADATA_EMPTY, "");
	prepared = getCachePrepared(batch->namespace, name, "insert", error);
	j_goto_error_subcommand(!prepared);
	if (!prepared->initialized)
	{
		schema = g_new0(bson_t, 1);
		schema_initialized = backend_schema_get(batch, name, schema, error);
		j_goto_error_subcommand(!schema_initialized);
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 0;
		prepared->variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		g_string_append_printf(prepared->sql, "INSERT INTO %s_%s (", batch->namespace, name);
		ret = bson_iter_init(&iter, schema);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		while (bson_iter_next(&iter))
		{
			ret = BSON_ITER_HOLDS_INT32(&iter);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
			if (prepared->variables_count)
				g_string_append(prepared->sql, ", ");
			prepared->variables_count++;
			g_string_append_printf(prepared->sql, "%s", bson_iter_key(&iter));
			g_hash_table_insert(prepared->variables_index, g_strdup(bson_iter_key(&iter)), GINT_TO_POINTER(prepared->variables_count));
		}
		g_string_append(prepared->sql, ") VALUES ( ?1");
		for (i = 1; i < prepared->variables_count; i++)
			g_string_append_printf(prepared->sql, ", ?%d", i + 1);
		g_string_append(prepared->sql, " )");
		j_sql_prepare(prepared->sql->str, &prepared->stmt);
		prepared->initialized = TRUE;
	}
	ret = bson_iter_init(&iter, metadata);
	j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
	ret = insert_helper(prepared, &iter, error);
	j_goto_error_subcommand(!ret);
	if (schema)
	{
		if (schema_initialized)
			bson_destroy(schema);
		g_free(schema);
	}
	j_sql_transaction_commit();
	return TRUE;
_error:
	if (schema)
	{
		if (schema_initialized)
			bson_destroy(schema);
		g_free(schema);
	}
	j_sql_transaction_abort();
	return FALSE;
}
static gboolean
build_selector_query(bson_iter_t* iter, GString* sql, JDBSelectorMode mode, guint* variables_count, GError** error)
{
	JDBSelectorMode mode_child;
	gint ret;
	JDBSelectorOperator op;
	gboolean first = TRUE;
	bson_iter_t iterchild;
	g_string_append(sql, "( ");
	while (bson_iter_next(iter))
	{
		if (!g_strcmp0(bson_iter_key(iter), "_mode"))
			continue;
		ret = BSON_ITER_HOLDS_DOCUMENT(iter);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(iter));
		ret = bson_iter_recurse(iter, &iterchild);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
		if (!first)
		{
			switch (mode)
			{
			case J_DB_SELECTOR_MODE_AND:
				g_string_append(sql, " AND ");
				break;
			case J_DB_SELECTOR_MODE_OR:
				g_string_append(sql, " OR ");
				break;
			case _J_DB_SELECTOR_MODE_COUNT:
			default:
				j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "");
			}
		}
		first = FALSE;
		if (bson_iter_find(&iterchild, "_mode"))
		{
			ret = BSON_ITER_HOLDS_INT32(&iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iterchild));
			mode_child = bson_iter_int32(&iterchild);
			ret = bson_iter_recurse(iter, &iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
			ret = build_selector_query(&iterchild, sql, mode_child, variables_count, error);
			j_goto_error_subcommand(!ret);
		}
		else
		{
			(*variables_count)++;
			ret = bson_iter_recurse(iter, &iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
			ret = bson_iter_find(&iterchild, "_name");
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_KEY_NOT_FOUND, "_name");
			ret = BSON_ITER_HOLDS_UTF8(&iterchild);
			g_string_append_printf(sql, "%s ", bson_iter_utf8(&iterchild, NULL));
			ret = bson_iter_recurse(iter, &iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
			ret = bson_iter_find(&iterchild, "_operator");
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_KEY_NOT_FOUND, "_operator");
			ret = BSON_ITER_HOLDS_INT32(&iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iterchild));
			op = bson_iter_int32(&iterchild);
			switch (op)
			{
			case J_DB_SELECTOR_OPERATOR_LT:
				g_string_append(sql, "<");
				break;
			case J_DB_SELECTOR_OPERATOR_LE:
				g_string_append(sql, "<=");
				break;
			case J_DB_SELECTOR_OPERATOR_GT:
				g_string_append(sql, ">");
				break;
			case J_DB_SELECTOR_OPERATOR_GE:
				g_string_append(sql, ">=");
				break;
			case J_DB_SELECTOR_OPERATOR_EQ:
				g_string_append(sql, "=");
				break;
			case J_DB_SELECTOR_OPERATOR_NE:
				g_string_append(sql, "!=");
				break;
			case _J_DB_SELECTOR_OPERATOR_COUNT:
			default:
				j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_COMPARATOR_INVALID, op);
			}
			g_string_append_printf(sql, " ?%d", *variables_count);
		}
	}
	g_string_append(sql, " )");
	j_goto_error_backend(first, J_BACKEND_DB_ERROR_SELECTOR_EMPTY, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
bind_selector_query(bson_iter_t* iter, JSqlCacheSQLPrepared* prepared, guint* variables_count, GError** error)
{
	uint32_t binary_len;
	const uint8_t* binary;
	bson_iter_t iterchild;
	gint ret;
	bson_type_t type;
	while (bson_iter_next(iter))
	{
		if (!g_strcmp0(bson_iter_key(iter), "_mode"))
			continue;
		ret = BSON_ITER_HOLDS_DOCUMENT(iter);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(iter));
		ret = bson_iter_recurse(iter, &iterchild);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
		if (bson_iter_find(&iterchild, "_mode"))
		{
			ret = bson_iter_recurse(iter, &iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
			ret = bind_selector_query(&iterchild, prepared, variables_count, error);
			j_goto_error_subcommand(!ret);
		}
		else
		{
			(*variables_count)++;
			ret = bson_iter_recurse(iter, &iterchild);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_RECOURSE, "");
			ret = bson_iter_find(&iterchild, "_value");
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_KEY_NOT_FOUND, "_value");
			type = bson_iter_type(&iterchild);
			switch (type)
			{
			case BSON_TYPE_DOUBLE:
				j_sql_bind_double(prepared->stmt, *variables_count, bson_iter_double(&iterchild));
				break;
			case BSON_TYPE_UTF8:
				j_sql_bind_text(prepared->stmt, *variables_count, bson_iter_utf8(&iterchild, NULL), -1);
				break;
			case BSON_TYPE_INT32:
				j_sql_bind_int(prepared->stmt, *variables_count, bson_iter_int32(&iterchild));
				break;
			case BSON_TYPE_INT64:
				j_sql_bind_int64(prepared->stmt, *variables_count, bson_iter_int64(&iterchild));
				break;
			case BSON_TYPE_NULL:
				j_sql_bind_null(prepared->stmt, *variables_count);
				break;
			case BSON_TYPE_BINARY:
				bson_iter_binary(&iterchild, NULL, &binary_len, &binary);
				j_sql_bind_blob(prepared->stmt, *variables_count, binary, binary_len);
				break;
			case BSON_TYPE_EOD:
			case BSON_TYPE_DOCUMENT:
			case BSON_TYPE_ARRAY:
			case BSON_TYPE_UNDEFINED:
			case BSON_TYPE_OID:
			case BSON_TYPE_BOOL:
			case BSON_TYPE_DATE_TIME:
			case BSON_TYPE_REGEX:
			case BSON_TYPE_DBPOINTER:
			case BSON_TYPE_CODE:
			case BSON_TYPE_SYMBOL:
			case BSON_TYPE_CODEWSCOPE:
			case BSON_TYPE_TIMESTAMP:
			case BSON_TYPE_DECIMAL128:
			case BSON_TYPE_MAXKEY:
			case BSON_TYPE_MINKEY:
			default:
				j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, type);
			}
		}
	}
	return TRUE;
_error:
	return FALSE;
}
static gboolean
_backend_query(gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	JDBSelectorMode mode_child;
	JSqlBatch* batch = _batch;
	guint64 tmp;
	gint ret;
	guint count = 0;
	bson_iter_t iter;
	guint variables_count;
	JSqlCacheSQLPrepared* prepared = NULL;
	GString* sql = g_string_new(NULL);
	JSqlIterator* iteratorOut;
	*iterator = NULL;
	iteratorOut = g_new(JSqlIterator, 1);
	iteratorOut->namespace = g_strdup(batch->namespace);
	iteratorOut->name = g_strdup(name);
	iteratorOut->index = 0;
	iteratorOut->arr = g_array_new(FALSE, FALSE, sizeof(guint64));
	g_string_append_printf(sql, "SELECT DISTINCT _id FROM %s_%s", batch->namespace, name);
	if (selector && (1 < bson_count_keys(selector)))
	{
		g_string_append(sql, " WHERE ");
		ret = bson_iter_init(&iter, selector);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		ret = bson_iter_find(&iter, "_mode");
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_KEY_NOT_FOUND, "_mode");
		ret = BSON_ITER_HOLDS_INT32(&iter);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		mode_child = bson_iter_int32(&iter);
		ret = bson_iter_init(&iter, selector);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		variables_count = 0;
		ret = build_selector_query(&iter, sql, mode_child, &variables_count, error);
		j_goto_error_subcommand(!ret);
	}
	prepared = getCachePrepared(batch->namespace, name, sql->str, error);
	j_goto_error_subcommand(!prepared);
	if (!prepared->initialized)
	{
		ret = backend_schema_get(batch, name, NULL, error);
		j_goto_error_subcommand(!ret);
		prepared->sql = g_string_new(sql->str);
		prepared->variables_count = variables_count;
		j_sql_prepare(prepared->sql->str, &prepared->stmt);
		prepared->initialized = TRUE;
	}
	if (selector && (1 < bson_count_keys(selector)))
	{
		ret = bson_iter_init(&iter, selector);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		variables_count = 0;
		ret = bind_selector_query(&iter, prepared, &variables_count, error);
		j_goto_error_subcommand(!ret);
	}
	j_sql_loop(prepared->stmt, ret)
	{
		count++;
		tmp = j_sql_column_uint64(prepared->stmt, 0);
		g_array_append_val(iteratorOut->arr, tmp);
	}
	j_sql_reset(prepared->stmt);
	j_goto_error_backend(!count, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "");
	g_string_free(sql, TRUE);
	*iterator = iteratorOut;
	return TRUE;
_error:
	g_string_free(sql, TRUE);
	freeJSqlIterator(iteratorOut);
	return FALSE;
}
static gboolean
backend_update(gpointer _batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error)
{
	uint32_t binary_len;
	const uint8_t* binary;
	JSqlBatch* batch = _batch;
	guint count;
	bson_type_t type;
	JSqlIterator* iterator = NULL;
	bson_iter_t iter;
	guint index;
	gint ret;
	guint i, j;
	bson_t* schema = NULL;
	gboolean schema_initialized = FALSE;
	JSqlCacheSQLPrepared* prepared = NULL;
	j_sql_transaction_begin();
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	j_goto_error_backend(!selector, J_BACKEND_DB_ERROR_SELECTOR_NULL, "");
	j_goto_error_backend(bson_count_keys(selector) < 2, J_BACKEND_DB_ERROR_SELECTOR_EMPTY, "");
	j_goto_error_backend(!metadata, J_BACKEND_DB_ERROR_METADATA_NULL, "");
	prepared = getCachePrepared(batch->namespace, name, "update", error);
	j_goto_error_subcommand(!prepared);
	if (!prepared->initialized)
	{
		schema = g_new0(bson_t, 1);
		schema_initialized = backend_schema_get(batch, name, schema, error);
		j_goto_error_subcommand(!schema_initialized);
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 0;
		prepared->variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		g_string_append_printf(prepared->sql, "UPDATE %s_%s SET ", batch->namespace, name);
		ret = bson_iter_init(&iter, schema);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		while (bson_iter_next(&iter))
		{
			ret = BSON_ITER_HOLDS_INT32(&iter);
			j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
			if (prepared->variables_count)
				g_string_append(prepared->sql, ", ");
			prepared->variables_count++;
			g_string_append_printf(prepared->sql, "%s = ?%d", bson_iter_key(&iter), prepared->variables_count);
			g_hash_table_insert(prepared->variables_index, g_strdup(bson_iter_key(&iter)), GINT_TO_POINTER(prepared->variables_count));
		}
		prepared->variables_count++;
		g_string_append_printf(prepared->sql, " WHERE _id = ?%d", prepared->variables_count);
		g_hash_table_insert(prepared->variables_index, g_strdup("_id"), GINT_TO_POINTER(prepared->variables_count));
		j_sql_prepare(prepared->sql->str, &prepared->stmt);
		prepared->initialized = TRUE;
	}
	ret = _backend_query(batch, name, selector, (gpointer*)&iterator, error);
	j_goto_error_subcommand(!ret);
	for (j = 0; j < iterator->arr->len; j++)
	{
		count = 0;
		for (i = 0; i < prepared->variables_count; i++)
			j_sql_bind_null(prepared->stmt, i + 1);
		index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, "_id"));
		j_goto_error_backend(!index, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "_id");
		j_sql_bind_int64(prepared->stmt, index, g_array_index(iterator->arr, guint64, j));
		ret = bson_iter_init(&iter, metadata);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		while (bson_iter_next(&iter))
		{
			type = bson_iter_type(&iter);
			index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, bson_iter_key(&iter)));
			j_goto_error_backend(!index, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, bson_iter_key(&iter));
			switch (type)
			{
			case BSON_TYPE_DOUBLE:
				count++;
				j_sql_bind_double(prepared->stmt, index, bson_iter_double(&iter));
				break;
			case BSON_TYPE_UTF8:
				count++;
				j_sql_bind_text(prepared->stmt, index, bson_iter_utf8(&iter, NULL), -1);
				break;
			case BSON_TYPE_INT32:
				count++;
				j_sql_bind_int(prepared->stmt, index, bson_iter_int32(&iter));
				break;
			case BSON_TYPE_INT64:
				count++;
				j_sql_bind_int64(prepared->stmt, index, bson_iter_int64(&iter));
				break;
			case BSON_TYPE_NULL:
				j_sql_bind_null(prepared->stmt, index);
				break;
			case BSON_TYPE_BINARY:
				count++;
				bson_iter_binary(&iter, NULL, &binary_len, &binary);
				j_sql_bind_blob(prepared->stmt, index, binary, binary_len);
				break;
			case BSON_TYPE_EOD:
			case BSON_TYPE_DOCUMENT:
			case BSON_TYPE_ARRAY:
			case BSON_TYPE_UNDEFINED:
			case BSON_TYPE_OID:
			case BSON_TYPE_BOOL:
			case BSON_TYPE_DATE_TIME:
			case BSON_TYPE_REGEX:
			case BSON_TYPE_DBPOINTER:
			case BSON_TYPE_CODE:
			case BSON_TYPE_SYMBOL:
			case BSON_TYPE_CODEWSCOPE:
			case BSON_TYPE_TIMESTAMP:
			case BSON_TYPE_DECIMAL128:
			case BSON_TYPE_MAXKEY:
			case BSON_TYPE_MINKEY:
			default:
				j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, type);
			}
		}
		j_sql_step_and_reset_check_done_constraint(prepared->stmt);
		j_goto_error_backend(!count, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "");
	}
	if (schema)
	{
		if (schema_initialized)
			bson_destroy(schema);
		g_free(schema);
	}
	freeJSqlIterator(iterator);
	j_sql_transaction_commit();
	return TRUE;
_error:
	if (schema)
	{
		if (schema_initialized)
			bson_destroy(schema);
		g_free(schema);
	}
	freeJSqlIterator(iterator);
	j_sql_transaction_abort();
	return FALSE;
}
static gboolean
backend_delete(gpointer _batch, gchar const* name, bson_t const* selector, GError** error)
{
	JSqlBatch* batch = _batch;
	JSqlIterator* iterator = NULL;
	guint j;
	gint ret;
	JSqlCacheSQLPrepared* prepared = NULL;
	j_sql_transaction_begin();
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	ret = _backend_query(batch, name, selector, (gpointer*)&iterator, error);
	j_goto_error_subcommand(!ret);
	prepared = getCachePrepared(batch->namespace, name, "delete", error);
	j_goto_error_subcommand(!prepared);
	if (!prepared->initialized)
	{
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 1;
		g_string_append_printf(prepared->sql, "DELETE FROM %s_%s WHERE _id = ?1", batch->namespace, name);
		j_sql_prepare(prepared->sql->str, &prepared->stmt);
		prepared->initialized = TRUE;
	}
	for (j = 0; j < iterator->arr->len; j++)
	{
		j_sql_bind_int64(prepared->stmt, 1, g_array_index(iterator->arr, guint64, j));
		j_sql_step_and_reset_check_done_constraint(prepared->stmt);
	}
	freeJSqlIterator(iterator);
	j_sql_transaction_commit();
	return TRUE;
_error:
	freeJSqlIterator(iterator);
	j_sql_transaction_abort();
	return FALSE;
}
static gboolean
backend_query(gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	JDBSelectorMode mode_child;
	bson_t* schema = NULL;
	gboolean schema_initialized = FALSE;
	JSqlBatch* batch = _batch;
	gboolean ret;
	bson_iter_t iter;
	guint variables_count;
	guint variables_count2;
	JSqlCacheSQLPrepared* prepared = NULL;
	GHashTable* variables_index = NULL;
	GHashTable* variables_type = NULL;
	GString* sql = g_string_new(NULL);
	j_goto_error_backend(!name, J_BACKEND_DB_ERROR_NAME_NULL, "");
	j_goto_error_backend(!batch, J_BACKEND_DB_ERROR_BATCH_NULL, "");
	variables_index = g_hash_table_new_full(g_direct_hash, NULL, NULL, g_free);
	variables_type = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	g_string_append(sql, "SELECT ");
	variables_count = 0;
	schema = g_new0(bson_t, 1);
	schema_initialized = backend_schema_get(batch, name, schema, error);
	j_goto_error_subcommand(!schema_initialized);
	ret = bson_iter_init(&iter, schema);
	j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
	g_string_append(sql, "_id");
	g_hash_table_insert(variables_index, GINT_TO_POINTER(variables_count), g_strdup("_id"));
	g_hash_table_insert(variables_type, g_strdup("_id"), GINT_TO_POINTER(J_DB_TYPE_UINT32));
	variables_count++;
	while (bson_iter_next(&iter))
	{
		ret = BSON_ITER_HOLDS_INT32(&iter);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		g_string_append_printf(sql, ", %s", bson_iter_key(&iter));
		g_hash_table_insert(variables_index, GINT_TO_POINTER(variables_count), g_strdup(bson_iter_key(&iter)));
		g_hash_table_insert(variables_type, g_strdup(bson_iter_key(&iter)), GINT_TO_POINTER(bson_iter_int32(&iter)));
		variables_count++;
	}
	g_string_append_printf(sql, " FROM %s_%s", batch->namespace, name);
	if (selector && (1 < bson_count_keys(selector)))
	{
		g_string_append(sql, " WHERE ");
		ret = bson_iter_init(&iter, selector);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		ret = bson_iter_find(&iter, "_mode");
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_KEY_NOT_FOUND, "_mode");
		ret = BSON_ITER_HOLDS_INT32(&iter);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		mode_child = bson_iter_int32(&iter);
		ret = bson_iter_init(&iter, selector);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		variables_count2 = 0;
		ret = build_selector_query(&iter, sql, mode_child, &variables_count2, error);
		j_goto_error_subcommand(!ret);
	}
	prepared = getCachePrepared(batch->namespace, name, sql->str, error);
	j_goto_error_subcommand(!prepared);
	if (!prepared->initialized)
	{
		ret = backend_schema_get(batch, name, NULL, error);
		j_goto_error_subcommand(!ret);
		prepared->sql = g_string_new(sql->str);
		prepared->variables_index = variables_index;
		prepared->variables_type = variables_type;
		prepared->variables_count = variables_count;
		j_sql_prepare(prepared->sql->str, &prepared->stmt);
		prepared->initialized = TRUE;
	}
	else
	{
		g_hash_table_destroy(variables_index);
		variables_index = NULL;
		g_hash_table_destroy(variables_type);
		variables_type = NULL;
	}
	if (selector && (1 < bson_count_keys(selector)))
	{
		ret = bson_iter_init(&iter, selector);
		j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_ITER_INIT, "");
		variables_count2 = 0;
		ret = bind_selector_query(&iter, prepared, &variables_count2, error);
		j_goto_error_subcommand(!ret);
	}
	*iterator = prepared;
	g_string_free(sql, TRUE);
	if (schema)
	{
		if (schema_initialized)
			bson_destroy(schema);
		g_free(schema);
	}
	return TRUE;
_error:
	g_string_free(sql, TRUE);
	if (schema)
	{
		if (schema_initialized)
			bson_destroy(schema);
		g_free(schema);
	}
	if (variables_index)
		g_hash_table_destroy(variables_index);
	if (variables_type)
		g_hash_table_destroy(variables_type);
	return FALSE;
}
static gboolean
backend_iterate(gpointer _iterator, bson_t* metadata, GError** error)
{
	const char* name;
	guint i;
	JDBType type;
	gint ret;
	JSqlCacheSQLPrepared* prepared = _iterator;
	gboolean found = FALSE;
	j_sql_step(prepared->stmt, ret)
	{
		found = TRUE;
		for (i = 0; i < prepared->variables_count; i++)
		{
			name = g_hash_table_lookup(prepared->variables_index, GINT_TO_POINTER(i));
			type = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_type, name));
			switch (type)
			{
			case J_DB_TYPE_SINT32:
				ret = bson_append_int32(metadata, name, -1, j_sql_column_sint32(prepared->stmt, i));
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "SINT32");
				break;
			case J_DB_TYPE_UINT32:
				ret = bson_append_int32(metadata, name, -1, j_sql_column_uint32(prepared->stmt, i));
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "UINT32");
				break;
			case J_DB_TYPE_FLOAT32:
				ret = bson_append_double(metadata, name, -1, j_sql_column_float32(prepared->stmt, i));
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "FLOAT32");
				break;
			case J_DB_TYPE_SINT64:
				ret = bson_append_int64(metadata, name, -1, j_sql_column_sint64(prepared->stmt, i));
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "SINT64");
				break;
			case J_DB_TYPE_UINT64:
				ret = bson_append_int64(metadata, name, -1, j_sql_column_uint64(prepared->stmt, i));
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "UINT64");
				break;
			case J_DB_TYPE_FLOAT64:
				ret = bson_append_double(metadata, name, -1, j_sql_column_float64(prepared->stmt, i));
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "FLOAT64");
				break;
			case J_DB_TYPE_STRING:
				ret = bson_append_utf8(metadata, name, -1, j_sql_column_text(prepared->stmt, i), -1);
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "STRING");
				break;
			case J_DB_TYPE_BLOB:
				if (j_sql_column_blob(prepared->stmt, i) != NULL)
					ret = bson_append_binary(metadata, name, -1, BSON_SUBTYPE_BINARY, (const uint8_t*)j_sql_column_blob(prepared->stmt, i), j_sql_column_blob_len(prepared->stmt, i));
				else
					ret = bson_append_null(metadata, name, -1);
				j_goto_error_backend(!ret, J_BACKEND_DB_ERROR_BSON_APPEND_FAILED, "BLOB");
				break;
			case _J_DB_TYPE_COUNT:
			default:
				j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_DB_TYPE_INVALID, "");
			}
		}
	}
	j_goto_error_backend(!found, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "");
	return TRUE;
_error:
	j_sql_reset(prepared->stmt);
	return FALSE;
}
#endif
