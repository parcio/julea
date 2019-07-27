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

#ifdef JULEA_DEBUG
#define j_goto(label)                                                \
	do                                                           \
	{                                                            \
		g_debug("%s:%s: goto " #label, G_STRLOC, G_STRFUNC); \
		goto label;                                          \
	} while (0)
#else
#define j_goto(label) goto label;
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
	void* stmt;
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
static void* stmt_schema_structure_create = NULL;
static void* stmt_schema_structure_get = NULL;
static void* stmt_schema_structure_delete = NULL;
static void* stmt_transaction_abort = NULL;
static void* stmt_transaction_begin = NULL;
static void* stmt_transaction_commit = NULL;
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
		{
			j_sql_finalize(p->stmt, NULL);
		}
		g_free(p);
	}
}
static JSqlCacheSQLPrepared*
getCachePrepared(gchar const* namespace, gchar const* name, gchar const* query, GError** error)
{
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
		if (!g_hash_table_insert(cacheNamespaces->namespaces, g_strdup(namespace), cacheNames))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_THREADING_ERROR, "some other thread modified critical variables without lock");
			j_goto(_error);
		}
	}
	cacheQueries = g_hash_table_lookup(cacheNames->names, name);
	if (!cacheQueries)
	{
		cacheQueries = g_new0(JSqlCacheSQLQueries, 1);
		cacheQueries->queries = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheSQLPrepared);
		if (!g_hash_table_insert(cacheNames->names, g_strdup(name), cacheQueries))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_THREADING_ERROR, "some other thread modified critical variables without lock");
			j_goto(_error);
		}
	}
	cachePrepared = g_hash_table_lookup(cacheQueries->queries, query);
	if (!cachePrepared)
	{
		cachePrepared = g_new0(JSqlCacheSQLPrepared, 1);
		if (!g_hash_table_insert(cacheQueries->queries, g_strdup(query), cachePrepared))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_THREADING_ERROR, "some other thread modified critical variables without lock");
			j_goto(_error);
		}
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
	GError* error = NULL;
	if (!j_sql_exec(
		"CREATE TABLE IF NOT EXISTS schema_structure ("
		"namespace TEXT,"
		"name TEXT,"
		"value TEXT,"
		"PRIMARY KEY (namespace, name)"
		")",
		    &error))
		j_goto(_error);
	if (!j_sql_prepare("INSERT INTO schema_structure(namespace, name, value) VALUES (?1, ?2, ?3)", &stmt_schema_structure_create, &error))
		j_goto(_error);
	if (!j_sql_prepare("SELECT value FROM schema_structure WHERE namespace=?1 AND name=?2", &stmt_schema_structure_get, &error))
		j_goto(_error);
	if (!j_sql_prepare("DELETE FROM schema_structure WHERE namespace=?1 AND name=?2", &stmt_schema_structure_delete, &error))
		j_goto(_error);
	if (!j_sql_prepare("BEGIN TRANSACTION", &stmt_transaction_begin, &error))
		j_goto(_error);
	if (!j_sql_prepare("COMMIT", &stmt_transaction_commit, &error))
		j_goto(_error);
	if (!j_sql_prepare("ROLLBACK", &stmt_transaction_abort, &error))
		j_goto(_error);
	return TRUE;
_error:
	g_error_free(error);
	return FALSE;
}
static void
fini_sql(void)
{
	freeJSqlCacheNamespaces(cacheNamespaces);
	j_sql_finalize(stmt_schema_structure_create, NULL);
	j_sql_finalize(stmt_schema_structure_get, NULL);
	j_sql_finalize(stmt_schema_structure_delete, NULL);
	j_sql_finalize(stmt_transaction_abort, NULL);
	j_sql_finalize(stmt_transaction_begin, NULL);
	j_sql_finalize(stmt_transaction_commit, NULL);
}
static gboolean
backend_batch_start(gchar const* namespace, JSemanticsSafety safety, gpointer* _batch, GError** error)
{
	JSqlBatch* batch = *_batch = g_slice_new(JSqlBatch);
	if (!namespace)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAMESPACE_NULL, "namespace not set");
		j_goto(_error);
	}
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
	gboolean has_next;
	gboolean equals;
	guint counter = 0;
	gboolean found_index = FALSE;
	JDBType_value value;
	char* json = NULL;
	const char* tmp_string;
	GString* sql = g_string_new(NULL);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_begin, error))
		j_goto(_error);
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	if (!schema)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_NULL, "schema not set");
		j_goto(_error);
	}
	g_string_append_printf(sql, "CREATE TABLE %s_%s ( _id INTEGER PRIMARY KEY", batch->namespace, name);
	if (!j_bson_iter_init(&iter, schema, error))
		j_goto(_error);
	while (TRUE)
	{
		if (!j_bson_iter_next(&iter, &has_next, error))
			j_goto(_error);
		if (!has_next)
			break;
		if (!j_bson_iter_key_equals(&iter, "_index", &equals, error))
			j_goto(_error);
		if (equals)
		{
			found_index = TRUE;
		}
		else
		{
			counter++;
			g_string_append_printf(sql, ", %s", j_bson_iter_key(&iter, error));
			if (!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error))
				j_goto(_error);
			type = value.val_uint32;
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
				g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_DB_TYPE_INVALID, "db type invalid");
				j_goto(_error);
			}
		}
	}
	g_string_append(sql, " )");
	if (!counter)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_EMPTY, "schema empty");
		j_goto(_error);
	}
	json = j_bson_as_json(schema, error);
	if (!json)
		j_goto(_error);
	value.val_string = batch->namespace;
	if (!j_sql_bind_value(stmt_schema_structure_create, 1, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	value.val_string = name;
	if (!j_sql_bind_value(stmt_schema_structure_create, 2, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	value.val_string = json;
	if (!j_sql_bind_value(stmt_schema_structure_create, 3, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	if (!j_sql_step_and_reset_check_done(stmt_schema_structure_create, error))
		j_goto(_error);
	if (!j_sql_exec(sql->str, error))
		j_goto(_error);
	j_bson_free_json(json);
	g_string_free(sql, TRUE);
	if (found_index)
	{
		i = 0;
		if (!j_bson_iter_init(&iter, schema, error))
			j_goto(_error);
		if (!j_bson_iter_find(&iter, "_index", error))
			j_goto(_error);
		if (!j_bson_iter_recurse_array(&iter, &iter_child, error))
			j_goto(_error);
		while (TRUE)
		{
			if (!j_bson_iter_next(&iter_child, &has_next, error))
				j_goto(_error);
			if (!has_next)
				break;
			sql = g_string_new(NULL);
			first = TRUE;
			g_string_append_printf(sql, "CREATE INDEX %s_%s_%d ON %s_%s ( ", batch->namespace, name, i, batch->namespace, name);
			if (!j_bson_iter_recurse_array(&iter_child, &iter_child2, error))
				j_goto(_error);
			while (TRUE)
			{
				if (!j_bson_iter_next(&iter_child2, &has_next, error))
					j_goto(_error);
				if (!has_next)
					break;
				if (first)
					first = FALSE;
				else
					g_string_append(sql, ", ");
				if (!j_bson_iter_value(&iter_child2, J_DB_TYPE_STRING, &value, error))
					j_goto(_error);
				tmp_string = value.val_string;
				g_string_append_printf(sql, "%s", tmp_string);
			}
			g_string_append(sql, " )");
			if (!j_sql_exec(sql->str, error))
				j_goto(_error);
			g_string_free(sql, TRUE);
			i++;
		}
	}
	if (!j_sql_step_and_reset_check_done(stmt_transaction_commit, error))
		j_goto(_error);
	return TRUE;
_error:
	j_bson_free_json(json);
	g_string_free(sql, TRUE);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_abort, error))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
backend_schema_get(gpointer _batch, gchar const* name, bson_t* schema, GError** error)
{
	JDBType_value value;
	JSqlBatch* batch = _batch;
	guint found = FALSE;
	gboolean sql_found;
	const char* json = NULL;
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	value.val_string = batch->namespace;
	if (!j_sql_bind_value(stmt_schema_structure_get, 1, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	value.val_string = name;
	if (!j_sql_bind_value(stmt_schema_structure_get, 2, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	if (!j_sql_step(stmt_schema_structure_get, &sql_found, error))
		j_goto(_error);
	if (sql_found)
	{
		if (schema)
		{
			if (!j_sql_column(stmt_schema_structure_get, 0, J_DB_TYPE_STRING, &value, error))
				j_goto(_error);
			json = value.val_string;
			if (json == NULL || !strlen(json))
			{
				g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND, "schema not found");
				j_goto(_error);
			}
			if (!j_bson_init_from_json(schema, json, error))
				j_goto(_error);
		}
		found = TRUE;
		}
	if (!found)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND, "schema not found");
		j_goto(_error);
	}
	if (!j_sql_reset(stmt_schema_structure_get, error))
		j_goto(_error);
	return TRUE;
_error:
	if (!j_sql_reset(stmt_schema_structure_get, NULL))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
backend_schema_delete(gpointer _batch, gchar const* name, GError** error)
{
	JDBType_value value;
	JSqlBatch* batch = _batch;
	GString* sql = g_string_new(NULL);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_begin, error))
		j_goto(_error);
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	deleteCachePrepared(batch->namespace, name);
	if (!backend_schema_get(batch, name, NULL, error))
		j_goto(_error);
	g_string_append_printf(sql, "DROP TABLE %s_%s", batch->namespace, name);
	value.val_string = batch->namespace;
	if (!j_sql_bind_value(stmt_schema_structure_delete, 1, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	value.val_string = name;
	if (!j_sql_bind_value(stmt_schema_structure_delete, 2, J_DB_TYPE_STRING, &value, error))
		j_goto(_error);
	if (!j_sql_step_and_reset_check_done(stmt_schema_structure_delete, error))
		j_goto(_error);
	if (!j_sql_exec(sql->str, error))
		j_goto(_error);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_commit, error))
		j_goto(_error);
	g_string_free(sql, TRUE);
	return TRUE;
_error:
	g_string_free(sql, TRUE);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_abort, error))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
insert_helper(JSqlCacheSQLPrepared* prepared, bson_iter_t* iter, GError** error)
{
	const char* tmp_string;
	JDBType type;
	guint i;
	gboolean has_next;
	guint index;
	guint count = 0;
	JDBType_value value;
	for (i = 0; i < prepared->variables_count; i++)
	{
		if (!j_sql_bind_null(prepared->stmt, i + 1, error))
			j_goto(_error);
	}
	while (TRUE)
		{
		if (!j_bson_iter_next(iter, &has_next, error))
			j_goto(_error);
		if (!has_next)
			break;
		if (!j_bson_iter_type_db(iter, &type, error))
			j_goto(_error);
		tmp_string = j_bson_iter_key(iter, error);
		if (!tmp_string)
			j_goto(_error);
		index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, tmp_string));
		if (!index)
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
			j_goto(_error);
		}
			count++;
		if (!j_bson_iter_value(iter, type, &value, error))
			j_goto(_error);
		if (!j_sql_bind_value(prepared->stmt, index, type, &value, error))
			j_goto(_error);
		}
	if (!count)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NO_VARIABLE_SET, "no variable set");
		j_goto(_error);
	}
	if (!j_sql_step_and_reset_check_done(prepared->stmt, error))
		j_goto(_error);
	return TRUE;
_error:
	return FALSE;
}
static gboolean
backend_insert(gpointer _batch, gchar const* name, bson_t const* metadata, GError** error)
{
	JSqlBatch* batch = _batch;
	gboolean has_next;
	guint i;
	bson_iter_t iter;
	bson_t schema;
	const char* tmp_string;
	gboolean schema_initialized = FALSE;
	JSqlCacheSQLPrepared* prepared = NULL;
	if (!j_sql_step_and_reset_check_done(stmt_transaction_begin, error))
		j_goto(_error);
	if (!metadata)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_METADATA_NULL, "metadata not set");
		j_goto(_error);
	}
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	if (!j_bson_has_enough_keys(metadata, 1, error))
		j_goto(_error);
	prepared = getCachePrepared(batch->namespace, name, "insert", error);
	if (!prepared)
		j_goto(_error);
	if (!prepared->initialized)
	{
		schema_initialized = backend_schema_get(batch, name, &schema, error);
		if (!schema_initialized)
			j_goto(_error);
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 0;
		prepared->variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		g_string_append_printf(prepared->sql, "INSERT INTO %s_%s (", batch->namespace, name);
		if (!j_bson_iter_init(&iter, &schema, error))
			j_goto(_error);
		while (TRUE)
		{
			if (!j_bson_iter_next(&iter, &has_next, error))
				j_goto(_error);
			if (!has_next)
				break;
			if (!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, NULL, error))
				j_goto(_error);
			if (prepared->variables_count)
				g_string_append(prepared->sql, ", ");
			prepared->variables_count++;
			tmp_string = j_bson_iter_key(&iter, error);
			if (!tmp_string)
				j_goto(_error);
			g_string_append_printf(prepared->sql, "%s", tmp_string);
			g_hash_table_insert(prepared->variables_index, g_strdup(tmp_string), GINT_TO_POINTER(prepared->variables_count));
		}
		g_string_append(prepared->sql, ") VALUES ( ?1");
		for (i = 1; i < prepared->variables_count; i++)
			g_string_append_printf(prepared->sql, ", ?%d", i + 1);
		g_string_append(prepared->sql, " )");
		if (!j_sql_prepare(prepared->sql->str, &prepared->stmt, error))
			j_goto(_error);
		prepared->initialized = TRUE;
	}
	if (!j_bson_iter_init(&iter, metadata, error))
		j_goto(_error);
	if (!insert_helper(prepared, &iter, error))
		j_goto(_error);
		if (schema_initialized)
		j_bson_destroy(&schema);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_commit, error))
		j_goto(_error);
	return TRUE;
_error:
		if (schema_initialized)
		j_bson_destroy(&schema);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_abort, error))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
build_selector_query(bson_iter_t* iter, GString* sql, JDBSelectorMode mode, guint* variables_count, GError** error)
{
	JDBSelectorMode mode_child;
	gboolean equals;
	gboolean has_next;
	JDBSelectorOperator op;
	gboolean first = TRUE;
	const char* tmp_string;
	JDBType_value value;
	bson_iter_t iterchild;
	g_string_append(sql, "( ");
	while (TRUE)
	{
		if (!j_bson_iter_next(iter, &has_next, error))
			j_goto(_error);
		if (!has_next)
			break;
		if (!j_bson_iter_key_equals(iter, "_mode", &equals, error))
			j_goto(_error);
		if (equals)
			continue;
		if (!j_bson_iter_recurse_document(iter, &iterchild, error))
			j_goto(_error);
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
				g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "operator invalid");
				j_goto(_error);
			}
		}
		first = FALSE;
		if (j_bson_iter_find(&iterchild, "_mode", NULL))
		{
			if (!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error))
				j_goto(_error);
			mode_child = value.val_uint32;
			if (!j_bson_iter_recurse_document(iter, &iterchild, error))
				j_goto(_error);
			if (!build_selector_query(&iterchild, sql, mode_child, variables_count, error))
				j_goto(_error);
		}
		else
		{
			(*variables_count)++;
			if (!j_bson_iter_recurse_document(iter, &iterchild, error))
				j_goto(_error);
			if (!j_bson_iter_find(&iterchild, "_name", error))
				j_goto(_error);
			if (!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error))
				j_goto(_error);
			tmp_string = value.val_string;
			g_string_append_printf(sql, "%s ", tmp_string);
			if (!j_bson_iter_recurse_document(iter, &iterchild, error))
				j_goto(_error);
			if (!j_bson_iter_find(&iterchild, "_operator", error))
				j_goto(_error);
			if (!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error))
				j_goto(_error);
			op = value.val_uint32;
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
				g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_COMPARATOR_INVALID, "comparator invalid");
				j_goto(_error);
			}
			g_string_append_printf(sql, " ?%d", *variables_count);
		}
	}
	g_string_append(sql, " )");
	if (first)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SELECTOR_EMPTY, "selector empty");
		j_goto(_error);
	}
	return TRUE;
_error:
	return FALSE;
}
static gboolean
bind_selector_query(bson_iter_t* iter, JSqlCacheSQLPrepared* prepared, guint* variables_count, GError** error)
{
	bson_iter_t iterchild;
	JDBType_value value;
	JDBType type;
	gboolean has_next;
	gboolean equals;
	while (TRUE)
	{
		if (!j_bson_iter_next(iter, &has_next, error))
			j_goto(_error);
		if (!has_next)
			break;
		if (!j_bson_iter_key_equals(iter, "_mode", &equals, error))
			j_goto(_error);
		if (equals)
			continue;
		if (!j_bson_iter_recurse_document(iter, &iterchild, error))
			j_goto(_error);
		if (j_bson_iter_find(&iterchild, "_mode", NULL))
		{
			if (!j_bson_iter_recurse_document(iter, &iterchild, error))
				j_goto(_error);
			if (!bind_selector_query(&iterchild, prepared, variables_count, error))
				j_goto(_error);
		}
		else
		{
			(*variables_count)++;
			if (!j_bson_iter_recurse_document(iter, &iterchild, error))
				j_goto(_error);
			if (!j_bson_iter_find(&iterchild, "_value", error))
				j_goto(_error);
			if (!j_bson_iter_type_db(&iterchild, &type, error))
				j_goto(_error);
			if (!j_bson_iter_value(&iterchild, type, &value, error))
				j_goto(_error);
			if (!j_sql_bind_value(prepared->stmt, *variables_count, type, &value, error))
				j_goto(_error);
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
	gboolean sql_found;
	JDBType_value value;
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
	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		g_string_append(sql, " WHERE ");
		if (!j_bson_iter_init(&iter, selector, error))
			j_goto(_error);
		if (!j_bson_iter_find(&iter, "_mode", error))
			j_goto(_error);
		if (!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error))
			j_goto(_error);
		mode_child = value.val_uint32;
		if (!j_bson_iter_init(&iter, selector, error))
			j_goto(_error);
		variables_count = 0;
		if (!build_selector_query(&iter, sql, mode_child, &variables_count, error))
			j_goto(_error);
	}
	prepared = getCachePrepared(batch->namespace, name, sql->str, error);
	if (!prepared)
		j_goto(_error);
	if (!prepared->initialized)
	{
		if (!backend_schema_get(batch, name, NULL, error))
			j_goto(_error);
		prepared->sql = g_string_new(sql->str);
		prepared->variables_count = variables_count;
		if (!j_sql_prepare(prepared->sql->str, &prepared->stmt, error))
			j_goto(_error);
		prepared->initialized = TRUE;
	}
	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (!j_bson_iter_init(&iter, selector, error))
			j_goto(_error);
		variables_count = 0;
		if (!bind_selector_query(&iter, prepared, &variables_count, error))
			j_goto(_error);
	}
	while (TRUE)
	{
		if (!j_sql_step(prepared->stmt, &sql_found, error))
			j_goto(_error);
		if (!sql_found)
			break;
		count++;
		if (!j_sql_column(prepared->stmt, 0, J_DB_TYPE_UINT32, &value, error))
			j_goto(_error);
		g_array_append_val(iteratorOut->arr, value.val_uint32);
	}
	if (!j_sql_reset(prepared->stmt, error))
		j_goto(_error);
	if (!count)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		j_goto(_error);
	}
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
	JSqlBatch* batch = _batch;
	guint count;
	JDBType type;
	JDBType_value value;
	JSqlIterator* iterator = NULL;
	bson_iter_t iter;
	guint index;
	guint i, j;
	const char* tmp_string;
	gboolean has_next;
	bson_t schema;
	gboolean schema_initialized = FALSE;
	JSqlCacheSQLPrepared* prepared = NULL;
	if (!j_sql_step_and_reset_check_done(stmt_transaction_begin, error))
		j_goto(_error);
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	if (!selector)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SELECTOR_NULL, "selector not set");
		j_goto(_error);
	}
	if (!j_bson_has_enough_keys(selector, 2, error))
		j_goto(_error);
	if (!metadata)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_METADATA_NULL, "metadata not set");
		j_goto(_error);
	}
	prepared = getCachePrepared(batch->namespace, name, "update", error);
	if (!prepared)
		j_goto(_error);
	if (!prepared->initialized)
	{
		schema_initialized = backend_schema_get(batch, name, &schema, error);
		if (!schema_initialized)
			j_goto(_error);
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 0;
		prepared->variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		g_string_append_printf(prepared->sql, "UPDATE %s_%s SET ", batch->namespace, name);
		if (!j_bson_iter_init(&iter, &schema, error))
			j_goto(_error);
		while (TRUE)
		{
			if (!j_bson_iter_next(&iter, &has_next, error))
				j_goto(_error);
			if (!has_next)
				break;
			if (!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, NULL, error))
				j_goto(_error);
			if (prepared->variables_count)
				g_string_append(prepared->sql, ", ");
			prepared->variables_count++;
			tmp_string = j_bson_iter_key(&iter, error);
			if (!tmp_string)
				j_goto(_error);
			g_string_append_printf(prepared->sql, "%s = ?%d", tmp_string, prepared->variables_count);
			g_hash_table_insert(prepared->variables_index, g_strdup(tmp_string), GINT_TO_POINTER(prepared->variables_count));
		}
		prepared->variables_count++;
		g_string_append_printf(prepared->sql, " WHERE _id = ?%d", prepared->variables_count);
		g_hash_table_insert(prepared->variables_index, g_strdup("_id"), GINT_TO_POINTER(prepared->variables_count));
		if (!j_sql_prepare(prepared->sql->str, &prepared->stmt, error))
			j_goto(_error);
		prepared->initialized = TRUE;
	}
	if (!_backend_query(batch, name, selector, (gpointer*)&iterator, error))
		j_goto(_error);
	for (j = 0; j < iterator->arr->len; j++)
	{
		count = 0;
		for (i = 0; i < prepared->variables_count; i++)
		{
			if (!j_sql_bind_null(prepared->stmt, i + 1, error))
				j_goto(_error);
		}
		index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, "_id"));
		if (!index)
			{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
			j_goto(_error);
		}
		value.val_uint32 = g_array_index(iterator->arr, guint64, j);
		if (!j_sql_bind_value(prepared->stmt, index, J_DB_TYPE_UINT32, &value, error))
			j_goto(_error);
		if (!j_bson_iter_init(&iter, metadata, error))
			j_goto(_error);
		while (TRUE)
		{
			if (!j_bson_iter_next(&iter, &has_next, error))
				j_goto(_error);
			if (!has_next)
				break;
			if (!j_bson_iter_type_db(&iter, &type, error))
				j_goto(_error);
			tmp_string = j_bson_iter_key(&iter, error);
			if (!tmp_string)
				j_goto(_error);
			index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, tmp_string));
			if (!index)
			{
				g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
				j_goto(_error);
			}
				count++;
			if (!j_bson_iter_value(&iter, type, &value, error))
				j_goto(_error);
			if (!j_sql_bind_value(prepared->stmt, index, type, &value, error))
				j_goto(_error);
			}
		if (!j_sql_step_and_reset_check_done(prepared->stmt, error))
			j_goto(_error);
		if (!count)
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
			j_goto(_error);
		}
	}
		if (schema_initialized)
		j_bson_destroy(&schema);
	freeJSqlIterator(iterator);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_commit, error))
		j_goto(_error);
	return TRUE;
_error:
		if (schema_initialized)
		j_bson_destroy(&schema);
	freeJSqlIterator(iterator);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_abort, error))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
backend_delete(gpointer _batch, gchar const* name, bson_t const* selector, GError** error)
{
	JSqlBatch* batch = _batch;
	JSqlIterator* iterator = NULL;
	guint j;
	JDBType_value value;
	JSqlCacheSQLPrepared* prepared = NULL;
	if (!j_sql_step_and_reset_check_done(stmt_transaction_begin, error))
		j_goto(_error);
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	if (!_backend_query(batch, name, selector, (gpointer*)&iterator, error))
		j_goto(_error);
	prepared = getCachePrepared(batch->namespace, name, "delete", error);
	if (!prepared)
		j_goto(_error);
	if (!prepared->initialized)
	{
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 1;
		g_string_append_printf(prepared->sql, "DELETE FROM %s_%s WHERE _id = ?1", batch->namespace, name);
		if (!j_sql_prepare(prepared->sql->str, &prepared->stmt, error))
			j_goto(_error);
		prepared->initialized = TRUE;
	}
	for (j = 0; j < iterator->arr->len; j++)
	{
		value.val_uint32 = g_array_index(iterator->arr, guint64, j);
		if (!j_sql_bind_value(prepared->stmt, 1, J_DB_TYPE_UINT32, &value, error))
			j_goto(_error);
		if (!j_sql_step_and_reset_check_done(prepared->stmt, error))
			j_goto(_error);
	}
	freeJSqlIterator(iterator);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_commit, error))
		j_goto(_error);
	return TRUE;
_error:
	freeJSqlIterator(iterator);
	if (!j_sql_step_and_reset_check_done(stmt_transaction_abort, error))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
backend_query(gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	JDBSelectorMode mode_child;
	bson_t schema;
	gboolean schema_initialized = FALSE;
	JSqlBatch* batch = _batch;
	bson_iter_t iter;
	guint variables_count;
	guint variables_count2;
	JDBType_value value;
	const char* tmp_string;
	JSqlCacheSQLPrepared* prepared = NULL;
	GHashTable* variables_index = NULL;
	gboolean has_next;
	GHashTable* variables_type = NULL;
	GString* sql = g_string_new(NULL);
	if (!name)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NAME_NULL, "name not set");
		j_goto(_error);
	}
	if (!batch)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_BATCH_NULL, "batch not set");
		j_goto(_error);
	}
	variables_index = g_hash_table_new_full(g_direct_hash, NULL, NULL, g_free);
	variables_type = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	g_string_append(sql, "SELECT ");
	variables_count = 0;
	schema_initialized = backend_schema_get(batch, name, &schema, error);
	if (!schema_initialized)
		j_goto(_error);
	if (!j_bson_iter_init(&iter, &schema, error))
		j_goto(_error);
	g_string_append(sql, "_id");
	g_hash_table_insert(variables_index, GINT_TO_POINTER(variables_count), g_strdup("_id"));
	g_hash_table_insert(variables_type, g_strdup("_id"), GINT_TO_POINTER(J_DB_TYPE_UINT32));
	variables_count++;
	while (TRUE)
	{
		if (!j_bson_iter_next(&iter, &has_next, error))
			j_goto(_error);
		if (!has_next)
			break;
		if (!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error))
			j_goto(_error);
		tmp_string = j_bson_iter_key(&iter, error);
		if (!tmp_string)
			j_goto(_error);
		g_string_append_printf(sql, ", %s", tmp_string);
		g_hash_table_insert(variables_index, GINT_TO_POINTER(variables_count), g_strdup(tmp_string));
		g_hash_table_insert(variables_type, g_strdup(tmp_string), GINT_TO_POINTER(value.val_uint32));
		variables_count++;
	}
	g_string_append_printf(sql, " FROM %s_%s", batch->namespace, name);
	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		g_string_append(sql, " WHERE ");
		if (!j_bson_iter_init(&iter, selector, error))
			j_goto(_error);
		if (!j_bson_iter_find(&iter, "_mode", error))
			j_goto(_error);
		if (!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error))
			j_goto(_error);
		mode_child = value.val_uint32;
		if (!j_bson_iter_init(&iter, selector, error))
			j_goto(_error);
		variables_count2 = 0;
		if (!build_selector_query(&iter, sql, mode_child, &variables_count2, error))
			j_goto(_error);
	}
	prepared = getCachePrepared(batch->namespace, name, sql->str, error);
	if (!prepared)
		j_goto(_error);
	if (!prepared->initialized)
	{
		if (!backend_schema_get(batch, name, NULL, error))
			j_goto(_error);
		prepared->sql = g_string_new(sql->str);
		prepared->variables_index = variables_index;
		prepared->variables_type = variables_type;
		prepared->variables_count = variables_count;
		if (!j_sql_prepare(prepared->sql->str, &prepared->stmt, error))
			j_goto(_error);
		prepared->initialized = TRUE;
	}
	else
	{
		g_hash_table_destroy(variables_index);
		variables_index = NULL;
		g_hash_table_destroy(variables_type);
		variables_type = NULL;
	}
	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (!j_bson_iter_init(&iter, selector, error))
			j_goto(_error);
		variables_count2 = 0;
		if (!bind_selector_query(&iter, prepared, &variables_count2, error))
			j_goto(_error);
	}
	*iterator = prepared;
	g_string_free(sql, TRUE);
		if (schema_initialized)
		j_bson_destroy(&schema);
	return TRUE;
_error:
	g_string_free(sql, TRUE);
		if (schema_initialized)
		j_bson_destroy(&schema);
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
	JDBType_value value;
	JDBType type;
	gboolean sql_found;
	JSqlCacheSQLPrepared* prepared = _iterator;
	gboolean found = FALSE;
	if (!j_sql_step(prepared->stmt, &sql_found, error))
		j_goto(_error);
	if (sql_found)
	{
		found = TRUE;
		for (i = 0; i < prepared->variables_count; i++)
		{
			name = g_hash_table_lookup(prepared->variables_index, GINT_TO_POINTER(i));
			type = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_type, name));
			if (!j_sql_column(prepared->stmt, i, type, &value, error))
				j_goto(_error);
			if (!j_bson_append_value(metadata, name, type, &value, error))
				j_goto(_error);
			}
		}
	if (!found)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		j_goto(_error);
	}
	return TRUE;
_error:
	if (!j_sql_reset(prepared->stmt, NULL))
		j_goto(_error2);
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
#endif
