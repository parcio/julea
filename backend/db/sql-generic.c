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

/*
 * this file does not care which sql-database is actually in use, and uses only defines sql-syntax to allow fast and easy implementations for any new sql-database backend
*/

struct JThreadVariables
{
	gboolean initialized;
	void* sql_backend;
	GHashTable* namespaces;
};

typedef struct JThreadVariables JThreadVariables;

struct JSqlCacheNames
{
	GHashTable* names;
};

typedef struct JSqlCacheNames JSqlCacheNames;

struct JSqlCacheSQLQueries
{
	GHashTable* types; // variablename(char*) -> variabletype(JDBType)
	GHashTable* queries; //sql(char*) -> (JSqlCacheSQLPrepared*)
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
	gchar* namespace;
	gchar* name;
	gpointer backend_data;
};

typedef struct JSqlCacheSQLPrepared JSqlCacheSQLPrepared;

struct JSqlBatch
{
	const gchar* namespace;
	JSemantics* semantics;
	gboolean open;
	gboolean aborted;
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

static void thread_variables_fini(void* ptr);
static GPrivate thread_variables_global = G_PRIVATE_INIT(thread_variables_fini);

static void
sql_generic_init(void)
{
	J_TRACE_FUNCTION(NULL);
}

static void
sql_generic_fini(void)
{
	J_TRACE_FUNCTION(NULL);
}

static void
thread_variables_fini(void* ptr)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = ptr;

	if (thread_variables)
	{
		if (thread_variables->namespaces)
		{
			g_hash_table_destroy(thread_variables->namespaces);
		}

		j_sql_close(thread_variables->sql_backend);
		g_free(thread_variables);
	}
}

static void freeJSqlCacheNames(void* ptr);

static JThreadVariables*
thread_variables_get(gpointer backend_data, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	thread_variables = g_private_get(&thread_variables_global);

	if (!thread_variables)
	{
		thread_variables = g_new0(JThreadVariables, 1);
		thread_variables->initialized = FALSE;
	}

	if (!thread_variables->initialized)
	{
		thread_variables->sql_backend = j_sql_open(backend_data);

		if (G_UNLIKELY(!j_sql_exec(thread_variables->sql_backend,
					   "CREATE TABLE IF NOT EXISTS schema_structure ("
					   "namespace VARCHAR(255),"
					   "name VARCHAR(255),"
					   "varname VARCHAR(255),"
					   "vartype INTEGER"
					   // FIXME figure out whether we should add an index instead
					   //"PRIMARY KEY (namespace, name, varname)"
					   ")",
					   error)))
		{
			goto _error;
		}

		thread_variables->namespaces = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheNames);
		thread_variables->initialized = TRUE;
		g_private_replace(&thread_variables_global, thread_variables);
	}

	return thread_variables;

_error:
	return NULL;
}

static void
freeJSqlIterator(gpointer ptr)
{
	J_TRACE_FUNCTION(NULL);

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
freeJSqlCacheNames(void* ptr)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheNames* p = ptr;

	if (ptr)
	{
		if (p->names)
		{
			g_hash_table_destroy(p->names);
		}

		g_free(p);
	}
}

static void
freeJSqlCacheSQLQueries(void* ptr)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheSQLQueries* p = ptr;

	if (ptr)
	{
		if (p->queries)
		{
			g_hash_table_destroy(p->queries);
		}

		if (p->types)
		{
			g_hash_table_destroy(p->types);
		}

		g_free(p);
	}
}

static void
freeJSqlCacheSQLPrepared(void* ptr)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheSQLPrepared* p = ptr;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(p->backend_data, NULL))))
		g_assert_not_reached();

	if (ptr)
	{
		if (p->initialized)
		{
			if (p->variables_index)
			{
				g_hash_table_destroy(p->variables_index);
			}

			if (p->sql)
			{
				g_string_free(p->sql, TRUE);
			}

			if (p->stmt)
			{
				j_sql_finalize(thread_variables->sql_backend, p->stmt, NULL);
			}
		}

		g_free(p->namespace);
		g_free(p->name);
		g_free(p);
	}
}

static JSqlCacheSQLQueries*
_getCachePrepared(gpointer backend_data, gchar const* namespace, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheNames* cacheNames = NULL;
	JSqlCacheSQLQueries* cacheQueries = NULL;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	g_return_val_if_fail(thread_variables->namespaces != NULL, NULL);

	if (!(cacheNames = g_hash_table_lookup(thread_variables->namespaces, namespace)))
	{
		cacheNames = g_new0(JSqlCacheNames, 1);
		cacheNames->names = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheSQLQueries);

		if (G_UNLIKELY(!g_hash_table_insert(thread_variables->namespaces, g_strdup(namespace), cacheNames)))
		{
			g_assert_not_reached();
		}
	}

	if (!(cacheQueries = g_hash_table_lookup(cacheNames->names, name)))
	{
		cacheQueries = g_new0(JSqlCacheSQLQueries, 1);
		cacheQueries->queries = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, freeJSqlCacheSQLPrepared);
		cacheQueries->types = NULL;

		if (G_UNLIKELY(!g_hash_table_insert(cacheNames->names, g_strdup(name), cacheQueries)))
		{
			g_assert_not_reached();
		}
	}

	return cacheQueries;

_error:
	return NULL;
}

static gboolean _backend_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error);

static GHashTable*
getCacheSchema(gpointer backend_data, gpointer _batch, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;
	gboolean schema_initialized = FALSE;
	gboolean has_next;
	gboolean equals;
	bson_t schema;
	JSqlCacheSQLQueries* cacheQueries = NULL;
	bson_iter_t iter;
	char const* string_tmp;
	JDBTypeValue value;

	if (!(cacheQueries = _getCachePrepared(backend_data, batch->namespace, name, error)))
	{
		goto _error;
	}

	if (cacheQueries->types == NULL)
	{
		cacheQueries->types = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

		if (!(schema_initialized = _backend_schema_get(backend_data, batch, name, &schema, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_init(&iter, &schema, error)))
		{
			goto _error;
		}

		while (TRUE)
		{
			if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
			{
				goto _error;
			}

			if (!has_next)
			{
				break;
			}

			if (G_UNLIKELY(!j_bson_iter_key_equals(&iter, "_index", &equals, error)))
			{
				goto _error;
			}

			if (equals)
			{
				continue;
			}

			string_tmp = j_bson_iter_key(&iter, error);

			if (G_UNLIKELY(!string_tmp))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			g_hash_table_insert(cacheQueries->types, g_strdup(string_tmp), GINT_TO_POINTER(value.val_uint32));
		}
	}

	if (schema_initialized)
	{
		j_bson_destroy(&schema);
	}

	return cacheQueries->types;

_error:
	if (schema_initialized)
	{
		j_bson_destroy(&schema);
	}

	if (cacheQueries->types)
	{
		g_hash_table_unref(cacheQueries->types);
		cacheQueries->types = NULL;
	}

	return NULL;
}

static JSqlCacheSQLPrepared*
getCachePrepared(gpointer backend_data, gchar const* namespace, gchar const* name, gchar const* query, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheSQLQueries* cacheQueries = NULL;
	JSqlCacheSQLPrepared* cachePrepared = NULL;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!(cacheQueries = _getCachePrepared(backend_data, namespace, name, error)))
	{
		goto _error;
	}

	if (!(cachePrepared = g_hash_table_lookup(cacheQueries->queries, query)))
	{
		cachePrepared = g_new0(JSqlCacheSQLPrepared, 1);
		cachePrepared->namespace = g_strdup(namespace);
		cachePrepared->name = g_strdup(name);
		cachePrepared->backend_data = backend_data;

		if (G_UNLIKELY(!g_hash_table_insert(cacheQueries->queries, g_strdup(query), cachePrepared)))
		{
			g_assert_not_reached();
		}
	}

	return cachePrepared;

_error:
	return NULL;
}

static void
deleteCachePrepared(gpointer backend_data, gchar const* namespace, gchar const* name)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheNames* cacheNames = NULL;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, NULL))))
	{
		goto _error;
	}

	g_return_if_fail(thread_variables->namespaces != NULL);

	if (G_UNLIKELY(!(cacheNames = g_hash_table_lookup(thread_variables->namespaces, namespace))))
	{
		goto _error;
	}

	g_hash_table_remove(cacheNames->names, name);

	return;

_error:
	return;
}

static gboolean
_backend_batch_start(gpointer backend_data, JSqlBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	g_return_val_if_fail(!batch->open, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!j_sql_start_transaction(thread_variables->sql_backend, error))
	{
		goto _error;
	}

	batch->open = TRUE;
	batch->aborted = FALSE;

	return TRUE;

_error:
	return FALSE;
}

static gboolean
_backend_batch_execute(gpointer backend_data, JSqlBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	g_return_val_if_fail(batch->open || (!batch->open && batch->aborted), FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!j_sql_commit_transaction(thread_variables->sql_backend, error))
	{
		goto _error;
	}

	batch->open = FALSE;

	return TRUE;

_error:
	return FALSE;
}

static gboolean
_backend_batch_abort(gpointer backend_data, JSqlBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	g_return_val_if_fail(batch->open, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!j_sql_abort_transaction(thread_variables->sql_backend, error))
	{
		goto _error;
	}

	batch->open = FALSE;
	batch->aborted = TRUE;

	return TRUE;

_error:
	return FALSE;
}

G_LOCK_DEFINE_STATIC(sql_backend_lock);

static gboolean
backend_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* _batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);
	g_return_val_if_fail(_batch != NULL, FALSE);

	if (SQL_MODE == SQL_MODE_SINGLE_THREAD)
		G_LOCK(sql_backend_lock);

	batch = *_batch = g_new(JSqlBatch, 1);
	batch->namespace = namespace;
	batch->semantics = j_semantics_ref(semantics);
	batch->open = FALSE;

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	j_semantics_unref(batch->semantics);
	g_free(batch);

	if (SQL_MODE == SQL_MODE_SINGLE_THREAD)
		G_UNLOCK(sql_backend_lock);

	return FALSE;
}

static gboolean
backend_batch_execute(gpointer backend_data, gpointer _batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;

	g_return_val_if_fail(batch != NULL, FALSE);

	if (G_UNLIKELY(!_backend_batch_execute(backend_data, batch, error)))
	{
		goto _error;
	}

	j_semantics_unref(batch->semantics);
	g_free(batch);

	if (SQL_MODE == SQL_MODE_SINGLE_THREAD)
		G_UNLOCK(sql_backend_lock);

	return TRUE;

_error:
	j_semantics_unref(batch->semantics);
	g_free(batch);

	if (SQL_MODE == SQL_MODE_SINGLE_THREAD)
		G_UNLOCK(sql_backend_lock);

	return FALSE;
}

static gboolean
backend_schema_create(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlCacheSQLPrepared* prepared = NULL;
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
	JDBTypeValue value;
	const char* string_tmp;
	GString* sql = g_string_new(NULL);
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(schema != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	prepared = getCachePrepared(backend_data, batch->namespace, name, "_schema_create", error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		type = J_DB_TYPE_STRING;
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);
		type = J_DB_TYPE_UINT32;
		g_array_append_val(arr_types_in, type);

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, "INSERT INTO schema_structure(namespace, name, varname, vartype) VALUES (?, ?, ?, ?)", &prepared->stmt, arr_types_in, NULL, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	if (G_UNLIKELY(!_backend_batch_execute(backend_data, batch, error)))
	{
		//no ddl in transaction - most databases wont support that - continue without any open transaction
		goto _error;
	}

	g_string_append_printf(sql, "CREATE TABLE " SQL_QUOTE "%s_%s" SQL_QUOTE " ( _id INTEGER " SQL_AUTOINCREMENT_STRING " PRIMARY KEY", batch->namespace, name);

	if (G_UNLIKELY(!j_bson_iter_init(&iter, schema, error)))
	{
		goto _error;
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(&iter, "_index", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			found_index = TRUE;
		}
		else
		{
			counter++;
			g_string_append_printf(sql, ", " SQL_QUOTE "%s" SQL_QUOTE, j_bson_iter_key(&iter, error));

			if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			type = value.val_uint32;

			switch (type)
			{
				case J_DB_TYPE_SINT32:
					g_string_append(sql, " INTEGER");
					break;
				case J_DB_TYPE_ID:
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
					g_string_append(sql, SQL_UINT64_TYPE);
					break;
				case J_DB_TYPE_FLOAT64:
					g_string_append(sql, " REAL");
					break;
				case J_DB_TYPE_STRING:
					g_string_append(sql, " VARCHAR(255)");
					break;
				case J_DB_TYPE_BLOB:
					g_string_append(sql, " BLOB");
					break;
				default:
					g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_DB_TYPE_INVALID, "db type invalid");
					goto _error;
			}
		}
	}

	g_string_append(sql, " )");

	if (G_UNLIKELY(!counter))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_EMPTY, "schema empty");
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_exec(thread_variables->sql_backend, sql->str, error)))
	{
		goto _error;
	}

	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	if (found_index)
	{
		i = 0;

		if (G_UNLIKELY(!j_bson_iter_init(&iter, schema, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_find(&iter, "_index", error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_array(&iter, &iter_child, error)))
		{
			goto _error;
		}

		while (TRUE)
		{
			if (G_UNLIKELY(!j_bson_iter_next(&iter_child, &has_next, error)))
			{
				goto _error;
			}

			if (!has_next)
			{
				break;
			}

			sql = g_string_new(NULL);
			first = TRUE;

			g_string_append_printf(sql, "CREATE INDEX " SQL_QUOTE "%s_%s_%d" SQL_QUOTE " ON " SQL_QUOTE "%s_%s" SQL_QUOTE " ( ", batch->namespace, name, i, batch->namespace, name);

			if (G_UNLIKELY(!j_bson_iter_recurse_array(&iter_child, &iter_child2, error)))
			{
				goto _error;
			}

			while (TRUE)
			{
				if (G_UNLIKELY(!j_bson_iter_next(&iter_child2, &has_next, error)))
				{
					goto _error;
				}

				if (!has_next)
				{
					break;
				}

				if (first)
				{
					first = FALSE;
				}
				else
				{
					g_string_append(sql, ", ");
				}

				if (G_UNLIKELY(!j_bson_iter_value(&iter_child2, J_DB_TYPE_STRING, &value, error)))
				{
					goto _error;
				}

				string_tmp = value.val_string;
				g_string_append_printf(sql, SQL_QUOTE "%s" SQL_QUOTE, string_tmp);
			}

			g_string_append(sql, " )");

			if (G_UNLIKELY(!j_sql_exec(thread_variables->sql_backend, sql->str, error)))
			{
				goto _error;
			}

			if (sql)
			{
				g_string_free(sql, TRUE);
				sql = NULL;
			}

			i++;
		}
	}

	value.val_string = batch->namespace;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 1, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_string = name;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 2, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_string = "_id";

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 3, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_uint32 = J_DB_TYPE_UINT32;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 4, J_DB_TYPE_UINT32, &value, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_step_and_reset_check_done(thread_variables->sql_backend, prepared->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_iter_init(&iter, schema, error)))
	{
		goto _error;
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(&iter, "_index", &equals, error)))
		{
			goto _error;
		}

		if (!equals)
		{
			value.val_string = batch->namespace;

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 1, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			value.val_string = name;

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 2, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			value.val_string = j_bson_iter_key(&iter, error);

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 3, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			if (value.val_uint32 == J_DB_TYPE_ID)
			{
				value.val_uint32 = J_DB_TYPE_UINT32;
			}

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 4, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_sql_step_and_reset_check_done(thread_variables->sql_backend, prepared->stmt, error)))
			{
				goto _error;
			}
		}
	}

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, NULL)))
	{
		goto _error2;
	}

	if (sql)
	{
		g_string_free(sql, TRUE);
	}

	return FALSE;

_error2:
	return FALSE;
}

static gboolean
_backend_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue value1;
	JDBTypeValue value2;
	JSqlBatch* batch = _batch;
	guint found = FALSE;
	gboolean sql_found;
	gboolean bson_initialized = FALSE;
	JSqlCacheSQLPrepared* prepared = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) arr_types_out = NULL;
	JDBType type;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	prepared = getCachePrepared(backend_data, batch->namespace, name, "_schema_get", error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		type = J_DB_TYPE_STRING;
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_out, type);
		type = J_DB_TYPE_UINT32;
		g_array_append_val(arr_types_out, type);

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, "SELECT varname, vartype FROM schema_structure WHERE namespace=? AND name=?", &prepared->stmt, arr_types_in, arr_types_out, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	value1.val_string = batch->namespace;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 1, J_DB_TYPE_STRING, &value1, error)))
	{
		goto _error;
	}

	value1.val_string = name;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 2, J_DB_TYPE_STRING, &value1, error)))
	{
		goto _error;
	}

	if (schema)
	{
		if (!j_bson_init(schema, error))
		{
			goto _error;
		}
	}

	bson_initialized = TRUE;

	while (TRUE)
	{
		if (G_UNLIKELY(!j_sql_step(thread_variables->sql_backend, prepared->stmt, &sql_found, error)))
		{
			goto _error;
		}

		if (!sql_found)
		{
			break;
		}

		found = TRUE;

		if (!schema)
		{
			break;
		}

		if (G_UNLIKELY(!j_sql_column(thread_variables->sql_backend, prepared->stmt, 0, J_DB_TYPE_STRING, &value1, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_sql_column(thread_variables->sql_backend, prepared->stmt, 1, J_DB_TYPE_UINT32, &value2, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_append_value(schema, value1.val_string, J_DB_TYPE_UINT32, &value2, error)))
		{
			goto _error;
		}
	}

	if (G_UNLIKELY(!found))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND, "schema not found");
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_reset(thread_variables->sql_backend, prepared->stmt, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(!j_sql_reset(thread_variables->sql_backend, prepared->stmt, NULL)))
	{
		goto _error2;
	}

	if (bson_initialized)
	{
		j_bson_destroy(schema);
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}

static gboolean
backend_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;

	ret = _backend_schema_get(backend_data, _batch, name, schema, error);

	if (!ret)
	{
		_backend_batch_abort(backend_data, _batch, NULL);
	}

	return ret;
}
static gboolean
backend_schema_delete(gpointer backend_data, gpointer _batch, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBType type;
	JDBTypeValue value;
	JSqlBatch* batch = _batch;
	GString* sql = g_string_new(NULL);
	JSqlCacheSQLPrepared* prepared = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	prepared = getCachePrepared(backend_data, batch->namespace, name, "_schema_delete", error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		type = J_DB_TYPE_STRING;
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, "DELETE FROM schema_structure WHERE namespace=? AND name=?", &prepared->stmt, arr_types_in, NULL, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	if (G_UNLIKELY(!_backend_batch_execute(backend_data, batch, error)))
	{
		//no ddl in transaction - most databases wont support that - continue without any open transaction
		goto _error;
	}

	g_string_append_printf(sql, "DROP TABLE IF EXISTS " SQL_QUOTE "%s_%s" SQL_QUOTE, batch->namespace, name);
	value.val_string = batch->namespace;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 1, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_string = name;

	if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 2, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_step_and_reset_check_done(thread_variables->sql_backend, prepared->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_exec(thread_variables->sql_backend, sql->str, error)))
	{
		goto _error;
	}

	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, error)))
	{
		goto _error;
	}

	deleteCachePrepared(backend_data, batch->namespace, name);

	return TRUE;

_error:
	if (sql)
	{
		g_string_free(sql, TRUE);
	}

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, NULL)))
	{
		goto _error2;
	}

_error2:
	return FALSE;
}

static gboolean
backend_insert(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* metadata, bson_t* id, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;
	guint i;
	bson_iter_t iter;
	gpointer type_tmp;
	JDBType type;
	GHashTableIter schema_iter;
	GHashTable* schema_cache = NULL;
	const char* string_tmp;
	gboolean found;
	JSqlCacheSQLPrepared* prepared = NULL;
	JSqlCacheSQLPrepared* prepared_id = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) id_arr_types_out = NULL;
	gboolean has_next;
	guint index;
	guint count = 0;
	JDBTypeValue value;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(metadata != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	id_arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_has_enough_keys(metadata, 1, error)))
	{
		goto _error;
	}

	prepared_id = getCachePrepared(backend_data, batch->namespace, name, "_insert_id", error);

	if (G_UNLIKELY(!prepared_id))
	{
		goto _error;
	}

	if (!prepared_id->initialized)
	{
		type = J_DB_TYPE_UINT32;
		g_array_append_val(id_arr_types_out, type);

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, SQL_LAST_INSERT_ID_STRING, &prepared_id->stmt, NULL, id_arr_types_out, error)))
		{
			goto _error;
		}

		prepared_id->initialized = TRUE;
	}

	prepared = getCachePrepared(backend_data, batch->namespace, name, "_insert", error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!(schema_cache = getCacheSchema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		gchar* key;

		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 0;
		prepared->variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		g_string_append_printf(prepared->sql, "INSERT INTO " SQL_QUOTE "%s_%s" SQL_QUOTE " (", batch->namespace, name);
		g_hash_table_iter_init(&schema_iter, schema_cache);

		while (g_hash_table_iter_next(&schema_iter, (gpointer*)&key, &type_tmp))
		{
			type = GPOINTER_TO_INT(type_tmp);

			if (prepared->variables_count)
			{
				g_string_append(prepared->sql, ", ");
			}

			prepared->variables_count++;
			g_string_append_printf(prepared->sql, SQL_QUOTE "%s" SQL_QUOTE, key);
			g_array_append_val(arr_types_in, type);
			g_hash_table_insert(prepared->variables_index, g_strdup(key), GINT_TO_POINTER(prepared->variables_count));
		}

		g_string_append(prepared->sql, ") VALUES (");

		if (prepared->variables_count)
		{
			g_string_append_printf(prepared->sql, " ?");
		}

		for (i = 1; i < prepared->variables_count; i++)
		{
			g_string_append_printf(prepared->sql, ", ?");
		}

		g_string_append(prepared->sql, " )");

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, prepared->sql->str, &prepared->stmt, arr_types_in, NULL, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	if (G_UNLIKELY(!j_bson_iter_init(&iter, metadata, error)))
	{
		goto _error;
	}

	for (i = 0; i < prepared->variables_count; i++)
	{
		if (G_UNLIKELY(!j_sql_bind_null(thread_variables->sql_backend, prepared->stmt, i + 1, error)))
		{
			goto _error;
		}
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		string_tmp = j_bson_iter_key(&iter, error);

		if (G_UNLIKELY(!string_tmp))
		{
			goto _error;
		}

		type = GPOINTER_TO_INT(g_hash_table_lookup(schema_cache, string_tmp));
		index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, string_tmp));

		if (G_UNLIKELY(!index))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
			goto _error;
		}

		count++;

		if (G_UNLIKELY(!j_bson_iter_value(&iter, type, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, index, type, &value, error)))
		{
			goto _error;
		}
	}

	if (G_UNLIKELY(!count))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NO_VARIABLE_SET, "no variable set");
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_step_and_reset_check_done(thread_variables->sql_backend, prepared->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_step(thread_variables->sql_backend, prepared_id->stmt, &found, error)))
	{
		goto _error;
	}

	if (!found)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_column(thread_variables->sql_backend, prepared_id->stmt, 0, J_DB_TYPE_UINT32, &value, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_reset(thread_variables->sql_backend, prepared_id->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_append_value(id, "_value", J_DB_TYPE_UINT32, &value, error)))
	{
		goto _error;
	}

	value.val_uint32 = J_DB_TYPE_UINT32;

	if (G_UNLIKELY(!j_bson_append_value(id, "_value_type", J_DB_TYPE_UINT32, &value, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(!_backend_batch_abort(backend_data, batch, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}

static gboolean
build_selector_query(gpointer backend_data, bson_iter_t* iter, GString* sql, JDBSelectorMode mode, guint* variables_count, GArray* arr_types_in, GHashTable* schema_cache, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	gboolean equals;
	gboolean has_next;
	JDBSelectorOperator op;
	gboolean first = TRUE;
	const char* string_tmp;
	JDBTypeValue value;
	bson_iter_t iterchild;
	JThreadVariables* thread_variables = NULL;
	JDBType type;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	g_string_append(sql, "( ");

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "_mode", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			continue;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
		{
			goto _error;
		}

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
				default:
					g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "operator invalid");
					goto _error;
			}
		}

		first = FALSE;

		if (j_bson_iter_find(&iterchild, "_mode", NULL))
		{
			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			mode_child = value.val_uint32;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!build_selector_query(backend_data, &iterchild, sql, mode_child, variables_count, arr_types_in, schema_cache, error)))
			{
				goto _error;
			}
		}
		else
		{
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_name", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			string_tmp = value.val_string;
			g_string_append_printf(sql, SQL_QUOTE "%s" SQL_QUOTE " ", string_tmp);

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_value", error)))
			{
				goto _error;
			}

			type = GPOINTER_TO_INT(g_hash_table_lookup(schema_cache, string_tmp));
			g_array_append_val(arr_types_in, type);

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_operator", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

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
				default:
					g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_COMPARATOR_INVALID, "comparator invalid");
					goto _error;
			}

			(*variables_count)++;

			g_string_append_printf(sql, " ?");
		}
	}

	g_string_append(sql, " )");

	if (first)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SELECTOR_EMPTY, "selector empty");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
build_selector_query_ex(bson_iter_t* iter, GString* sql, JDBSelectorMode mode, guint* variables_count, GArray* arr_types_in, GHashTable* variables_type, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	gboolean equals;
	gboolean has_next;
	JDBSelectorOperator op;
	gboolean first = TRUE;
	const char* string_tmp;
	JDBTypeValue value;
	bson_iter_t iterchild;
	JDBType type;

	GString* sub_sql = g_string_new(NULL);

	//g_string_append(sql, "( ");

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		// Following code checks the value of the iterator and skips its processing if it contains tables' data.
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "tables", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			// Skip and continue with the next iterator.
			continue;
		}

		// Following code checks the value of the iterator and skips its processing if it contains join related data.
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "join", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			// Skip and continue with the next iterator.
			continue;
		}

		// Fetch mode (operator) that would be appended in between current and next (child) BSON element.
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "_mode", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			continue;
		}

		// As per the BSON formation, the above key "_mode" is followed by fields/columns that are appended as a BSON document.
		// Therefore the following code tries to fetch the first iterator of the next available document.
		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
		{
			// Terminate as code could not find BSON document.
			goto _error;
		}

		/*
		 * If the child iterator has key "_mode" it indicates commencement of a new BSON document (or selector)
		 */
		if (j_bson_iter_find(&iterchild, "_mode", NULL))
		{
			/*
			 * The value of "_mode" is used to join the fields/columns that are followed by it as BSON document items.
			 * It also would be used to join the selector (or BSON document) that is followed by it.
			 */
			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			mode_child = value.val_uint32;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			GString* child_sql = g_string_new(NULL);
			// Repeat the same routine for this child BSON document (or selector).
			if (G_UNLIKELY(!build_selector_query_ex(&iterchild, child_sql, mode_child, variables_count, arr_types_in, variables_type, error)))
			{
				goto _error;
			}


			if (child_sql->len > 0)
			{
				if (sub_sql->len > 0)
				{
					/* 
					 * The operator in between the selectors (or BSON documents) is the one that is defined for the first selector (first BSON document).
					 * If both the query strings, for current and child documents, are not empty then the operator defined for the first document
					 * would be appended as a connector.
					 * E.g. "<string for 1st selector/BSON doc>" <operator-linked-to-1st-BSON-doc> "<string for 2nd selector/BSON doc>" <operator-linked-to-2nd-BSON-doc> ...
					 */
					switch (mode)
					{
						case J_DB_SELECTOR_MODE_AND:
							g_string_append(sub_sql, " AND ");
							break;
						case J_DB_SELECTOR_MODE_OR:
							g_string_append(sub_sql, " OR ");
							break;
						default:
							g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "operator invalid");
							goto _error;
					}
				}
				
				g_string_append_printf(sub_sql, " ( %s )", child_sql->str);
			}
			
			g_string_free(child_sql, TRUE);
		}
		// The following code snippet iterates through the BSON document items, they are fields/columns and are appended to sql query string.
		else
		{
			if (sub_sql->len > 0)
			{
				// Append operator to the query string before appending details for the next BSON document element.
				switch (mode)
				{
					case J_DB_SELECTOR_MODE_AND:
						g_string_append(sub_sql, " AND ");
						break;
					case J_DB_SELECTOR_MODE_OR:
						g_string_append(sub_sql, " OR ");
						break;
					default:
						g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "operator invalid");
						goto _error;
				}
			}

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			JDBTypeValue table_name;
			
			// Fetch table name.
			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_table", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &table_name, error)))
			{
				goto _error;
			}
			
			// Fetch field or column name.
			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_name", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			string_tmp = value.val_string;
			
			// Append table name to field name. E.g. "<string for table name>.<string for field name>"
			g_string_append_printf(sub_sql, "%s." SQL_QUOTE "%s" SQL_QUOTE, table_name.val_string, string_tmp);

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_value", error)))
			{
				goto _error;
			}

			type = GPOINTER_TO_UINT(g_hash_table_lookup(variables_type, string_tmp));
			g_array_append_val(arr_types_in, type);

			// Fetch operator value that is used as a filter for the current field/column.
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_operator", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			op = value.val_uint32;

			switch (op)
			{
				case J_DB_SELECTOR_OPERATOR_LT:
					g_string_append(sub_sql, "<");
					break;
				case J_DB_SELECTOR_OPERATOR_LE:
					g_string_append(sub_sql, "<=");
					break;
				case J_DB_SELECTOR_OPERATOR_GT:
					g_string_append(sub_sql, ">");
					break;
				case J_DB_SELECTOR_OPERATOR_GE:
					g_string_append(sub_sql, ">=");
					break;
				case J_DB_SELECTOR_OPERATOR_EQ:
					g_string_append(sub_sql, "=");
					break;
				case J_DB_SELECTOR_OPERATOR_NE:
					g_string_append(sub_sql, "!=");
					break;
				default:
					g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_COMPARATOR_INVALID, "comparator invalid");
					goto _error;
			}

			(*variables_count)++;

			g_string_append_printf(sub_sql, " ?");
		}
	}

	//g_string_append(sql, " )");

	//if (first)
	//{
	//	g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SELECTOR_EMPTY, "selector empty");
	//	//goto _error;
	//}

	if (sub_sql->len > 0)
	{
		g_string_append_printf(sql, " ( %s )", sub_sql->str);
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
build_query_selection_part(bson_t const* selector, gpointer backend_data, GString* sql, JSqlBatch* batch, GHashTable* variables_index, GHashTable* variables_type, guint* variables_count, GArray* arr_types_out, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iter;
	bson_iter_t iterchild;

	gboolean has_next;
	JDBTypeValue value;

	GString* tables_sql = g_string_new(NULL);
	GString* table_name = NULL;
	GString* field_name = NULL;

	GHashTable* schema_cache = NULL;
	GHashTableIter schema_iter;
	JDBType type;
	char* string_tmp;
	gpointer type_tmp;

	// Initialize BSON iterator.
	if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
	{
		goto _error;
	}

	// Look for the key that is assigned to BSON array that contains tables' information.
	if (G_UNLIKELY(!j_bson_iter_find(&iter, "tables", error)))
	{
		goto _error;
	}
	
	// Try to intialize child element.
	if (G_UNLIKELY(!j_bson_iter_recurse_array(&iter, &iterchild, error)))
	{
		goto _error;
	}

	// Iterate through all the child elements.
	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(&iterchild, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			// Return as no more elements left.
			break;
		}
		
		// Extract namespace for the table from BSON element.
		if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
		{
			goto _error;
		}

		table_name = g_string_new(NULL);

		// Append namespace for the table.
		g_string_append(table_name, value.val_string);

		// The next element should contains the table name therefore the following snippet ensures that the next element should be present.
		if (G_UNLIKELY(!j_bson_iter_next(&iterchild, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		// Extract name for the table from BSON element.
		if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
		{
			goto _error;
		}
		
		// Append name for the table.
		g_string_append(table_name, "_");
		g_string_append(table_name, value.val_string);


		// Fetching table's schema to extract all the columns' names.
		if (!(schema_cache = getCacheSchema(backend_data, batch, value.val_string, error)))
		{
			goto _error;
		}

		// Initializing iterator.
		g_hash_table_iter_init(&schema_iter, schema_cache);

		if ( sql->len > 0)
		{
			g_string_append(sql, ", ");
		}			

		field_name = g_string_new(NULL);

		// Prepare field name.
		g_string_append_printf(field_name, "%s._id", table_name->str);  

		// Append field name to query string.		
		g_string_append_printf(sql, "%s._id", table_name->str);

		type = J_DB_TYPE_UINT32;
		g_hash_table_insert(variables_index, GINT_TO_POINTER(*variables_count), g_strdup(field_name->str));	// Maintains hash-table for column name and id (an auto-increment number).
		g_hash_table_insert(variables_type, g_strdup(field_name->str), GINT_TO_POINTER(type)); // Maintains hash-table for column name and their respective data types.
		g_array_append_val(arr_types_out, type);
		(*variables_count)++;

		// Iterate through all the columns the belong to current table and append their names to query string and other data structures.
		while (g_hash_table_iter_next(&schema_iter, (gpointer*)&string_tmp, &type_tmp))
		{
			type = GPOINTER_TO_INT(type_tmp);

			if (strcmp(string_tmp, "_id") == 0)
			{
				// Proceed with the nect column as it has already been added.
				continue;
			}
				
			g_string_free(field_name, TRUE);
			field_name = NULL;

			field_name = g_string_new(NULL);
			g_string_append_printf(field_name, "%s.%s", table_name->str, string_tmp);  

			g_string_append_printf(sql, ", %s." SQL_QUOTE "%s" SQL_QUOTE, table_name->str,string_tmp);
			g_hash_table_insert(variables_index, GINT_TO_POINTER(*variables_count), g_strdup(field_name->str));
			g_hash_table_insert(variables_type, g_strdup(field_name->str), GINT_TO_POINTER(type_tmp));
			g_array_append_val(arr_types_out, type);
			(*variables_count)++;
		}

		if ( tables_sql->len > 0)
		{
			g_string_append(tables_sql, ", ");
		}
			
		g_string_append_printf(tables_sql, SQL_QUOTE "%s" SQL_QUOTE, table_name->str);
		
		if (table_name)
		{
			g_string_free(table_name, TRUE);
			table_name = NULL;			
		}
	}

	g_string_append(sql, " FROM ");
	g_string_append(sql, tables_sql->str);

	if (tables_sql)
	{
		g_string_free(tables_sql, TRUE);
		tables_sql = NULL;
	}

	if (table_name)
	{
		g_string_free(table_name, TRUE);
		table_name = NULL;		
	}

	if (field_name)
	{
		g_string_free(field_name, TRUE);
		field_name = NULL;	
	}

	return TRUE;

_error:
	if (tables_sql)
	{
		g_string_free(tables_sql, TRUE);
		tables_sql = NULL;
	}

	if (table_name)
	{
		g_string_free(table_name, TRUE);
		table_name = NULL;	
	}

	if (field_name)
	{
		g_string_free(field_name, TRUE);
		field_name = NULL;	
	}

	return FALSE;
}

static gboolean
build_query_join_part(bson_t const* selector, gpointer backend_data, GString* sql, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iter;
	bson_iter_t iterchild;

	gboolean has_next;
	gboolean equals;
	JDBTypeValue value;

	GString* join_sql = NULL;

	// Initialize the iterator.
	if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
	{
		goto _error;
	}

	// The following code iterates through the BSON elements and processes that are linked to join operation.
	while (TRUE)
	{
	
		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		// Look for the key that is assigned to BSON document that contains join information.
		if (G_UNLIKELY(!j_bson_iter_key_equals(&iter, "join", &equals, error)))
		{
			goto _error;
		}

		if (!equals)
		{
			// Return as the reterived element has a different key.
			continue;
		}

		// Try to intialize child element.
		if (G_UNLIKELY(!j_bson_iter_recurse_document(&iter, &iterchild, error)))
		{
			goto _error;
		}

		join_sql = g_string_new(NULL);
		
		// Iterate through the elements and append values to the query string.
		while (TRUE)
		{
			if (G_UNLIKELY(!j_bson_iter_next(&iterchild, &has_next, error)))
			{
				goto _error;
			}

			if (!has_next)
			{
				break;
			}

			// Extract the value of the current BSON element.
			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			if ( join_sql->len > 0) 
			{
				g_string_append(join_sql, "=");
			}

			// Append the name of the field to the query string.
			g_string_append_printf(join_sql, "%s", value.val_string);
		}

		if ( sql->len > 0)
		{
			g_string_append(sql, " AND ");
		}			
		g_string_append_printf(sql, "%s", join_sql->str);
		
		if (join_sql)
		{
			g_string_free(join_sql, TRUE);
			join_sql = NULL;
		}
	}
	
	if (join_sql)
	{
		g_string_free(join_sql, TRUE);
		join_sql = NULL;
	}

	return TRUE;

_error:
	if (join_sql)
	{
		g_string_free(join_sql, TRUE);
		join_sql = NULL;
	}

	return FALSE;
}

static gboolean
bind_selector_query(gpointer backend_data, bson_iter_t* iter, JSqlCacheSQLPrepared* prepared, guint* variables_count, GHashTable* schema_cache, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterchild;
	JDBTypeValue value;
	JDBType type;
	gboolean has_next;
	gboolean equals;
	JThreadVariables* thread_variables = NULL;
	char const* string_tmp;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "_mode", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			continue;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
		{
			goto _error;
		}

		if (j_bson_iter_find(&iterchild, "_mode", NULL))
		{
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!bind_selector_query(backend_data, &iterchild, prepared, variables_count, schema_cache, error)))
			{
				goto _error;
			}
		}
		else
		{
			(*variables_count)++;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_name", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			string_tmp = value.val_string;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_value", error)))
			{
				goto _error;
			}

			type = GPOINTER_TO_INT(g_hash_table_lookup(schema_cache, string_tmp));

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, type, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, *variables_count, type, &value, error)))
			{
				goto _error;
			}
		}
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
bind_selector_queryex(gpointer backend_data, bson_iter_t* iter, JSqlCacheSQLPrepared* prepared, guint* variables_count, GHashTable* variables_index , GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterchild;
	JDBTypeValue value;
	JDBType type;
	gboolean has_next;
	gboolean equals;
	JThreadVariables* thread_variables = NULL;
	char const* string_tmp;

	GString* fieldname = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "tables", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "join", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			continue;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "_mode", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			continue;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
		{
			goto _error;
		}

		if (j_bson_iter_find(&iterchild, "_mode", NULL))
		{
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!bind_selector_queryex(backend_data, &iterchild, prepared, variables_count, variables_index, error)))
			{
				goto _error;
			}
		}
		else
		{
			(*variables_count)++;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			JDBTypeValue table_name;
			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_table", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &table_name, error)))
			{
				goto _error;
			}
						
			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_name", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			fieldname = g_string_new(NULL);
			g_string_append_printf(fieldname, "%s.%s" , table_name.val_string, value.val_string);

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_value", error)))
			{
				goto _error;
			}

			type = GPOINTER_TO_UINT(g_hash_table_lookup(prepared->variables_type, (gpointer)fieldname->str));
			
			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, type, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, *variables_count, type, &value, error)))
			{
				goto _error;
			}
	
			if (fieldname)
			{
				g_string_free(fieldname, TRUE);
				fieldname = NULL;
			}
		}
	}

	return TRUE;

_error:
	if (fieldname)
	{
		g_string_free(fieldname, TRUE);
		fieldname = NULL;
	}

	return FALSE;
}

static gboolean
_backend_query(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	JSqlBatch* batch = _batch;
	gboolean sql_found;
	JDBTypeValue value;
	guint count = 0;
	bson_iter_t iter;
	guint variables_count;
	JSqlCacheSQLPrepared* prepared = NULL;
	GString* sql = g_string_new(NULL);
	JSqlIterator* iteratorOut = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) arr_types_out = NULL;
	JDBType type;
	GHashTable* schema_cache = NULL;

	if (!(schema_cache = getCacheSchema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));
	type = J_DB_TYPE_UINT32;
	g_array_append_val(arr_types_out, type);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	*iterator = NULL;
	iteratorOut = g_new(JSqlIterator, 1);
	iteratorOut->namespace = g_strdup(batch->namespace);
	iteratorOut->name = g_strdup(name);
	iteratorOut->index = 0;
	iteratorOut->arr = g_array_new(FALSE, FALSE, sizeof(guint64));

	g_string_append_printf(sql, "SELECT DISTINCT _id FROM " SQL_QUOTE "%s_%s" SQL_QUOTE, batch->namespace, name);

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		g_string_append(sql, " WHERE ");

		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_find(&iter, "_mode", error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
		{
			goto _error;
		}

		mode_child = value.val_uint32;

		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		variables_count = 0;

		if (G_UNLIKELY(!build_selector_query(backend_data, &iter, sql, mode_child, &variables_count, arr_types_in, schema_cache, error)))
		{
			goto _error;
		}
	}

	prepared = getCachePrepared(backend_data, batch->namespace, name, sql->str, error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		prepared->sql = g_string_new(sql->str);
		prepared->variables_count = variables_count;

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, prepared->sql->str, &prepared->stmt, arr_types_in, arr_types_out, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		variables_count = 0;

		if (G_UNLIKELY(!bind_selector_query(backend_data, &iter, prepared, &variables_count, schema_cache, error)))
		{
			goto _error;
		}
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_sql_step(thread_variables->sql_backend, prepared->stmt, &sql_found, error)))
		{
			goto _error;
		}

		if (!sql_found)
		{
			break;
		}
		count++;

		if (G_UNLIKELY(!j_sql_column(thread_variables->sql_backend, prepared->stmt, 0, J_DB_TYPE_UINT32, &value, error)))
		{
			goto _error;
		}

		g_array_append_val(iteratorOut->arr, value.val_uint32);
	}

	if (G_UNLIKELY(!j_sql_reset(thread_variables->sql_backend, prepared->stmt, error)))
	{
		goto _error;
	}

	if (!count)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		goto _error;
	}

	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	*iterator = iteratorOut;

	return TRUE;

_error:
	if (sql)
	{
		g_string_free(sql, TRUE);
	}

	freeJSqlIterator(iteratorOut);

	return FALSE;
}
static gboolean
backend_update(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;
	guint count;
	gboolean equals;
	JDBType type;
	JDBTypeValue value;
	JSqlIterator* iterator = NULL;
	guint variables_count;
	bson_iter_t iter;
	guint index;
	guint j;
	GHashTable* schema_cache = NULL;
	const char* string_tmp;
	gboolean has_next;
	GString* sql = g_string_new(NULL);
	JSqlCacheSQLPrepared* prepared = NULL;
	GHashTable* variables_index = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(metadata != NULL, FALSE);
	g_return_val_if_fail(selector != NULL, FALSE);

	if (!(schema_cache = getCacheSchema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_has_enough_keys(selector, 2, error)))
	{
		goto _error;
	}

	variables_count = 0;
	variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	g_string_append_printf(sql, "UPDATE " SQL_QUOTE "%s_%s" SQL_QUOTE " SET ", batch->namespace, name);

	if (G_UNLIKELY(!j_bson_iter_init(&iter, metadata, error)))
	{
		goto _error;
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_key_equals(&iter, "_index", &equals, error)))
		{
			goto _error;
		}

		if (equals)
		{
			continue;
		}

		if (variables_count)
		{
			g_string_append(sql, ", ");
		}

		variables_count++;
		string_tmp = j_bson_iter_key(&iter, error);

		if (G_UNLIKELY(!string_tmp))
		{
			goto _error;
		}

		type = GPOINTER_TO_INT(g_hash_table_lookup(schema_cache, string_tmp));
		g_array_append_val(arr_types_in, type);
		g_string_append_printf(sql, SQL_QUOTE "%s" SQL_QUOTE " = ?", string_tmp);
		g_hash_table_insert(variables_index, g_strdup(string_tmp), GINT_TO_POINTER(variables_count));
	}

	variables_count++;
	type = J_DB_TYPE_UINT32;
	g_array_append_val(arr_types_in, type);
	g_string_append_printf(sql, " WHERE _id = ?");
	g_hash_table_insert(variables_index, g_strdup("_id"), GINT_TO_POINTER(variables_count));
	prepared = getCachePrepared(backend_data, batch->namespace, name, sql->str, error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		prepared->sql = sql;
		sql = NULL;
		prepared->variables_count = variables_count;
		prepared->variables_index = variables_index;
		variables_index = NULL;

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, prepared->sql->str, &prepared->stmt, arr_types_in, NULL, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	if (G_UNLIKELY(!_backend_query(backend_data, batch, name, selector, (gpointer*)&iterator, error)))
	{
		goto _error;
	}

	for (j = 0; j < iterator->arr->len; j++)
	{
		count = 0;
		index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, "_id"));

		if (G_UNLIKELY(!index))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
			goto _error;
		}

		value.val_uint32 = g_array_index(iterator->arr, guint64, j);

		if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, index, J_DB_TYPE_UINT32, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_init(&iter, metadata, error)))
		{
			goto _error;
		}

		while (TRUE)
		{
			if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
			{
				goto _error;
			}

			if (!has_next)
			{
				break;
			}

			string_tmp = j_bson_iter_key(&iter, error);

			if (G_UNLIKELY(!string_tmp))
			{
				goto _error;
			}

			type = GPOINTER_TO_INT(g_hash_table_lookup(schema_cache, string_tmp));
			index = GPOINTER_TO_INT(g_hash_table_lookup(prepared->variables_index, string_tmp));

			if (G_UNLIKELY(!index))
			{
				g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
				goto _error;
			}

			count++;

			if (G_UNLIKELY(!j_bson_iter_value(&iter, type, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, index, type, &value, error)))
			{
				goto _error;
			}
		}

		if (G_UNLIKELY(!j_sql_step_and_reset_check_done(thread_variables->sql_backend, prepared->stmt, error)))
		{
			goto _error;
		}

		if (!count)
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
			goto _error;
		}
	}

	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	if (variables_index)
		g_hash_table_destroy(variables_index);

	freeJSqlIterator(iterator);

	return TRUE;

_error:
	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	if (variables_index)
		g_hash_table_destroy(variables_index);

	freeJSqlIterator(iterator);

	if (G_UNLIKELY(!_backend_batch_abort(backend_data, batch, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBType type;
	JSqlBatch* batch = _batch;
	JSqlIterator* iterator = NULL;
	guint j;
	JDBTypeValue value;
	JSqlCacheSQLPrepared* prepared = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	type = J_DB_TYPE_UINT32;
	g_array_append_val(arr_types_in, type);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (G_UNLIKELY(!_backend_query(backend_data, batch, name, selector, (gpointer*)&iterator, error)))
	{
		goto _error;
	}

	prepared = getCachePrepared(backend_data, batch->namespace, name, "_delete", error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	if (!prepared->initialized)
	{
		prepared->sql = g_string_new(NULL);
		prepared->variables_count = 1;
		g_string_append_printf(prepared->sql, "DELETE FROM " SQL_QUOTE "%s_%s" SQL_QUOTE " WHERE _id = ?", batch->namespace, name);

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, prepared->sql->str, &prepared->stmt, arr_types_in, NULL, error)))
		{
			goto _error;
		}

		prepared->initialized = TRUE;
	}

	for (j = 0; j < iterator->arr->len; j++)
	{
		value.val_uint32 = g_array_index(iterator->arr, guint64, j);

		if (G_UNLIKELY(!j_sql_bind_value(thread_variables->sql_backend, prepared->stmt, 1, J_DB_TYPE_UINT32, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_sql_step_and_reset_check_done(thread_variables->sql_backend, prepared->stmt, error)))
		{
			goto _error;
		}
	}

	freeJSqlIterator(iterator);

	return TRUE;

_error:
	freeJSqlIterator(iterator);

	if (G_UNLIKELY(!_backend_batch_abort(backend_data, batch, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}

static gboolean
backend_query(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	GHashTableIter schema_iter;

	GHashTable* schema_cache = NULL;
	JDBType type;
	gpointer type_tmp;

	JSqlBatch* batch = _batch;
	bson_iter_t iter;
	guint variables_count;
	guint variables_count2;
	JDBTypeValue value;
	char* string_tmp;
	JSqlCacheSQLPrepared* prepared = NULL;
	GHashTable* variables_index = NULL;			// Maintains indices for the fields (or columns) in the query so that their respective values-
								// can be fetched from the resultant vector using the indices.
	GHashTable* variables_type = NULL;			// Maintains data types of the fields (or columns) that are involved in the query.
	GString* sql = g_string_new(NULL);			// Maintains query string.
	GString* sql_selection_part = g_string_new(NULL);	// Maintains selection part of the query string. (e.g. SELECT A,B,C,... FROM X,Y,Z,...)
	GString* sql_join_part = g_string_new(NULL);		// Maintains join part of the query string.
	GString* sql_condition_part = g_string_new(NULL);	// Maintains condition part of the query string. (e.g. A = ? AND B < ? OR C > ? ,...)
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) arr_types_out = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	// Initialize the hash table to maintain indices. 
	variables_index = g_hash_table_new_full(g_direct_hash, NULL, NULL, g_free);

	// Initialize the hash table to maintain data types. 
	variables_type = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);

	variables_count = 0;
	g_string_append(sql, "SELECT ");

	// Formulate the selection part of the query.
	build_query_selection_part(selector, backend_data, sql_selection_part, batch, variables_index, variables_type, &variables_count, arr_types_out, error);

	// Extend the query string.
	g_string_append(sql, sql_selection_part->str);

	// Formulate the join part of the query.
	build_query_join_part(selector, backend_data, sql_join_part, error);

	// Extend the query string.
	if (sql_join_part->len >0)
	{
		g_string_append(sql, " WHERE ");
		g_string_append(sql, sql_join_part->str);
	}

	// Formulate the condition part of the query.
	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		// Fetch the operator that would be appended in between parent and child selector.
		if (G_UNLIKELY(!j_bson_iter_find(&iter, "_mode", error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
		{
			goto _error;
		}

		mode_child = value.val_uint32;

		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		variables_count2 = 0;

		if (G_UNLIKELY(!build_selector_query_ex(&iter, sql_condition_part, mode_child, &variables_count2, arr_types_in, variables_type, error)))
		{
			goto _error;
		}
	}

	// Extend the query string.
	if (sql_condition_part->len >0)
	{
		if (sql_join_part->len > 0)
		{
			g_string_append(sql, " AND ");
		}
		else
		{
			g_string_append(sql, " WHERE ");
		}
		
		g_string_append(sql, sql_condition_part->str);
	}
	
	prepared = getCachePrepared(backend_data, batch->namespace, name, sql->str, error);

	if (G_UNLIKELY(!prepared))
	{
		goto _error;
	}

	/*
	 * The following code snippet prepares the query string object. 
	 * E.g. in the case of sqlite, after the initialzion of sqlite object and establishment of the connection, the SQL query string must be first 
	 * compiled into byte codes. Refer to this link for details: https://www.sqlite.org/cintro.html. 
	 */
	if (!prepared->initialized)
	{
		prepared->sql = g_string_new(sql->str);
		prepared->variables_index = variables_index;
		prepared->variables_count = variables_count;
		prepared->variables_type = variables_type;

		if (G_UNLIKELY(!j_sql_prepare(thread_variables->sql_backend, prepared->sql->str, &prepared->stmt, arr_types_in, arr_types_out, error)))
		{
			goto _error;
		}
		prepared->initialized = TRUE;
	}
	else
	{
		g_hash_table_destroy(variables_index);
		variables_index = NULL;
		g_hash_table_destroy(variables_type);
		variables_type = NULL;
		// TODO: shouldn't the control return with a 'FALSE'?
	}

	/*
	 * In the case of sqlite, after the compilation of the query string into byte-code, the query parameters are required to be binded with the provided values.
	 */
	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		variables_count2 = 0;

		if (G_UNLIKELY(!bind_selector_queryex(backend_data, &iter, prepared, &variables_count2, variables_type, error)))
		{
			goto _error;
		}
	}

	*iterator = prepared;

	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	g_string_free(sql_selection_part, TRUE);
	g_string_free(sql_join_part, TRUE);
	g_string_free(sql_condition_part, TRUE);

	return TRUE;

_error:
	if (sql)
	{
		g_string_free(sql, TRUE);
		sql = NULL;
	}

	if (variables_index)
	{
		g_hash_table_destroy(variables_index);
	}

	if (variables_type)
	{
		g_hash_table_destroy(variables_type);
	}

	g_string_free(sql_selection_part, TRUE);
	g_string_free(sql_join_part, TRUE);
	g_string_free(sql_condition_part, TRUE);

	return FALSE;
}

static gboolean
backend_iterate(gpointer backend_data, gpointer _iterator, bson_t* metadata, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	GHashTable* schema_cache = NULL;
	const char* string_tmp;
	guint i;
	JSqlCacheSQLQueries* queries = NULL;
	JDBTypeValue value;
	JDBType type;
	gboolean sql_found;
	JSqlCacheSQLPrepared* prepared = _iterator;
	gboolean found = FALSE;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!(queries = _getCachePrepared(backend_data, prepared->namespace, prepared->name, error)))
	{
		goto _error;
	}

	if (!(schema_cache = queries->types))
	{
		goto _error;
	}

	// In the case of sqlite it is required to check if there is still any result row left to be processed.
	if (G_UNLIKELY(!j_sql_step(thread_variables->sql_backend, prepared->stmt, &sql_found, error)))
	{
		goto _error;
	}

	
	if (sql_found)
	{
		found = TRUE;

		for (i = 0; i < prepared->variables_count; i++)
		{
			string_tmp = g_hash_table_lookup(prepared->variables_index, GINT_TO_POINTER(i));

			// The following code snippet extracts the type of the field (column). An extension is made for join operation that is given below.
			if ( prepared->variables_type != NULL)
			{
				/*
				 * This code snippet is added to address join operation. In the old implemntation the columns' data type is extracted using the
				 * table schema. When multiple tables are invovled (as it is in the case of join), instead of fetching tables' schema again and again,
				 * it is more feasible to store the data types in a hash table and use it when required, as it is done in this case. 
				 * This code would only hit when it is called for a SELECT query and the function "build_query_selection_part" populates the hash-table for data types.
				 */
				type = GPOINTER_TO_UINT(g_hash_table_lookup(prepared->variables_type, string_tmp));
			}
			else
			{
				// Entertains the old implementation.
				type = GPOINTER_TO_INT(g_hash_table_lookup(schema_cache, string_tmp));
			}

			if (G_UNLIKELY(!j_sql_column(thread_variables->sql_backend, prepared->stmt, i, type, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_append_value(metadata, string_tmp, type, &value, error)))
			{
				goto _error;
			}
		}
	}

	if (!found)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		goto _error;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(!j_sql_reset(thread_variables->sql_backend, prepared->stmt, NULL)))
	{
		goto _error3;
	}

	return FALSE;

_error3:
	/*something failed very hard*/
	return FALSE;
}
#endif
