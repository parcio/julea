/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2022 Timm Leon Erxleben
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

#include "sql-generic-internal.h"

gboolean
_backend_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlStatement* metadata_query = NULL;
	const gchar* metadata_query_sql = "SELECT varname, vartype FROM schema_structure WHERE namespace=? AND name=?";
	JDBTypeValue value1;
	JDBTypeValue value2;
	JSqlBatch* batch = _batch;
	guint found = FALSE;
	gboolean sql_found;
	gboolean bson_initialized = FALSE;
	JThreadVariables* thread_variables = NULL;
	JDBType type;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	metadata_query = g_hash_table_lookup(thread_variables->query_cache, metadata_query_sql);

	if (G_UNLIKELY(!metadata_query))
	{
		g_autoptr(GArray) arr_types_in = NULL;
		g_autoptr(GArray) arr_types_out = NULL;
		arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
		arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));

		type = J_DB_TYPE_STRING;
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_out, type);
		type = J_DB_TYPE_UINT32;
		g_array_append_val(arr_types_out, type);

		if (!(metadata_query = j_sql_statement_new(metadata_query_sql, arr_types_in, arr_types_out, NULL, NULL)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(metadata_query_sql), metadata_query))
		{
			// in all other error cases metadata_query is already owned by the hash table
			j_sql_statement_free(metadata_query);
			goto _error;
		}
	}

	value1.val_string = batch->namespace;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_query->stmt, 1, J_DB_TYPE_STRING, &value1, error)))
	{
		goto _error;
	}

	value1.val_string = name;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_query->stmt, 2, J_DB_TYPE_STRING, &value1, error)))
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
		if (G_UNLIKELY(!specs->func.statement_step(thread_variables->db_connection, metadata_query->stmt, &sql_found, error)))
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

		if (G_UNLIKELY(!specs->func.statement_column(thread_variables->db_connection, metadata_query->stmt, 0, J_DB_TYPE_STRING, &value1, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!specs->func.statement_column(thread_variables->db_connection, metadata_query->stmt, 1, J_DB_TYPE_UINT32, &value2, error)))
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

	if (G_UNLIKELY(!specs->func.statement_reset(thread_variables->db_connection, metadata_query->stmt, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(thread_variables == NULL || metadata_query == NULL))
	{
		goto _error2;
	}

	if (G_UNLIKELY(!specs->func.statement_reset(thread_variables->db_connection, metadata_query->stmt, NULL)))
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

GHashTable*
get_schema(gpointer backend_data, gpointer _batch, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;
	JThreadVariables* thread_variables = NULL;
	GHashTable* schema_map = NULL;
	bson_t schema;
	g_autoptr(GString) cache_key = g_string_new(NULL);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	g_string_append_printf(cache_key, "%s_%s", batch->namespace, name);
	schema_map = g_hash_table_lookup(thread_variables->schema_cache, cache_key->str);

	if (!schema_map)
	{
		char const* string_tmp;
		gboolean has_next;
		JDBTypeValue value;
		bson_iter_t iter;
		gboolean schema_initialized = FALSE;

		schema_map = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

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

			if (G_UNLIKELY(!j_bson_iter_skip_key(&iter, "_index", error)))
			{
				// there was either an error or no key is left now
				if (*error)
				{
					goto _error;
				}
				else
				{
					break;
				}
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

			g_hash_table_insert(schema_map, g_strdup(string_tmp), GINT_TO_POINTER(value.val_uint32));
		}

		g_hash_table_insert(thread_variables->schema_cache, g_strdup(cache_key->str), schema_map);
	}

	return schema_map;

_error:

	if (schema_map)
	{
		g_hash_table_unref(schema_map);
	}

	return NULL;
}

gboolean
sql_generic_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error)
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

gboolean
build_selector_query(gpointer backend_data, bson_iter_t* iter, GString* sql, JDBSelectorMode mode, GArray* arr_types_in, GHashTable* schema_cache, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	gboolean has_next;
	JDBSelectorOperator op;
	gboolean first = TRUE;
	const char* string_tmp;
	JDBTypeValue value;
	bson_iter_t iterchild;
	JDBType type;

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

		if (G_UNLIKELY(!j_bson_iter_skip_key(iter, "_mode", error)))
		{
			// there was either an error or no key is left now
			if (*error)
			{
				goto _error;
			}
			else
			{
				break;
			}
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

			if (G_UNLIKELY(!build_selector_query(backend_data, &iterchild, sql, mode_child, arr_types_in, schema_cache, error)))
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
			g_string_append_printf(sql, "%s%s%s ", specs->sql.quote, string_tmp, specs->sql.quote);

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
_bind_selector_query(gpointer backend_data, bson_iter_t* iter, JSqlStatement* statement, GHashTable* schema, guint64* position, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterchild;
	JDBTypeValue value;
	JDBType type;
	gboolean has_next;
	JThreadVariables* thread_variables = NULL;
	const gchar* string_tmp;

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

		if (G_UNLIKELY(!j_bson_iter_skip_key(iter, "_mode", error)))
		{
			// there was either an error or no key is left now
			if (*error)
			{
				goto _error;
			}
			else
			{
				break;
			}
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

			if (G_UNLIKELY(!_bind_selector_query(backend_data, &iterchild, statement, schema, position, error)))
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

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "_value", error)))
			{
				goto _error;
			}

			type = GPOINTER_TO_INT(g_hash_table_lookup(schema, string_tmp));

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, type, &value, error)))
			{
				goto _error;
			}

			(*position)++;

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, statement->stmt, *position, type, &value, error)))
			{
				goto _error;
			}
		}
	}

	return TRUE;

_error:
	return FALSE;
}

gboolean
bind_selector_query(gpointer backend_data, bson_iter_t* iter, JSqlStatement* statement, GHashTable* schema, GError** error)
{
	guint64 pos = 0;

	return _bind_selector_query(backend_data, iter, statement, schema, &pos, error);
}

// this function is used for update and delete and returns the IDs of entries that match the selector
gboolean
_backend_query_ids(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GArray** matches, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	JSqlBatch* batch = _batch;
	gboolean sql_found;
	JDBTypeValue value;
	guint count = 0;
	bson_iter_t iter;
	JSqlStatement* id_query = NULL;
	GString* id_sql = g_string_new(NULL);
	GArray* ids_out = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) arr_types_out = NULL;
	JDBType type;
	GHashTable* schema_cache = NULL;

	if (!(schema_cache = get_schema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));
	type = BACKEND_ID_TYPE;
	g_array_append_val(arr_types_out, type);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	ids_out = g_array_new(FALSE, FALSE, sizeof(guint64));

	g_string_append_printf(id_sql, "SELECT DISTINCT _id FROM %s%s_%s%s", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		g_string_append(id_sql, " WHERE ");

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

		if (G_UNLIKELY(!build_selector_query(backend_data, &iter, id_sql, mode_child, arr_types_in, schema_cache, error)))
		{
			goto _error;
		}
	}

	// even though the string and variables_index need to be built anyway this statement is cached because the DB server might cache some ressources as consequence (e.g., the execution plan)
	/// \todo check if caching this statement makes any difference with different databases
	id_query = g_hash_table_lookup(thread_variables->query_cache, id_sql->str);

	if (!id_query)
	{
		// the only out_variable _id is hard coded
		if (!(id_query = j_sql_statement_new(id_sql->str, arr_types_in, arr_types_out, NULL, NULL)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(id_sql->str), id_query))
		{
			// in all other error cases id_query is already owned by the hash table
			j_sql_statement_free(id_query);
			goto _error;
		}
	}

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!bind_selector_query(backend_data, &iter, id_query, schema_cache, error)))
		{
			goto _error;
		}
	}

	while (TRUE)
	{
		if (G_UNLIKELY(!specs->func.statement_step(thread_variables->db_connection, id_query->stmt, &sql_found, error)))
		{
			goto _error;
		}

		if (!sql_found)
		{
			break;
		}
		count++;

		if (G_UNLIKELY(!specs->func.statement_column(thread_variables->db_connection, id_query->stmt, 0, BACKEND_ID_TYPE, &value, error)))
		{
			goto _error;
		}

		g_array_append_val(ids_out, value.val_uint64);
	}

	specs->func.statement_reset(thread_variables->db_connection, id_query->stmt, error);

	if (!count)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		goto _error;
	}

	if (id_sql)
	{
		g_string_free(id_sql, TRUE);
		id_sql = NULL;
	}

	*matches = ids_out;

	return TRUE;

_error:
	if (id_sql)
	{
		g_string_free(id_sql, TRUE);
	}

	if (ids_out)
	{
		g_array_free(ids_out, TRUE);
	}

	return FALSE;
}

gboolean
sql_generic_query(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBSelectorMode mode_child;
	GHashTableIter schema_iter;

	// the schema is owned by the schema cache
	GHashTable* schema = NULL;
	JDBType type;
	gpointer type_tmp;

	JSqlBatch* batch = _batch;
	bson_iter_t iter;
	guint variables_count;
	JDBTypeValue value;
	char* string_tmp;
	JSqlStatement* query_statement = NULL;
	g_autoptr(GHashTable) out_variables_index = NULL;
	g_autoptr(GString) sql = g_string_new(NULL);
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

	out_variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	g_string_append(sql, "SELECT ");
	variables_count = 0;

	if (!(schema = get_schema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	g_hash_table_iter_init(&schema_iter, schema);

	g_string_append(sql, "_id");

	g_hash_table_insert(out_variables_index, g_strdup("_id"), GINT_TO_POINTER(variables_count));
	type = BACKEND_ID_TYPE;
	g_array_append_val(arr_types_out, type);
	variables_count++;

	while (g_hash_table_iter_next(&schema_iter, (gpointer*)&string_tmp, &type_tmp))
	{
		type = GPOINTER_TO_INT(type_tmp);

		if (strcmp(string_tmp, "_id") == 0)
			continue;

		g_string_append_printf(sql, ", %s%s%s", specs->sql.quote, string_tmp, specs->sql.quote);

		g_hash_table_insert(out_variables_index, g_strdup(string_tmp), GINT_TO_POINTER(variables_count));
		g_array_append_val(arr_types_out, type);
		variables_count++;
	}

	g_string_append_printf(sql, " FROM %s%s_%s%s", specs->sql.quote, batch->namespace, name, specs->sql.quote);

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

		if (G_UNLIKELY(!build_selector_query(backend_data, &iter, sql, mode_child, arr_types_in, schema, error)))
		{
			goto _error;
		}
	}

	/// \todo check if caching this statement makes any difference with different databases
	query_statement = g_hash_table_lookup(thread_variables->query_cache, sql->str);

	if (G_UNLIKELY(!query_statement))
	{
		if (!(query_statement = j_sql_statement_new(sql->str, arr_types_in, arr_types_out, NULL, &out_variables_index)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(sql->str), query_statement))
		{
			// in all other error cases query_statement is already owned by the hash table
			j_sql_statement_free(query_statement);
			goto _error;
		}
	}

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!bind_selector_query(backend_data, &iter, query_statement, schema, error)))
		{
			goto _error;
		}
	}

	*iterator = j_sql_iterator_new(query_statement, batch, name);

	return TRUE;

_error:
	return FALSE;
}

gboolean
sql_generic_iterate(gpointer backend_data, gpointer _iterator, bson_t* query_result, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	GHashTable* schema = NULL;
	JDBTypeValue value;
	JDBType type;
	gboolean sql_found;
	JSqlIterator* iter = _iterator;
	JSqlStatement* bound_statement = iter->statement;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!(schema = get_schema(backend_data, iter->batch, iter->name, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_step(thread_variables->db_connection, bound_statement->stmt, &sql_found, error)))
	{
		goto _error;
	}

	if (sql_found)
	{
		GHashTableIter map_iter;
		gpointer var_name, position;

		g_hash_table_iter_init(&map_iter, bound_statement->out_variables_index);
		while (g_hash_table_iter_next(&map_iter, &var_name, &position))
		{
			type = GPOINTER_TO_INT(g_hash_table_lookup(schema, (gchar*)var_name));

			if (G_UNLIKELY(!specs->func.statement_column(thread_variables->db_connection, bound_statement->stmt, GPOINTER_TO_INT(position), type, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_append_value(query_result, (gchar*)var_name, type, &value, error)))
			{
				goto _error;
			}
		}
	}
	else
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements while iterating");
		goto _error;
	}

	return TRUE;

_error:
	j_sql_iterator_free(iter);

	if (G_UNLIKELY(!specs->func.statement_reset(thread_variables->db_connection, bound_statement->stmt, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}
