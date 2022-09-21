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
_backend_schema_get(gpointer backend_data, gchar const* namespace, gchar const* name, bson_t* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue value1;
	JDBTypeValue value2;
	gboolean found = FALSE;
	gboolean bson_initialized = FALSE;
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* metadata_query = NULL;
	const gchar* metadata_query_sql = "SELECT varname, vartype FROM schema_structure WHERE namespace=? AND name=?";

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	metadata_query = g_hash_table_lookup(thread_variables->query_cache, metadata_query_sql);

	if (G_UNLIKELY(!metadata_query))
	{
		JDBType type;

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

		if (!(metadata_query = j_sql_statement_new(metadata_query_sql, arr_types_in, arr_types_out, NULL, NULL, NULL, error)))
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

	value1.val_string = namespace;

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
		gboolean sql_found;

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
get_schema(gpointer backend_data, gchar const* namespace, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;
	GHashTable* schema_map = NULL;
	g_autoptr(GString) cache_key = g_string_new(namespace);

	g_string_append(cache_key, "_");
	g_string_append(cache_key, name);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	schema_map = g_hash_table_lookup(thread_variables->schema_cache, cache_key->str);

	if (!schema_map)
	{
		bson_t schema;
		bson_iter_t iter;
		gboolean has_next;
		JDBTypeValue value;
		gboolean schema_initialized = FALSE;
		char const* string_tmp;

		schema_map = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

		if (!(schema_initialized = _backend_schema_get(backend_data, namespace, name, &schema, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_init(&iter, &schema, error)))
		{
			goto _error;
		}

		while (TRUE)
		{
			g_autoptr(GString) full_name = NULL;

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

			full_name = get_full_field_name(namespace, name, string_tmp);

			g_hash_table_insert(schema_map, g_strdup(full_name->str), GINT_TO_POINTER(value.val_uint32));
		}

		g_hash_table_insert(thread_variables->schema_cache, g_strdup(cache_key->str), schema_map);
	}

	return g_hash_table_ref(schema_map);

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
	JSqlBatch* batch = _batch;

	ret = _backend_schema_get(backend_data, batch->namespace, name, schema, error);

	if (!ret)
	{
		_backend_batch_abort(backend_data, _batch, NULL);
	}

	return ret;
}

static guint
get_full_field_name_length(const gchar* namespace, const gchar* table, const gchar* field)
{
	guint ns, t, f;
	// the full and correctly quoted field name is <quote><namespace>_<table><quote>.<quote><field><quote>

	ns = strlen(namespace);
	t = strlen(table);
	f = strlen(field);

	// one more than 6 additional characters for \0
	return ns + t + f + 7;
}

GString*
get_full_field_name(const gchar* namespace, const gchar* table, const gchar* field)
{
	GString* res = g_string_sized_new(get_full_field_name_length(namespace, table, field));
	// the full and correctly quoted field name is <quote><namespace>_<table><quote>.<quote><field><quote>

	g_string_append(res, specs->sql.quote);
	g_string_append(res, namespace);
	g_string_append(res, "_");
	g_string_append(res, table);
	g_string_append(res, specs->sql.quote);
	g_string_append(res, ".");
	g_string_append(res, specs->sql.quote);
	g_string_append(res, field);
	g_string_append(res, specs->sql.quote);

	return res;
}

gboolean
build_query_condition_part(gpointer backend_data, JSqlBatch* batch, bson_iter_t* iter, GString* sql, JDBSelectorMode mode, GArray* arr_types_in, GHashTable* variable_types, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean first = TRUE;

	g_string_append(sql, "( ");

	while (TRUE)
	{
		JDBTypeValue value;
		gboolean has_next;
		bson_iter_t iterchild;
		const gchar* key = NULL;

		if (G_UNLIKELY(!j_bson_iter_next(iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_skip_key(iter, "m", error)))
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

		key = j_bson_iter_key(iter, error);

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

		if (g_str_equal(key, "_s"))
		{
			JDBSelectorMode mode_child;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			j_bson_iter_find(&iterchild, "m", error);

			if (G_UNLIKELY(!j_bson_iter_value(&iterchild, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			mode_child = value.val_uint32;

			if (G_UNLIKELY(!build_query_condition_part(backend_data, batch, &iterchild, sql, mode_child, arr_types_in, variable_types, error)))
			{
				goto _error;
			}
		}
		else
		{
			JDBType type;
			JDBSelectorOperator op;
			g_autoptr(GString) full_name = NULL;
			const gchar* field = NULL;
			const gchar* table = NULL;
			g_autoptr(GHashTable) schema = NULL;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			field = j_bson_iter_key(iter, error);

			j_bson_iter_find(&iterchild, "t", error);

			j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error);

			table = value.val_string;

			full_name = get_full_field_name(batch->namespace, table, field);

			schema = get_schema(backend_data, batch->namespace, table, error);

			g_string_append(sql, full_name->str);

			// add type infos
			type = GPOINTER_TO_INT(g_hash_table_lookup(schema, full_name->str));
			g_array_append_val(arr_types_in, type);

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "o", error)))
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

	// the selector was empty (which is allowed and will be the case for simple queries like 'SELECT * FROM table')
	if (first)
	{
		// this is only valid if sql does not contain any previous results!
		// if we decide to append to previous query parts another solution would be to append "(TRUE)""
		g_string_assign(sql, "");
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
_bind_selector_query(gpointer backend_data, const gchar* namespace, bson_iter_t* iter, JSqlStatement* statement, GHashTable* variable_types, guint64* position, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	while (TRUE)
	{
		bson_iter_t iterchild;
		JDBTypeValue value;
		gboolean has_next;

		if (G_UNLIKELY(!j_bson_iter_next(iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		if (G_UNLIKELY(!j_bson_iter_skip_key(iter, "m", error)))
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

		// ?
		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
		{
			goto _error;
		}

		if (j_bson_iter_find(&iterchild, "m", NULL))
		{
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!_bind_selector_query(backend_data, namespace, &iterchild, statement, variable_types, position, error)))
			{
				goto _error;
			}
		}
		else
		{
			JDBType type;
			g_autoptr(GString) full_name = NULL;
			const gchar* field;
			const gchar* table;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterchild, error)))
			{
				goto _error;
			}

			field = j_bson_iter_key(iter, error);

			if (!j_bson_iter_find(&iterchild, "t", error))
			{
				goto _error;
			}

			if (!j_bson_iter_value(&iterchild, J_DB_TYPE_STRING, &value, error))
			{
				goto _error;
			}

			table = value.val_string;

			full_name = get_full_field_name(namespace, table, field);

			if (G_UNLIKELY(!j_bson_iter_find(&iterchild, "v", error)))
			{
				goto _error;
			}

			type = GPOINTER_TO_INT(g_hash_table_lookup(statement->variable_types, full_name->str));

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
bind_selector_query(gpointer backend_data, const gchar* namespace, bson_iter_t* iter, JSqlStatement* statement, GHashTable* schema, GError** error)
{
	guint64 pos = 0;

	return _bind_selector_query(backend_data, namespace, iter, statement, schema, &pos, error);
}

static gboolean
build_query_selection_part(bson_t const* selector, gpointer backend_data, GString* sql, JSqlBatch* batch, GHashTable* out_variables_index, GHashTable* variables_type, GArray* arr_types_out, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterChildDocument;
	bson_iter_t iterCurrentDocument;
	gboolean first = TRUE;

	g_autoptr(GString) sqlTablesName = g_string_new(NULL);

	// Initialize iterator for BSON document.
	if (G_UNLIKELY(!j_bson_iter_init(&iterCurrentDocument, selector, error)))
	{
		goto _error;
	}

	// Look for the key that is assigned to BSON array that contains tables' information.
	if (G_UNLIKELY(!j_bson_iter_find(&iterCurrentDocument, "t", error)))
	{
		// no joins present
		goto _error;
	}

	// As per the format the respective values are added as a child document therefore initializing the iterator.
	if (G_UNLIKELY(!j_bson_iter_recurse_array(&iterCurrentDocument, &iterChildDocument, error)))
	{
		goto _error;
	}

	// Iterate through all the child elements.
	while (TRUE)
	{
		gboolean hasItems;
		g_autoptr(GString) tableName = NULL;
		g_autoptr(GHashTable) schema = NULL;
		GHashTableIter schemaIter;
		JDBTypeValue itemValue;
		gpointer key;
		gpointer field_type;

		if (G_UNLIKELY(!j_bson_iter_next(&iterChildDocument, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems)
		{
			break;
		}

		// Extract the table's name
		if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &itemValue, error)))
		{
			goto _error;
		}

		tableName = g_string_new(batch->namespace); // Add namespace for the table.
		g_string_append(tableName, "_");
		g_string_append(tableName, itemValue.val_string);

		// Fetching table's schema to extract all the columns' names.
		if (!(schema = get_schema(backend_data, batch->namespace, itemValue.val_string, error)))
		{
			goto _error;
		}

		g_hash_table_iter_init(&schemaIter, schema); // Initializing the iterator.

		while (g_hash_table_iter_next(&schemaIter, &key, &field_type))
		{
			JDBType itemDataType;
			const gchar* field = key;

			itemDataType = GPOINTER_TO_INT(field_type);

			if (!first)
			{
				g_string_append(sql, ", ");
			}

			first = FALSE;

			g_string_append(sql, field);

			if (!g_hash_table_insert(out_variables_index, g_strdup(field), GINT_TO_POINTER(g_hash_table_size(out_variables_index))))
			{
				goto _error;
			}

			if (!g_hash_table_insert(variables_type, g_strdup(field), GINT_TO_POINTER(itemDataType)))
			{
				goto _error;
			}

			g_array_append_val(arr_types_out, itemDataType);
		}

		if (sqlTablesName->len > 0)
		{
			g_string_append(sqlTablesName, ", ");
		}

		g_string_append_printf(sqlTablesName, "%s%s%s", specs->sql.quote, tableName->str, specs->sql.quote);
	}

	g_string_append(sql, " FROM ");
	g_string_append(sql, sqlTablesName->str);

	return TRUE;

_error:
	return FALSE;
}

static gboolean
build_query_join_part(bson_t const* selector, JSqlBatch* batch, GString* sql, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean hasItems;
	JDBTypeValue itemValue;

	bson_iter_t iter_selector;
	bson_iter_t iter_joins;
	bson_iter_t iter_join_entry;

	// Initialize the iterator.
	if (G_UNLIKELY(!j_bson_iter_init(&iter_selector, selector, error)))
	{
		goto _error;
	}

	if (!j_bson_iter_find(&iter_selector, "j", error))
	{
		goto _error;
	}

	if (!j_bson_iter_recurse_document(&iter_selector, &iter_joins, error))
	{
		goto _error;
	}

	// The following code iterates through the BSON elements and prepares query part for the join operation.
	while (TRUE)
	{
		g_autoptr(GString) sqlJoin = NULL;
		g_autoptr(GString) first = NULL;
		g_autoptr(GString) second = NULL;
		gboolean next = FALSE;
		const gchar* table = NULL;
		const gchar* field = NULL;

		if (G_UNLIKELY(!j_bson_iter_next(&iter_joins, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems)
		{
			break;
		}

		// Recurse into join of the form { "<table_name>" : "<field_name>", "<table_name>" : "<field_name>" }
		if (G_UNLIKELY(!j_bson_iter_recurse_document(&iter_joins, &iter_join_entry, error)))
		{
			goto _error;
		}

		// First field
		j_bson_iter_next(&iter_join_entry, &next, error);

		if (!next)
		{
			goto _error;
		}

		table = j_bson_iter_key(&iter_join_entry, error);

		if (!j_bson_iter_value(&iter_join_entry, J_DB_TYPE_STRING, &itemValue, error))
		{
			goto _error;
		}

		field = itemValue.val_string;

		first = get_full_field_name(batch->namespace, table, field);

		// Second field
		j_bson_iter_next(&iter_join_entry, &next, error);

		if (!next)
		{
			goto _error;
		}

		table = j_bson_iter_key(&iter_join_entry, error);

		if (!j_bson_iter_value(&iter_join_entry, J_DB_TYPE_STRING, &itemValue, error))
		{
			goto _error;
		}

		field = itemValue.val_string;

		second = get_full_field_name(batch->namespace, table, field);

		// add parts together
		sqlJoin = g_string_new(first->str);
		g_string_append(sqlJoin, " = ");
		g_string_append(sqlJoin, second->str);

		if (sql->len > 0)
		{
			g_string_append(sql, " AND ");
		}

		g_string_append(sql, sqlJoin->str);
	}

	return TRUE;

_error:
	return FALSE;
}

gboolean
_backend_query_ids(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GArray** matches, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue value;
	guint count = 0;
	bson_iter_t iter, child;
	JSqlBatch* batch = _batch;
	g_autoptr(GHashTable) schema = NULL;
	JSqlStatement* id_query = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GString) id_sql = g_string_new(NULL);
	g_autoptr(GArray) arr_types_in = NULL;
	GArray* ids_out = NULL;

	if (!(schema = get_schema(backend_data, batch->namespace, name, error)))
	{
		goto _error;
	}

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	ids_out = g_array_new(FALSE, FALSE, sizeof(guint64));

	g_string_append_printf(id_sql, "SELECT DISTINCT _id FROM %s%s_%s%s", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (selector)
	{
		JDBSelectorMode mode_child;

		g_string_append(id_sql, " WHERE ");

		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_find(&iter, "s", error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_document(&iter, &child, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_find(&child, "m", error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_value(&child, J_DB_TYPE_UINT32, &value, error)))
		{
			goto _error;
		}

		mode_child = value.val_uint32;

		if (G_UNLIKELY(!build_query_condition_part(backend_data, batch, &child, id_sql, mode_child, arr_types_in, schema, error)))
		{
			goto _error;
		}
	}

	// even though the string and variables_index need to be built anyway this statement is cached because the DB server might cache some ressources as consequence (e.g., the execution plan)
	/// \todo check if caching this statement makes any difference with different databases
	id_query = g_hash_table_lookup(thread_variables->query_cache, id_sql->str);

	if (!id_query)
	{
		g_autoptr(GArray) arr_types_out = NULL;
		JDBType type = BACKEND_ID_TYPE;

		arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));
		g_array_append_val(arr_types_out, type);

		// the only out_variable _id is hard coded
		if (!(id_query = j_sql_statement_new(id_sql->str, arr_types_in, arr_types_out, NULL, NULL, schema, error)))
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

	if (selector)
	{
		if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_find(&iter, "s", error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_document(&iter, &child, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!bind_selector_query(backend_data, batch->namespace, &child, id_query, schema, error)))
		{
			goto _error;
		}
	}

	while (TRUE)
	{
		gboolean sql_found;

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

	*matches = ids_out;

	return TRUE;

_error:

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
	JSqlBatch* batch = _batch;
	bson_iter_t iter, iter_selection;
	JDBTypeValue value;
	JSqlStatement* statement = NULL;
	JThreadVariables* thread_variables = NULL;
	gboolean where_appended = FALSE;
	g_autoptr(GHashTable) out_variables_index = NULL; // Maintains indices for the fields (or columns) in the query so that their respective values can be fetched from the resultant vector using the indices.
	g_autoptr(GHashTable) variables_type = NULL; // Maintains datatypes of the fields (or columns) that are involved in the query.
	g_autoptr(GString) sql = g_string_new("SELECT "); // Maintains query string.
	g_autoptr(GString) sql_condition_part = g_string_new(NULL); // Maintains condition part of the query string. (e.g. A = ? AND B < ? OR C > ? ,...)
	g_autoptr(GArray) arr_types_in = NULL; // Maintains in-params for MYSQL.
	g_autoptr(GArray) arr_types_out = NULL; // Maintains out-params for MYSQL.

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	out_variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

	// contains joins?
	if (bson_has_field(selector, "t"))
	{
		g_autoptr(GString) sql_join_part = g_string_new(NULL); // Maintains join part of the query string.
		g_autoptr(GString) sql_selection_part = g_string_new(NULL); // Maintains selection part of the query string. (e.g. SELECT A,B,C,... FROM X,Y,Z,...)

		variables_type = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

		// Formulate the selection part of the query.
		if (!build_query_selection_part(selector, backend_data, sql_selection_part, batch, out_variables_index, variables_type, arr_types_out, error))
		{
			goto _error;
		}

		// Extend the query string.
		g_string_append(sql, sql_selection_part->str);

		// Formulate the join part of the query.
		if (!build_query_join_part(selector, batch, sql_join_part, error))
		{
			goto _error;
		}

		// Extend the query string.
		if (sql_join_part->len > 0)
		{
			g_string_append(sql, " WHERE ");
			g_string_append(sql, sql_join_part->str);
			where_appended = TRUE;
		}
	}
	else
	{
		g_autoptr(GHashTable) schema = NULL;
		GHashTableIter schema_iter;
		JDBType type;
		const gchar* string_tmp;
		gpointer type_tmp, key_tmp;
		gboolean first_field = TRUE;

		if (!(schema = get_schema(backend_data, batch->namespace, name, error)))
		{
			goto _error;
		}

		variables_type = g_hash_table_ref(schema);

		g_hash_table_iter_init(&schema_iter, schema);

		while (g_hash_table_iter_next(&schema_iter, &key_tmp, &type_tmp))
		{
			type = GPOINTER_TO_INT(type_tmp);
			string_tmp = key_tmp;

			if (!first_field)
			{
				g_string_append(sql, ", ");
			}

			g_string_append(sql, string_tmp);

			first_field = FALSE;

			if (!g_hash_table_insert(out_variables_index, g_strdup(string_tmp), GINT_TO_POINTER(g_hash_table_size(out_variables_index))))
			{
				goto _error;
			}

			g_array_append_val(arr_types_out, type);
		}

		g_string_append_printf(sql, " FROM %s%s_%s%s", specs->sql.quote, batch->namespace, name, specs->sql.quote);
	}

	// Formulate the condition part of the query.
	if (G_UNLIKELY(!j_bson_iter_init(&iter, selector, error)))
	{
		goto _error;
	}

	if (!j_bson_iter_find(&iter, "s", error))
	{
		goto _error;
	}

	if (!j_bson_iter_recurse_document(&iter, &iter_selection, error))
	{
		goto _error;
	}

	// Fetch the operator that would be appended in between parent and child selector.
	if (G_UNLIKELY(!j_bson_iter_find(&iter_selection, "m", error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_iter_value(&iter_selection, J_DB_TYPE_UINT32, &value, error)))
	{
		goto _error;
	}

	mode_child = value.val_uint32;

	if (G_UNLIKELY(!j_bson_iter_recurse_document(&iter, &iter_selection, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!build_query_condition_part(backend_data, batch, &iter_selection, sql_condition_part, mode_child, arr_types_in, variables_type, error)))
	{
		goto _error;
	}

	// Extend the query string.
	if (sql_condition_part->len > 0)
	{
		if (where_appended)
		{
			g_string_append(sql, " AND ");
		}
		else
		{
			g_string_append(sql, " WHERE ");
		}

		g_string_append(sql, sql_condition_part->str);
	}

	statement = g_hash_table_lookup(thread_variables->query_cache, sql->str);

	if (G_UNLIKELY(!statement))
	{
		if (!(statement = j_sql_statement_new(sql->str, arr_types_in, arr_types_out, NULL, out_variables_index, variables_type, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(sql->str), statement))
		{
			j_sql_statement_free(statement);
			goto _error;
		}
	}

	if (sql_condition_part->len > 0)
	{
		if (!j_bson_iter_init(&iter, selector, error))
		{
			goto _error;
		}

		if (!j_bson_iter_find(&iter, "s", error))
		{
			goto _error;
		}

		if (!j_bson_iter_recurse_document(&iter, &iter_selection, error))
		{
			goto _error;
		}

		if (G_UNLIKELY(!bind_selector_query(backend_data, batch->namespace, &iter_selection, statement, statement->variable_types, error)))
		{
			goto _error;
		}
	}

	*iterator = statement;

	return TRUE;

_error:
	return FALSE;
}

gboolean
sql_generic_iterate(gpointer backend_data, gpointer _iterator, bson_t* query_result, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean sql_found;
	JSqlStatement* bound_statement = _iterator;
	JThreadVariables* thread_variables = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
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
			JDBTypeValue value;
			JDBType type = GPOINTER_TO_INT(g_hash_table_lookup(bound_statement->variable_types, (gchar*)var_name));
			g_autoptr(GString) var_name_no_quotes = g_string_new((gchar*)var_name);

			if (G_UNLIKELY(!specs->func.statement_column(thread_variables->db_connection, bound_statement->stmt, GPOINTER_TO_INT(position), type, &value, error)))
			{
				goto _error;
			}

			/// \todo Backend specific quotes need to be removed. There should be a better solution.
			g_string_replace(var_name_no_quotes, specs->sql.quote, "", 0);

			if (G_UNLIKELY(!j_bson_append_value(query_result, var_name_no_quotes->str, type, &value, error)))
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
	if (G_UNLIKELY(!specs->func.statement_reset(thread_variables->db_connection, bound_statement->stmt, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}
