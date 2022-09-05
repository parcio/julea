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

	JDBTypeValue value1;
	JDBTypeValue value2;
	gboolean found = FALSE;
	gboolean bson_initialized = FALSE;
	JSqlBatch* batch = _batch;
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* metadata_query = NULL;
	const gchar* metadata_query_sql = "SELECT varname, vartype FROM schema_structure WHERE namespace=? AND name=?";

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

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
get_schema(gpointer backend_data, gpointer _batch, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;
	JThreadVariables* thread_variables = NULL;
	GHashTable* schema_map = NULL;
	g_autoptr(GString) cache_key = g_string_new(NULL);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	g_string_append_printf(cache_key, "%s_%s", batch->namespace, name);
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

	gboolean first = TRUE;

	g_string_append(sql, "( ");

	while (TRUE)
	{
		JDBTypeValue value;
		gboolean has_next;
		bson_iter_t iterchild;

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
			JDBSelectorMode mode_child;

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
			JDBType type;
			JDBSelectorOperator op;
			const char* string_tmp;

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
			JDBType type;
			const gchar* string_tmp;

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

static gboolean
bind_selector_query_ex(gpointer backend_data, bson_iter_t* iter, JSqlStatement* statement, guint* variables_count, GHashTable* schema, GError** error)
{
	/* Note: This method is almost the replica of method named 'build_selector_query_ex'. There we iterate through the BSON data to prepare query string,
	 * whereas in this document we re-iterate through the BSON to fetch merely the data types for the fields that are used in the query string.
	 * More suitable is to merge both the methods.
	 */
	J_TRACE_FUNCTION(NULL);

	gboolean hasItems;
	gboolean keyExists;
	JDBType itemDatatype;
	JDBTypeValue itemValue;

	JDBTypeValue tableName;
	GString* fieldName = NULL;

	bson_iter_t iterChildDocument;
	JThreadVariables* thread_variables = NULL;

	gboolean retCode = FALSE;

	// Fetch thread's local cache to get SQL's backend object.
	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	while (TRUE)
	{
		// Check for the next item in the document.
		if (G_UNLIKELY(!j_bson_iter_next(iter, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems)
		{
			break;
		}

		/* Check for the name of the first item. Skip processing if it contains tables' names. 
		 * Here the code expects a document that contains data for an individual field that has to be included in the query.
		 */
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "tables", &keyExists, error)))
		{
			goto _error;
		}

		if (keyExists)
		{
			break;
		}

		/* Check for the name of the first item. Skip processing if it contains join related data. 
		 * Here the code expects a document that contains data for an individual field that has to be included in the query.
		 */
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "join", &keyExists, error)))
		{
			goto _error;
		}

		if (keyExists)
		{
			continue;
		}

		// Fetch the mode (or operator) that has to be appended in between the current and the next (child) BSON item/document.
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "_mode", &keyExists, error)))
		{
			goto _error;
		}

		if (keyExists)
		{
			continue;
		}

		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
		{
			goto _error;
		}

		if (j_bson_iter_find(&iterChildDocument, "_mode", NULL))
		{
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!bind_selector_query_ex(backend_data, &iterChildDocument, statement, variables_count, schema, error)))
			{
				goto _error;
			}
		}
		else
		{
			(*variables_count)++;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_table", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &tableName, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_name", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &itemValue, error)))
			{
				goto _error;
			}

			fieldName = g_string_new(NULL);
			g_string_append_printf(fieldName, "%s.%s", tableName.val_string, itemValue.val_string);

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_value", error)))
			{
				goto _error;
			}

			// TODO
			itemDatatype = GPOINTER_TO_UINT(g_hash_table_lookup(schema, (gpointer)fieldName->str));

			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, itemDatatype, &itemValue, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, statement->stmt, *variables_count, itemDatatype, &itemValue, error)))
			{
				goto _error;
			}

			g_string_free(fieldName, TRUE);
			fieldName = NULL;
		}
	}

	retCode = TRUE;

	goto _exit;

_error: // handle unexpected behaviour.
	retCode = FALSE;

_exit: // exit the function by deallocating the memory.
	if (fieldName)
	{
		g_string_free(fieldName, TRUE);
		fieldName = NULL;
	}

	return retCode;
}

static gboolean
build_selector_query_ex(bson_iter_t* iter, GString* sql, JDBSelectorMode mode, guint* variables_count, GArray* arr_types_in, GHashTable* variables_type, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean hasItems;
	gboolean keyExists;
	JDBType itemDataType;
	JDBTypeValue itemValue;
	JDBTypeValue tableName;
	JDBSelectorMode modeChild;
	JDBSelectorOperator op;
	bson_iter_t iterChildDocument;

	GString* key = NULL;
	GString* sqlChildDocument = NULL;
	GString* sqlCurrentDocument = g_string_new(NULL);

	gboolean retCode = FALSE;

	while (TRUE)
	{
		// Check for the next item in the document.
		if (G_UNLIKELY(!j_bson_iter_next(iter, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems) // TODO: Above fucntion should return an ERROR CODE and this check should be done on that. Also this approach should be used at other places as well.
		{
			break;
		}

		/* Check for the name of the first item. Skip processing if it contains tables' names.
		 * Here the code expects a document that contains data for an individual field that has to be included in the query.
		 */
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "tables", &keyExists, error)))
		{
			goto _error;
		}

		if (keyExists)
		{
			// Skip and continue with the next iterator.
			continue;
		}

		/* Check for the name of the first item. Skip processing if it contains join related data. 
		 * Here the code expects a document that contains data for an individual field that has to be included in the query.
		 */
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "join", &keyExists, error)))
		{
			goto _error;
		}

		if (keyExists)
		{
			// Skip and continue with the next iterator.
			continue;
		}

		// Fetch the mode (or operator) that has to be appended in between the current and the next (child) BSON item/document.
		if (G_UNLIKELY(!j_bson_iter_key_equals(iter, "_mode", &keyExists, error)))
		{
			goto _error;
		}

		if (keyExists)
		{
			continue;
		}

		/* As per the BSON formation followed in jdb-selector.c, the above key "_mode" is either followed by a set of 
		 * fields/columns (that are appended as a BSON document to current document) or a child BSON document that contains 
		 * information for a child Selector that again starts with "_mode". Therefore the following code tries to fetch 
		 * the first iterator of the next available document.
		 */
		if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
		{
			// Terminate as code could not find BSON document.
			goto _error;
		}

		// If the child iterator starts with "_mode" it indicates the commencement of a new BSON document (or a child Selector)
		if (j_bson_iter_find(&iterChildDocument, "_mode", NULL))
		{
			/* Extract value for "_mode" that would be appended to the fileds of this child BSON document.
			 * It also would be used to append the grand child BSON document attached to this child document (if any).
			 */
			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_UINT32, &itemValue, error)))
			{
				goto _error;
			}

			modeChild = itemValue.val_uint32;

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			sqlChildDocument = g_string_new(NULL);

			// Repeat the same routine for this child BSON document (or selector).
			if (G_UNLIKELY(!build_selector_query_ex(&iterChildDocument, sqlChildDocument, modeChild, variables_count, arr_types_in, variables_type, error)))
			{
				goto _error;
			}

			if (sqlChildDocument->len > 0)
			{
				if (sqlCurrentDocument->len > 0)
				{
					/* The operator in between the selectors (or BSON documents) is the one that is defined for the preceding selector (or BSON document).
					 * If both the query strings, for current and child documents, are not empty then the operator defined for the precedded document
					 * would be appended as a connector.
					 * E.g. "<string for 1st selector/BSON doc>" <operator-linked-to-1st-BSON-doc> "<string for 2nd selector/BSON doc>" <operator-linked-to-2nd-BSON-doc> ...
					 */
					switch (mode)
					{
						case J_DB_SELECTOR_MODE_AND:
							g_string_append(sqlCurrentDocument, " AND ");
							break;
						case J_DB_SELECTOR_MODE_OR:
							g_string_append(sqlCurrentDocument, " OR ");
							break;
						default:
							g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "operator invalid");
							goto _error;
					}
				}

				g_string_append_printf(sqlCurrentDocument, " ( %s )", sqlChildDocument->str);
			}

			g_string_free(sqlChildDocument, TRUE);
			sqlChildDocument = NULL;
		}
		else // The following code snippet (whole "else" block) iterates through the BSON document items and appended them to sql query string.
		{
			// Use the operator attached to the BSON document (or selector) to append the query string of the individual fields.
			if (sqlCurrentDocument->len > 0)
			{
				switch (mode)
				{
					case J_DB_SELECTOR_MODE_AND:
						g_string_append(sqlCurrentDocument, " AND ");
						break;
					case J_DB_SELECTOR_MODE_OR:
						g_string_append(sqlCurrentDocument, " OR ");
						break;
					default:
						g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_OPERATOR_INVALID, "operator invalid");
						goto _error;
				}
			}

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			// Fetch table name.
			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_table", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &tableName, error)))
			{
				goto _error;
			}

			// Fetch field or column name.
			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_name", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &itemValue, error)))
			{
				goto _error;
			}

			// Append table name to field name. E.g. "<string for table name>.<string for field name>"
			g_string_append_printf(sqlCurrentDocument, "%s%s.%s%s", specs->sql.quote, tableName.val_string, itemValue.val_string, specs->sql.quote);

			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_value", error)))
			{
				goto _error;
			}

			// Key is fully resolved field name. E.g. "<string for table name>.<string for field name>"
			key = g_string_new(NULL);
			g_string_append_printf(key, "%s.%s", tableName.val_string, itemValue.val_string);

			// "variables_type" is a Hashmap. It maintains the data types against each field that is latered used in the process of fetching the records.
			itemDataType = GPOINTER_TO_UINT(g_hash_table_lookup(variables_type, key->str));
			// "arr_types_in" is an Array. It maintains structures to pass input params this is equired by MYSQL.
			// https://mariadb.com/kb/en/mysql_stmt_bind_param/
			g_array_append_val(arr_types_in, itemDataType);

			g_string_free(key, TRUE);
			key = NULL;

			// Fetch operator value that is used as a filter for the current field/column.
			if (G_UNLIKELY(!j_bson_iter_recurse_document(iter, &iterChildDocument, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_find(&iterChildDocument, "_operator", error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_UINT32, &itemValue, error)))
			{
				goto _error;
			}

			op = itemValue.val_uint32;

			switch (op)
			{
				case J_DB_SELECTOR_OPERATOR_LT:
					g_string_append(sqlCurrentDocument, "<");
					break;
				case J_DB_SELECTOR_OPERATOR_LE:
					g_string_append(sqlCurrentDocument, "<=");
					break;
				case J_DB_SELECTOR_OPERATOR_GT:
					g_string_append(sqlCurrentDocument, ">");
					break;
				case J_DB_SELECTOR_OPERATOR_GE:
					g_string_append(sqlCurrentDocument, ">=");
					break;
				case J_DB_SELECTOR_OPERATOR_EQ:
					g_string_append(sqlCurrentDocument, "=");
					break;
				case J_DB_SELECTOR_OPERATOR_NE:
					g_string_append(sqlCurrentDocument, "!=");
					break;
				default:
					g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_COMPARATOR_INVALID, "comparator invalid");
					goto _error;
			}

			(*variables_count)++;

			g_string_append_printf(sqlCurrentDocument, " ?");
		}
	}

	if (sqlCurrentDocument->len > 0)
	{
		g_string_append_printf(sql, " ( %s )", sqlCurrentDocument->str);
	}

	retCode = TRUE;

	goto _exit;

_error: // handle unexpected behaviour.
	retCode = FALSE;

_exit: // exit the function by deallocating the memory.
	if (key)
	{
		g_string_free(key, TRUE);
	}

	if (sqlChildDocument)
	{
		g_string_free(sqlChildDocument, TRUE);
	}

	return retCode;
}

static gboolean
build_query_selection_part(bson_t const* selector, gpointer backend_data, GString* sql, JSqlBatch* batch, GHashTable* variables_index, GHashTable* variables_type, guint* variables_count, GArray* arr_types_out, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean hasItems;
	JDBType itemDataType;
	JDBTypeValue itemValue;

	char* szFieldname;
	gpointer ptrFieldType;

	bson_iter_t iterChildDocument;
	bson_iter_t iterCurrentDocument;

	GString* tableName = NULL;
	GString* fieldName = NULL;
	GString* sqlTablesName = g_string_new(NULL);

	GHashTable* schemaCache = NULL;
	GHashTableIter schemaCacheIter;

	gboolean retCode = FALSE;

	// Initialize iterator for BSON document.
	if (G_UNLIKELY(!j_bson_iter_init(&iterCurrentDocument, selector, error)))
	{
		goto _error;
	}

	// Look for the key that is assigned to BSON array that contains tables' information.
	if (G_UNLIKELY(!j_bson_iter_find(&iterCurrentDocument, "tables", error)))
	{
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
		if (G_UNLIKELY(!j_bson_iter_next(&iterChildDocument, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems)
		{
			// Return as no more elements left.
			break;
		}

		// Extract namespace for the table from BSON element.
		if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &itemValue, error)))
		{
			goto _error;
		}

		tableName = g_string_new(NULL);
		g_string_append(tableName, itemValue.val_string); // Append namespace for the table.

		// The next element should contain the table name therefore the following snippet ensures that the next element should be present.
		if (G_UNLIKELY(!j_bson_iter_next(&iterChildDocument, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems)
		{
			break;
		}

		// Extract name for the table from BSON element.
		if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &itemValue, error)))
		{
			goto _error;
		}

		// Append name of the table.
		g_string_append(tableName, "_");
		g_string_append(tableName, itemValue.val_string);

		// Fetching table's schema to extract all the columns' names.
		if (!(schemaCache = get_schema(backend_data, batch, itemValue.val_string, error)))
		{
			goto _error;
		}

		if (sql->len > 0)
		{
			g_string_append(sql, ", ");
		}

		g_hash_table_iter_init(&schemaCacheIter, schemaCache); // Initializing the iterator.

		/* The following code snippet prepares data for "_id" field or column.
		 * It also appends to the required data structures for later usage.
		 */
		fieldName = g_string_new(NULL);
		g_string_append_printf(fieldName, "%s._id", tableName->str); // Prepare field name.

		g_string_append_printf(sql, "%s._id", tableName->str); // Append field name to query string.

		itemDataType = J_DB_TYPE_UINT32;

		// Maintains hash-table for column name and id (an auto-increment number).
		g_hash_table_insert(variables_index, GINT_TO_POINTER(*variables_count), g_strdup(fieldName->str));

		// "variables_type" is a Hashmap. It maintains the data types against each field that is latered used in the process of fetching the records.
		g_hash_table_insert(variables_type, g_strdup(fieldName->str), GINT_TO_POINTER(itemDataType));

		// "arr_types_in" is an Array. It maintains structures to pass input params this is equired by MYSQL.
		// https://mariadb.com/kb/en/mysql_stmt_bind_param/
		g_array_append_val(arr_types_out, itemDataType);

		(*variables_count)++;

		g_string_free(fieldName, TRUE);
		fieldName = NULL;

		/* The following code snippet prepares data for the remaining fields or columns.
		 * It also appends to the required data structures for later usage.
		 */
		while (g_hash_table_iter_next(&schemaCacheIter, (gpointer*)&szFieldname, &ptrFieldType))
		{
			itemDataType = GPOINTER_TO_INT(ptrFieldType);

			if (strcmp(szFieldname, "_id") == 0)
			{
				// Proceed with the next column as it has already been added.
				continue;
			}

			fieldName = g_string_new(NULL);
			g_string_append_printf(fieldName, "%s.%s", tableName->str, szFieldname);

			g_string_append_printf(sql, ", %s%s%s", specs->sql.quote, fieldName->str, specs->sql.quote);
			g_hash_table_insert(variables_index, GINT_TO_POINTER(*variables_count), g_strdup(fieldName->str));
			g_hash_table_insert(variables_type, g_strdup(fieldName->str), GINT_TO_POINTER(itemDataType));
			g_array_append_val(arr_types_out, itemDataType);
			(*variables_count)++;

			g_string_free(fieldName, TRUE);
			fieldName = NULL;
		}

		if (sqlTablesName->len > 0)
		{
			g_string_append(sqlTablesName, ", ");
		}

		g_string_append_printf(sqlTablesName, "%s%s%s", specs->sql.quote, tableName->str, specs->sql.quote);

		g_string_free(tableName, TRUE);
		tableName = NULL;
	}

	g_string_append(sql, " FROM ");
	g_string_append(sql, sqlTablesName->str);

	retCode = TRUE;

	goto _exit;

_error: // handle unexpected behaviour.
	retCode = FALSE;

_exit: // exit the function by deallocating the memory.
	if (sqlTablesName)
	{
		g_string_free(sqlTablesName, TRUE);
		sqlTablesName = NULL;
	}

	if (tableName)
	{
		g_string_free(tableName, TRUE);
		tableName = NULL;
	}

	if (fieldName)
	{
		g_string_free(fieldName, TRUE);
		fieldName = NULL;
	}

	return retCode;
}

static gboolean
build_query_join_part(bson_t const* selector, GString* sql, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	gboolean hasItems;
	gboolean keyExists;
	JDBTypeValue itemValue;

	bson_iter_t iterChildDocument;
	bson_iter_t iterCurrentDocument;

	GString* sqlJoin = NULL;

	gboolean retCode = FALSE;

	// Initialize the iterator.
	if (G_UNLIKELY(!j_bson_iter_init(&iterCurrentDocument, selector, error)))
	{
		goto _error;
	}

	// The following code iterates through the BSON elements and prepares query part for the join operation.
	while (TRUE)
	{
		if (G_UNLIKELY(!j_bson_iter_next(&iterCurrentDocument, &hasItems, error)))
		{
			goto _error;
		}

		if (!hasItems)
		{
			break;
		}

		// Look for the key that is assigned to BSON document that contains the join information.
		if (G_UNLIKELY(!j_bson_iter_key_equals(&iterCurrentDocument, "join", &keyExists, error)))
		{
			goto _error;
		}

		if (!keyExists)
		{
			// Return as the reterived element has a different key.
			continue;
		}

		// Try to intialize child element.
		if (G_UNLIKELY(!j_bson_iter_recurse_document(&iterCurrentDocument, &iterChildDocument, error)))
		{
			goto _error;
		}

		sqlJoin = g_string_new(NULL);

		// Iterate through the elements and append values to the query string.
		while (TRUE)
		{
			if (G_UNLIKELY(!j_bson_iter_next(&iterChildDocument, &hasItems, error)))
			{
				goto _error;
			}

			if (!hasItems)
			{
				break;
			}

			// Extract the value of the current BSON element.
			if (G_UNLIKELY(!j_bson_iter_value(&iterChildDocument, J_DB_TYPE_STRING, &itemValue, error)))
			{
				goto _error;
			}

			if (sqlJoin->len > 0)
			{
				g_string_append(sqlJoin, "=");
			}

			// Append the name of the field to the query string.
			g_string_append_printf(sqlJoin, "%s", itemValue.val_string);
		}

		if (sql->len > 0)
		{
			g_string_append(sql, " AND ");
		}

		g_string_append_printf(sql, "%s", sqlJoin->str);

		g_string_free(sqlJoin, TRUE);
		sqlJoin = NULL;
	}

	retCode = TRUE;

	goto _exit;

_error: // handle unexpected behaviour.
	retCode = FALSE;

_exit: // exit the function by deallocating the memory.
	if (sqlJoin)
	{
		g_string_free(sqlJoin, TRUE);
		sqlJoin = NULL;
	}

	return retCode;
}

// this function is used for update and delete and returns the IDs of entries that match the selector
gboolean
_backend_query_ids(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GArray** matches, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue value;
	guint count = 0;
	bson_iter_t iter;
	JSqlBatch* batch = _batch;
	GHashTable* schema = NULL;
	JSqlStatement* id_query = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GString) id_sql = g_string_new(NULL);
	g_autoptr(GArray) arr_types_in = NULL;
	GArray* ids_out = NULL;

	if (!(schema = get_schema(backend_data, batch, name, error)))
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

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		JDBSelectorMode mode_child;

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

		if (G_UNLIKELY(!build_selector_query(backend_data, &iter, id_sql, mode_child, arr_types_in, schema, error)))
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

		if (G_UNLIKELY(!bind_selector_query(backend_data, &iter, id_query, schema, error)))
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

	bson_iter_t iter;
	GHashTableIter schema_iter;
	JDBType type;
	gpointer type_tmp;
	char* string_tmp;
	JSqlBatch* batch = _batch;
	GHashTable* schema = NULL;
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* query_statement = NULL;
	g_autoptr(GString) query_sql = g_string_new(NULL);
	g_autoptr(GHashTable) out_variables_index = NULL;
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
	g_string_append(query_sql, "SELECT ");

	if (!(schema = get_schema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	g_hash_table_iter_init(&schema_iter, schema);

	g_string_append(query_sql, "_id");

	g_hash_table_insert(out_variables_index, g_strdup("_id"), GINT_TO_POINTER(g_hash_table_size(out_variables_index)));
	type = BACKEND_ID_TYPE;
	g_array_append_val(arr_types_out, type);

	while (g_hash_table_iter_next(&schema_iter, (gpointer*)&string_tmp, &type_tmp))
	{
		type = GPOINTER_TO_INT(type_tmp);

		if (strcmp(string_tmp, "_id") == 0)
			continue;

		g_string_append_printf(query_sql, ", %s%s%s", specs->sql.quote, string_tmp, specs->sql.quote);

		g_hash_table_insert(out_variables_index, g_strdup(string_tmp), GINT_TO_POINTER(g_hash_table_size(out_variables_index)));
		g_array_append_val(arr_types_out, type);
	}

	g_string_append_printf(query_sql, " FROM %s%s_%s%s", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (selector && j_bson_has_enough_keys(selector, 2, NULL))
	{
		JDBTypeValue value;
		JDBSelectorMode mode_child;

		g_string_append(query_sql, " WHERE ");

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

		if (G_UNLIKELY(!build_selector_query(backend_data, &iter, query_sql, mode_child, arr_types_in, schema, error)))
		{
			goto _error;
		}
	}

	/// \todo check if caching this statement makes any difference with different databases
	query_statement = g_hash_table_lookup(thread_variables->query_cache, query_sql->str);

	if (G_UNLIKELY(!query_statement))
	{
		if (!(query_statement = j_sql_statement_new(query_sql->str, arr_types_in, arr_types_out, NULL, &out_variables_index)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(query_sql->str), query_statement))
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
			JDBTypeValue value;
			JDBType type = GPOINTER_TO_INT(g_hash_table_lookup(schema, (gchar*)var_name));

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
