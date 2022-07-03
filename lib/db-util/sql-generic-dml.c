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
sql_generic_insert(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* metadata, bson_t* id, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue value;
	bson_iter_t iter;
	JDBType type;
	gboolean found;
	guint value_count = 0;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GHashTable) schema = NULL;
	JSqlBatch* batch = _batch;
	JSqlStatement* id_query = NULL;
	JSqlStatement* insert_query = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) id_arr_types_out = NULL;
	g_autoptr(GString) insert_sql = g_string_new(NULL);

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

	// used to query the ID of a row after its insertion
	id_query = g_hash_table_lookup(thread_variables->query_cache, specs->sql.select_last);

	if (!id_query)
	{
		type = BACKEND_ID_TYPE;
		g_array_append_val(id_arr_types_out, type);

		if (!(id_query = j_sql_statement_new(specs->sql.select_last, NULL, id_arr_types_out, NULL, NULL, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(specs->sql.select_last), id_query))
		{
			// in all other error cases id_query is already owned by the hash table
			j_sql_statement_free(id_query);
			goto _error;
		}
	}

	if (!(schema = get_schema(backend_data, batch->namespace, name, error)))
	{
		goto _error;
	}

	g_string_append_printf(insert_sql, "INSERT INTO %s%s_%s%s (", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (!j_bson_iter_init(&iter, metadata, error))
	{
		goto _error;
	}

	while (TRUE)
	{
		gboolean next;
		const gchar* field = NULL;
		g_autoptr(GString) full_name = NULL;

		if (!j_bson_iter_next(&iter, &next, error))
		{
			goto _error;
		}

		if (!next)
		{
			break;
		}

		field = j_bson_iter_key(&iter, error);

		full_name = j_sql_get_full_field_name(batch->namespace, name, field);
		type = GPOINTER_TO_INT(g_hash_table_lookup(schema, full_name->str));

		if (value_count)
		{
			g_string_append(insert_sql, ", ");
		}

		g_string_append_printf(insert_sql, "%s%s%s", specs->sql.quote, field, specs->sql.quote);
		g_array_append_val(arr_types_in, type);

		value_count++;
	}

	g_string_append(insert_sql, ") VALUES (");

	if (value_count)
	{
		g_string_append_printf(insert_sql, " ?");
	}

	for (guint i = 1; i < value_count; i++)
	{
		g_string_append_printf(insert_sql, ", ?");
	}

	g_string_append(insert_sql, " )");

	insert_query = g_hash_table_lookup(thread_variables->query_cache, insert_sql->str);

	if (!insert_query)
	{
		if (!(insert_query = j_sql_statement_new(insert_sql->str, arr_types_in, NULL, NULL, NULL, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(insert_sql->str), insert_query))
		{
			// in all other error cases insert_query is already owned by the hash table
			j_sql_statement_free(insert_query);
			goto _error;
		}
	}

	// bind values to the query

	if (G_UNLIKELY(!j_bson_iter_init(&iter, metadata, error)))
	{
		goto _error;
	}

	for (guint i = 0; i < value_count; i++)
	{
		if (G_UNLIKELY(!specs->func.statement_bind_null(thread_variables->db_connection, insert_query->stmt, i + 1, error)))
		{
			goto _error;
		}
	}

	value_count = 0;

	while (TRUE)
	{
		gboolean has_next;

		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		type = g_array_index(arr_types_in, JDBType, value_count);

		// increment first s.t. value_count is the position (starting at 1 for in-variables)
		value_count++;

		if (G_UNLIKELY(!j_bson_iter_value(&iter, type, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, insert_query->stmt, value_count, type, &value, error)))
		{
			goto _error;
		}
	}

	if (G_UNLIKELY(!value_count))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_NO_VARIABLE_SET, "no variable set");

		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, insert_query->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_step(thread_variables->db_connection, id_query->stmt, &found, error)))
	{
		goto _error;
	}

	if (!found)
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_column(thread_variables->db_connection, id_query->stmt, 0, BACKEND_ID_TYPE, &value, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_reset(thread_variables->db_connection, id_query->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_append_value(id, "_value", BACKEND_ID_TYPE, &value, error)))
	{
		goto _error;
	}

	value.val_uint32 = BACKEND_ID_TYPE;

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

gboolean
sql_generic_update(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, bson_t const* entry_updates, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBType type;
	bson_iter_t iter;
	guint value_count = 0;
	guint id_pos = 0;
	gboolean has_next;
	JSqlBatch* batch = _batch;
	g_autoptr(GHashTable) schema = NULL;
	const char* string_tmp;
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* update_statement = NULL;
	g_autoptr(GString) update_sql = g_string_new(NULL);
	g_autoptr(GArray) matches = NULL;
	g_autoptr(GArray) arr_types_in = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(entry_updates != NULL, FALSE);
	g_return_val_if_fail(selector != NULL, FALSE);

	if (!(schema = get_schema(backend_data, batch->namespace, name, error)))
	{
		goto _error;
	}

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	g_string_append_printf(update_sql, "UPDATE %s%s_%s%s SET ", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (G_UNLIKELY(!j_bson_iter_init(&iter, entry_updates, error)))
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

		if (value_count)
		{
			g_string_append(update_sql, ", ");
		}

		string_tmp = j_bson_iter_key(&iter, error);

		if (G_UNLIKELY(!string_tmp))
		{
			goto _error;
		}

		full_name = j_sql_get_full_field_name(batch->namespace, name, string_tmp);

		type = GPOINTER_TO_INT(g_hash_table_lookup(schema, full_name->str));
		g_array_append_val(arr_types_in, type);
		g_string_append_printf(update_sql, "%s%s%s = ?", specs->sql.quote, string_tmp, specs->sql.quote);

		value_count++;
	}

	type = BACKEND_ID_TYPE;
	g_array_append_val(arr_types_in, type);
	g_string_append_printf(update_sql, " WHERE _id = ?");

	id_pos = value_count + 1;

	update_statement = g_hash_table_lookup(thread_variables->query_cache, update_sql->str);

	if (G_UNLIKELY(!update_statement))
	{
		if (!(update_statement = j_sql_statement_new(update_sql->str, arr_types_in, NULL, NULL, NULL, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(update_sql->str), update_statement))
		{
			// in all other error cases update_statement is already owned by the hash table
			j_sql_statement_free(update_statement);
			goto _error;
		}
	}

	if (G_UNLIKELY(!_backend_query_ids(backend_data, batch, name, selector, &matches, error)))
	{
		goto _error;
	}

	// bind all values except _id
	// bound values remain in SQLite and MySQL even if the statement gets resetted
	value_count = 0;

	if (G_UNLIKELY(!j_bson_iter_init(&iter, entry_updates, error)))
	{
		goto _error;
	}

	while (TRUE)
	{
		JDBTypeValue value;

		if (G_UNLIKELY(!j_bson_iter_next(&iter, &has_next, error)))
		{
			goto _error;
		}

		if (!has_next)
		{
			break;
		}

		type = g_array_index(arr_types_in, JDBType, value_count);

		if (G_UNLIKELY(!j_bson_iter_value(&iter, type, &value, error)))
		{
			goto _error;
		}

		value_count++;

		if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, update_statement->stmt, value_count, type, &value, error)))
		{
			goto _error;
		}
	}

	// bind id for each match
	for (guint j = 0; j < matches->len; j++)
	{
		JDBTypeValue value;

		value.val_uint64 = g_array_index(matches, guint64, j);

		if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, update_statement->stmt, id_pos, BACKEND_ID_TYPE, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, update_statement->stmt, error)))
		{
			goto _error;
		}
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

gboolean
sql_generic_delete(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;
	JSqlStatement* delete_statement = NULL;
	JThreadVariables* thread_variables = NULL;
	g_autoptr(GArray) matches = NULL;
	g_autoptr(GString) delete_sql = g_string_new(NULL);

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (G_UNLIKELY(!_backend_query_ids(backend_data, batch, name, selector, &matches, error)))
	{
		goto _error;
	}

	g_string_append_printf(delete_sql, "DELETE FROM %s%s_%s%s WHERE _id = ?", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	delete_statement = g_hash_table_lookup(thread_variables->query_cache, delete_sql->str);

	if (G_UNLIKELY(!delete_statement))
	{
		g_autoptr(GArray) arr_types_in = NULL;
		JDBType type = BACKEND_ID_TYPE;

		arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
		g_array_append_val(arr_types_in, type);

		if (!(delete_statement = j_sql_statement_new(delete_sql->str, arr_types_in, NULL, NULL, NULL, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(delete_sql->str), delete_statement))
		{
			// in all other error cases delete_statement is already owned by the hash table
			j_sql_statement_free(delete_statement);
			goto _error;
		}
	}

	for (guint j = 0; j < matches->len; j++)
	{
		JDBTypeValue value;

		value.val_uint64 = g_array_index(matches, guint64, j);

		if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, delete_statement->stmt, 1, BACKEND_ID_TYPE, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, delete_statement->stmt, error)))
		{
			goto _error;
		}
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
