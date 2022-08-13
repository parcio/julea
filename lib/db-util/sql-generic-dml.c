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

	guint count = 0;
	JDBTypeValue value;
	bson_iter_t iter;
	JDBType type;
	gboolean found;
	JThreadVariables* thread_variables = NULL;
	GHashTable* schema = NULL;
	JSqlBatch* batch = _batch;
	JSqlStatement* id_query = NULL;
	JSqlStatement* insert_query = NULL;
	g_autoptr(GString) insert_cache_key = NULL;
	g_autoptr(GArray) arr_types_in = NULL;
	g_autoptr(GArray) id_arr_types_out = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(metadata != NULL, FALSE);

	arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
	id_arr_types_out = g_array_new(FALSE, FALSE, sizeof(JDBType));
	insert_cache_key = g_string_new(NULL);

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

		if (!(id_query = j_sql_statement_new(specs->sql.select_last, NULL, id_arr_types_out, NULL, NULL)))
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

	if (!(schema = get_schema(backend_data, batch, name, error)))
	{
		goto _error;
	}

	// note that a keyword is used as cache key instead of a sql string
	// so the string does not need to be constructed if the statement is already cached
	g_string_append_printf(insert_cache_key, "_insert_%s_%s", batch->namespace, name);

	insert_query = g_hash_table_lookup(thread_variables->query_cache, insert_cache_key->str);

	if (G_UNLIKELY(!insert_query))
	{
		gchar* key;
		gpointer type_tmp;
		GHashTableIter schema_iter;
		g_autoptr(GHashTable) in_variables_index = NULL;
		g_autoptr(GString) insert_sql = g_string_new(NULL);

		in_variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		g_string_append_printf(insert_sql, "INSERT INTO %s%s_%s%s (", specs->sql.quote, batch->namespace, name, specs->sql.quote);
		g_hash_table_iter_init(&schema_iter, schema);

		while (g_hash_table_iter_next(&schema_iter, (gpointer*)&key, &type_tmp))
		{
			type = GPOINTER_TO_INT(type_tmp);

			if (g_hash_table_size(in_variables_index))
			{
				g_string_append(insert_sql, ", ");
			}

			g_string_append_printf(insert_sql, "%s%s%s", specs->sql.quote, key, specs->sql.quote);
			g_array_append_val(arr_types_in, type);

			g_hash_table_insert(in_variables_index, g_strdup(key), GINT_TO_POINTER(g_hash_table_size(in_variables_index) + 1));
		}

		g_string_append(insert_sql, ") VALUES (");

		if (g_hash_table_size(in_variables_index))
		{
			g_string_append_printf(insert_sql, " ?");
		}

		for (guint i = 1; i < g_hash_table_size(in_variables_index); i++)
		{
			g_string_append_printf(insert_sql, ", ?");
		}

		g_string_append(insert_sql, " )");

		if (!(insert_query = j_sql_statement_new(insert_sql->str, arr_types_in, NULL, &in_variables_index, NULL)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(insert_cache_key->str), insert_query))
		{
			// in all other error cases insert_query is already owned by the hash table
			j_sql_statement_free(insert_query);
			goto _error;
		}
	}

	if (G_UNLIKELY(!j_bson_iter_init(&iter, metadata, error)))
	{
		goto _error;
	}

	for (guint i = 0; i < g_hash_table_size(insert_query->in_variables_index); i++)
	{
		if (G_UNLIKELY(!specs->func.statement_bind_null(thread_variables->db_connection, insert_query->stmt, i + 1, error)))
		{
			goto _error;
		}
	}

	while (TRUE)
	{
		guint index;
		gboolean has_next;
		const char* string_tmp;

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

		type = GPOINTER_TO_INT(g_hash_table_lookup(schema, string_tmp));
		index = GPOINTER_TO_INT(g_hash_table_lookup(insert_query->in_variables_index, string_tmp));

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

		if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, insert_query->stmt, index, type, &value, error)))
		{
			goto _error;
		}
	}

	if (G_UNLIKELY(!count))
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
	gboolean has_next;
	JSqlBatch* batch = _batch;
	GHashTable* schema = NULL;
	const char* string_tmp;
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* update_statement = NULL;
	g_autoptr(GString) update_sql = g_string_new(NULL);
	g_autoptr(GHashTable) in_variables_index = NULL;
	g_autoptr(GArray) matches = NULL;
	g_autoptr(GArray) arr_types_in = NULL;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(entry_updates != NULL, FALSE);
	g_return_val_if_fail(selector != NULL, FALSE);

	if (!(schema = get_schema(backend_data, batch, name, error)))
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

	in_variables_index = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	g_string_append_printf(update_sql, "UPDATE %s%s_%s%s SET ", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (G_UNLIKELY(!j_bson_iter_init(&iter, entry_updates, error)))
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

		if (g_hash_table_size(in_variables_index))
		{
			g_string_append(update_sql, ", ");
		}

		string_tmp = j_bson_iter_key(&iter, error);

		if (G_UNLIKELY(!string_tmp))
		{
			goto _error;
		}

		type = GPOINTER_TO_INT(g_hash_table_lookup(schema, string_tmp));
		g_array_append_val(arr_types_in, type);
		g_string_append_printf(update_sql, "%s%s%s = ?", specs->sql.quote, string_tmp, specs->sql.quote);

		g_hash_table_insert(in_variables_index, g_strdup(string_tmp), GINT_TO_POINTER(g_hash_table_size(in_variables_index) + 1));
	}

	type = BACKEND_ID_TYPE;
	g_array_append_val(arr_types_in, type);
	g_string_append_printf(update_sql, " WHERE _id = ?");

	g_hash_table_insert(in_variables_index, g_strdup("_id"), GINT_TO_POINTER(g_hash_table_size(in_variables_index) + 1));

	update_statement = g_hash_table_lookup(thread_variables->query_cache, update_sql->str);

	if (G_UNLIKELY(!update_statement))
	{
		if (!(update_statement = j_sql_statement_new(update_sql->str, arr_types_in, NULL, &in_variables_index, NULL)))
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

	for (guint j = 0; j < matches->len; j++)
	{
		JDBTypeValue value;
		guint count = 0;
		guint index = GPOINTER_TO_INT(g_hash_table_lookup(update_statement->in_variables_index, "_id"));

		if (G_UNLIKELY(!index))
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND, "variable not found");
			goto _error;
		}

		value.val_uint64 = g_array_index(matches, guint64, j);

		if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, update_statement->stmt, index, BACKEND_ID_TYPE, &value, error)))
		{
			goto _error;
		}

		if (G_UNLIKELY(!j_bson_iter_init(&iter, entry_updates, error)))
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

			type = GPOINTER_TO_INT(g_hash_table_lookup(schema, string_tmp));
			index = GPOINTER_TO_INT(g_hash_table_lookup(update_statement->in_variables_index, string_tmp));

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

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, update_statement->stmt, index, type, &value, error)))
			{
				goto _error;
			}
		}

		if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, update_statement->stmt, error)))
		{
			goto _error;
		}

		if (!count)
		{
			g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
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

		if (!(delete_statement = j_sql_statement_new(delete_sql->str, arr_types_in, NULL, NULL, NULL)))
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
