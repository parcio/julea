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

#include <julea-config.h>

#include "sql-generic-internal.h"

gboolean
sql_generic_schema_create(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* schema, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBType type;
	gboolean has_next;
	gboolean equals;
	guint counter = 0;
	gboolean found_index = FALSE;
	JDBTypeValue value;
	bson_iter_t iter;
	JSqlBatch* batch = _batch;
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* metadata_insert_query = NULL;
	const gchar* metadata_insert_sql = "INSERT INTO schema_structure(namespace, name, varname, vartype) VALUES (?, ?, ?, ?)";
	g_autoptr(GString) create_sql = g_string_new(NULL);

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(schema != NULL, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	metadata_insert_query = g_hash_table_lookup(thread_variables->query_cache, metadata_insert_sql);

	if (!metadata_insert_query)
	{
		g_autoptr(GArray) arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));

		type = J_DB_TYPE_STRING;
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);
		type = J_DB_TYPE_UINT32;
		g_array_append_val(arr_types_in, type);

		if (!(metadata_insert_query = j_sql_statement_new(metadata_insert_sql, arr_types_in, NULL, NULL, NULL, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(metadata_insert_sql), metadata_insert_query))
		{
			// in all other error cases metadata_insert_query is already owned by the hash table
			j_sql_statement_free(metadata_insert_query);
			goto _error;
		}
	}

	if (G_UNLIKELY(!_backend_batch_execute(backend_data, batch, error)))
	{
		//no ddl in transaction - most databases won't support that - continue without any open transaction
		goto _error;
	}

	/// \todo IDs will allways be casted to an unsigned 64-bit integer, so it's safe to change this in the client as well
	g_string_append_printf(create_sql, "CREATE TABLE %s%s_%s%s ( _id %s %s PRIMARY KEY",
			       specs->sql.quote, batch->namespace, name, specs->sql.quote, specs->sql.id_type, specs->sql.autoincrement);

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
			g_string_append_printf(create_sql, ", %s%s%s", specs->sql.quote,
					       j_bson_iter_key(&iter, error), specs->sql.quote);

			if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			type = value.val_uint32;

			switch (type)
			{
				case J_DB_TYPE_SINT32:
					g_string_append(create_sql, " INTEGER");
					break;
				case J_DB_TYPE_ID:
				case J_DB_TYPE_UINT32:
					g_string_append(create_sql, " INTEGER");
					break;
				case J_DB_TYPE_FLOAT32:
					g_string_append(create_sql, " REAL");
					break;
				case J_DB_TYPE_SINT64:
					g_string_append(create_sql, " INTEGER");
					break;
				case J_DB_TYPE_UINT64:
					g_string_append(create_sql, specs->sql.uint64_type);
					break;
				case J_DB_TYPE_FLOAT64:
					g_string_append(create_sql, " REAL");
					break;
				case J_DB_TYPE_STRING:
					g_string_append(create_sql, " VARCHAR(255)");
					break;
				case J_DB_TYPE_BLOB:
					g_string_append(create_sql, " BLOB");
					break;
				default:
					g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_DB_TYPE_INVALID, "db type invalid");
					goto _error;
			}
		}
	}

	g_string_append(create_sql, " )");

	if (G_UNLIKELY(!counter))
	{
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_SCHEMA_EMPTY, "schema empty");
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.sql_exec(thread_variables->db_connection, create_sql->str, error)))
	{
		goto _error;
	}

	if (found_index)
	{
		guint i = 0;
		bson_iter_t iter_child;
		gboolean first;

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
			bson_iter_t iter_child2;
			g_autoptr(GString) index_create = g_string_new(NULL);

			if (G_UNLIKELY(!j_bson_iter_next(&iter_child, &has_next, error)))
			{
				goto _error;
			}

			if (!has_next)
			{
				break;
			}

			first = TRUE;

			g_string_append_printf(index_create, "CREATE INDEX %s%s_%s_%d%s ON %s%s_%s%s ( ",
					       specs->sql.quote, batch->namespace, name, i, specs->sql.quote,
					       specs->sql.quote, batch->namespace, name, specs->sql.quote);

			if (G_UNLIKELY(!j_bson_iter_recurse_array(&iter_child, &iter_child2, error)))
			{
				goto _error;
			}

			while (TRUE)
			{
				const char* string_tmp;

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
					g_string_append(index_create, ", ");
				}

				if (G_UNLIKELY(!j_bson_iter_value(&iter_child2, J_DB_TYPE_STRING, &value, error)))
				{
					goto _error;
				}

				string_tmp = value.val_string;
				g_string_append_printf(index_create, "%s%s%s", specs->sql.quote, string_tmp, specs->sql.quote);
			}

			g_string_append(index_create, " )");

			if (G_UNLIKELY(!specs->func.sql_exec(thread_variables->db_connection, index_create->str, error)))
			{
				goto _error;
			}

			i++;
		}
	}

	value.val_string = batch->namespace;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 1, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_string = name;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 2, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_string = "_id";

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 3, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_uint32 = BACKEND_ID_TYPE;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 4, J_DB_TYPE_UINT32, &value, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, metadata_insert_query->stmt, error)))
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

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 1, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			value.val_string = name;

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 2, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			value.val_string = j_bson_iter_key(&iter, error);

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 3, J_DB_TYPE_STRING, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!j_bson_iter_value(&iter, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			if (value.val_uint32 == J_DB_TYPE_ID)
			{
				value.val_uint32 = BACKEND_ID_TYPE;
			}

			if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_insert_query->stmt, 4, J_DB_TYPE_UINT32, &value, error)))
			{
				goto _error;
			}

			if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, metadata_insert_query->stmt, error)))
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

	return FALSE;

_error2:
	return FALSE;
}

gboolean
sql_generic_schema_delete(gpointer backend_data, gpointer _batch, gchar const* name, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue value;
	JThreadVariables* thread_variables = NULL;
	JSqlBatch* batch = _batch;
	g_autoptr(GString) table_drop_sql = NULL;
	JSqlStatement* metadata_delete_query = NULL;
	const gchar* metadata_delete_sql = "DELETE FROM schema_structure WHERE namespace=? AND name=?";

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(batch != NULL, FALSE);

	table_drop_sql = g_string_new("DROP TABLE IF EXISTS ");
	g_string_append_printf(table_drop_sql, "%s%s_%s%s", specs->sql.quote, batch->namespace, name, specs->sql.quote);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	metadata_delete_query = g_hash_table_lookup(thread_variables->query_cache, metadata_delete_sql);

	if (!metadata_delete_query)
	{
		JDBType type;
		g_autoptr(GArray) arr_types_in = NULL;
		arr_types_in = g_array_new(FALSE, FALSE, sizeof(JDBType));
		type = J_DB_TYPE_STRING;
		g_array_append_val(arr_types_in, type);
		g_array_append_val(arr_types_in, type);

		if (!(metadata_delete_query = j_sql_statement_new(metadata_delete_sql, arr_types_in, NULL, NULL, NULL, error)))
		{
			goto _error;
		}

		if (!g_hash_table_insert(thread_variables->query_cache, g_strdup(metadata_delete_sql), metadata_delete_query))
		{
			// in all other error cases metadata_delete_query is already owned by the hash table
			j_sql_statement_free(metadata_delete_query);
			goto _error;
		}
	}

	if (G_UNLIKELY(!_backend_batch_execute(backend_data, batch, error)))
	{
		//no ddl in transaction - most databases wont support that - continue without any open transaction
		goto _error;
	}

	value.val_string = batch->namespace;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_delete_query->stmt, 1, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	value.val_string = name;

	if (G_UNLIKELY(!specs->func.statement_bind_value(thread_variables->db_connection, metadata_delete_query->stmt, 2, J_DB_TYPE_STRING, &value, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.statement_step_and_reset_check_done(thread_variables->db_connection, metadata_delete_query->stmt, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.sql_exec(thread_variables->db_connection, table_drop_sql->str, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	if (table_drop_sql)
	{
		g_string_free(table_drop_sql, TRUE);
	}

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, NULL)))
	{
		goto _error2;
	}

_error2:
	return FALSE;
}
