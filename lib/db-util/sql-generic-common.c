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

GPrivate thread_variables_global = G_PRIVATE_INIT(thread_variables_fini);
JSQLSpecifics* specs;
G_LOCK_DEFINE(sql_backend_lock);

gboolean
sql_generic_init(JSQLSpecifics* specifics)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;
	gpointer connection = NULL;

	// keep reference to the DB specifics
	specs = specifics;

	// init the DB schema management
	if (!(connection = specs->func.connection_open(specs->backend_data)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!specs->func.sql_exec(connection,
					     "CREATE TABLE IF NOT EXISTS schema_structure ("
					     "namespace VARCHAR(255),"
					     "name VARCHAR(255),"
					     "varname VARCHAR(255),"
					     "vartype INTEGER"
					     /// \todo figure out whether we should add an index instead
					     //"PRIMARY KEY (namespace, name, varname)"
					     ")",
					     NULL)))
	{
		goto _error;
	}

	ret = TRUE;

_error:
	specs->func.connection_close(connection);
	return ret;
}

void
sql_generic_fini(void)
{
	J_TRACE_FUNCTION(NULL);
}

void
thread_variables_fini(void* ptr)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = ptr;

	if (thread_variables)
	{
		specs->func.connection_close(thread_variables->db_connection);

		if (thread_variables->query_cache)
		{
			// keys and values will be freed by the at create time supplied free functions
			g_hash_table_destroy(thread_variables->query_cache);
		}

		if (thread_variables->schema_cache)
		{
			// keys and values will be freed by the at create time supplied free functions
			g_hash_table_destroy(thread_variables->schema_cache);
		}

		g_free(thread_variables);
	}
}

JThreadVariables*
thread_variables_get(gpointer backend_data, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;
	(void)error;

	thread_variables = g_private_get(&thread_variables_global);

	if (!thread_variables)
	{
		thread_variables = g_new0(JThreadVariables, 1);

		thread_variables->db_connection = specs->func.connection_open(backend_data);
		thread_variables->query_cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (void (*)(void*))j_sql_statement_free);
		thread_variables->schema_cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (void (*)(void*))g_hash_table_destroy);

		if (!(thread_variables->db_connection && thread_variables->query_cache && thread_variables->schema_cache))
		{
			goto _error;
		}

		g_private_replace(&thread_variables_global, thread_variables);
	}

	return thread_variables;

_error:
	/// TODO mem management and error msg

	return NULL;
}

// takes ownership of *_variables_index which is destroyed on error avoiding an unnecessary copy
JSqlStatement*
j_sql_statement_new(gchar const* query, GArray* types_in, GArray* types_out, GHashTable* in_variables_index, GHashTable* out_variables_index)
{
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* statement = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(specs->backend_data, NULL))))
	{
		goto _error;
	}

	statement = g_new(JSqlStatement, 1);

	if (!specs->func.statement_prepare(thread_variables->db_connection, query, &statement->stmt, types_in, types_out, NULL))
	{
		goto _error;
	}

	statement->in_variables_index = in_variables_index;
	statement->out_variables_index = out_variables_index;

	/// \todo fetching the output types for joins is easy here using out_variables_index and out_types
	return statement;

_error:
	if (in_variables_index)
	{
		g_hash_table_destroy(in_variables_index);
	}

	if (out_variables_index)
	{
		g_hash_table_destroy(out_variables_index);
	}

	if (statement)
	{
		g_free(statement);
	}

	return NULL;
}

void
j_sql_statement_free(JSqlStatement* ptr)
{
	J_TRACE_FUNCTION(NULL);
	JThreadVariables* thread_variables = NULL;

	if (ptr)
	{
		if (G_UNLIKELY(!(thread_variables = thread_variables_get(specs->backend_data, NULL))))
			g_assert_not_reached();

		if (ptr->out_variables_index)
		{
			g_hash_table_destroy(ptr->out_variables_index);
		}

		if (ptr->in_variables_index)
		{
			g_hash_table_destroy(ptr->in_variables_index);
		}

		specs->func.statement_finalize(thread_variables->db_connection, ptr->stmt, NULL);
		g_free(ptr);
	}
}

JSqlIterator*
j_sql_iterator_new(JSqlStatement* stmt, JSqlBatch* batch, const gchar* name)
{
	JSqlIterator* sql_iterator = g_new(JSqlIterator, 1);

	sql_iterator->batch = batch;
	sql_iterator->statement = stmt;
	sql_iterator->name = name;

	return sql_iterator;
}

// the iterator is just a convenient wrapper arround multiple arguments
// the contained objects are managed in other places
void
j_sql_iterator_free(JSqlIterator* iter)
{
	g_free(iter);
}
