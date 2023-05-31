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

		if (thread_variables->db_connection)
		{
			specs->func.connection_close(thread_variables->db_connection);
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
		thread_variables->schema_cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (void (*)(void*))g_hash_table_unref);

		if (!(thread_variables->db_connection && thread_variables->query_cache && thread_variables->schema_cache))
		{
			goto _error;
		}

		g_private_replace(&thread_variables_global, thread_variables);
	}

	return thread_variables;

_error:
	g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_FAILED, "could not initiliaze thread lokal variables");
	// thread_variables_fini is NULL safe
	thread_variables_fini(thread_variables);

	return NULL;
}

JSqlStatement*
j_sql_statement_new(gchar const* query, GArray* types_in, GArray* types_out, GHashTable* out_variables_index, GHashTable* variable_types, GError** error)
{
	JThreadVariables* thread_variables = NULL;
	JSqlStatement* statement = NULL;

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(specs->backend_data, NULL))))
	{
		goto _error;
	}

	statement = g_new0(JSqlStatement, 1);

	if (!specs->func.statement_prepare(thread_variables->db_connection, query, &statement->stmt, types_in, types_out, error))
	{
		goto _error;
	}

	if (out_variables_index)
	{
		statement->out_variables_index = g_hash_table_ref(out_variables_index);
	}

	if (variable_types)
	{
		statement->variable_types = g_hash_table_ref(variable_types);
	}

	return statement;

_error:
	g_free(statement);

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
			g_hash_table_unref(ptr->out_variables_index);
		}

		if (ptr->variable_types)
		{
			g_hash_table_unref(ptr->variable_types);
		}

		specs->func.statement_finalize(thread_variables->db_connection, ptr->stmt, NULL);
		g_free(ptr);
	}
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
j_sql_get_full_field_name(const gchar* namespace, const gchar* table, const gchar* field)
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
