/*
 * JULEA - Flexible storage framework
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

#ifndef SQL_GENERIC_BACKEND_INTERNAL_H
#define SQL_GENERIC_BACKEND_INTERNAL_H

#include <glib.h>
#include <db-util/sql-generic.h>
#include <db-util/jbson.h>

/**
 * This library contains bson utility functions and a generic SQL DB backend
 * implementation that must be specialized by providing a JSQLSpecifics struct at init.
 *
 * The implementation of the SQL functions is split into:
 *    - Data Definition Language (DDL)
 *    - Data Manipulation Language (DML)
 *    - Data Query Language (DQL)
 *    - Transaction Control Language (TCL)
 */

/**
 * \brief The ID type that will be sent back to the client.
 * 
 * \attention Not all DB backends support this type as autoincremented primary key (e.g., SQLite). In this case a type conversion will happen.
 */
#define BACKEND_ID_TYPE J_DB_TYPE_UINT64

// each thread will keep its own DB connection
struct JThreadVariables
{
	/// The backend-specific database connection handle.
	void* db_connection; 

	/**
	 * \brief
	 * 
	 * sql or keyword(char*) -> (JSqlStatement*)
	 * Caching prepared statements enables different DB backend server-side optimizations.
	 * Each thread maintains its own cache because the statments likely depend on the (thread-private) DB connection object.
	 */
	GHashTable* query_cache;

	/**
	 * \brief 
	 * 
	 * namespace_name(char*) -> GHashTable* (varname(char*) -> JDBType(directly as int))
	 */
	GHashTable* schema_cache;
};

typedef struct JThreadVariables JThreadVariables;

/**
 * \brief The JSqlStatement wraps a DB backend-specific prepared statement.
 * 
 */
struct JSqlStatement
{
	/// The prepared statement of a DB backend.
	gpointer stmt;

	/** 
	 * \brief Indices of the input variables.
	 * 
	 * variable(char*) -> position (int directly stored to the pointer field)
	 * This field is not NULL iff the statement is an UPDATE or DELETE.
	 * Even though the WHERE part of a SELECT query does of course contain inputs, the same input variable can occur several times (e.g., "x > 3 and x < 6").
	 * Consequently the key-value pairs would be overridden for SELECT statements.
	 * However, determining the position for those inputs can be simply done by counting while iterating the selector bson.
	 * This method is correct because the statement was generated using the same bson.
	**/
	GHashTable* in_variables_index;

	/**
	 * \brief Indices of the output variables (i.e., columns).
	 * 
	 * variable(char*) -> position (int directly stored to the pointer field)
	 * This field is not NULL iff the statement is a SELECT.
	 */
	GHashTable* out_variables_index;
};

typedef struct JSqlStatement JSqlStatement;

struct JSqlBatch
{
	const gchar* namespace;
	JSemantics* semantics;
	gboolean open;
	gboolean aborted;
};

typedef struct JSqlBatch JSqlBatch;

// an iterator associates a prepared and bound statement with a batch and certain schema information
// using this object the statement can be stepped through and results can be extracted
struct JSqlIterator
{
	JSqlStatement* statement;
	JSqlBatch* batch;
	/// \todo the name field has to change when implementing the join
	const gchar* name;
};

typedef struct JSqlIterator JSqlIterator;

// common
extern GPrivate thread_variables_global;
extern JSQLSpecifics* specs;
G_LOCK_EXTERN(sql_backend_lock);

void thread_variables_fini(void* ptr);
JThreadVariables* thread_variables_get(gpointer backend_data, GError** error);
JSqlStatement* j_sql_statement_new(gchar const* query, GArray* types_in, GArray* types_out, GHashTable* in_variables_index, GHashTable* out_variables_index);
void j_sql_statement_free(JSqlStatement* ptr);
JSqlIterator* j_sql_iterator_new(JSqlStatement* stmt, JSqlBatch* batch, const gchar* name);
void j_sql_iterator_free(JSqlIterator* iter);
// DQL
// retrieve schema as bson (suitable for clients)
gboolean _backend_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error);
// use a transparent cache to provide schema info as hash table for internal usage
GHashTable* get_schema(gpointer backend_data, gpointer _batch, gchar const* name, GError** error);
gboolean build_selector_query(gpointer backend_data, bson_iter_t* iter, GString* sql, JDBSelectorMode mode, GArray* arr_types_in, GHashTable* schema_cache, GError** error);
gboolean bind_selector_query(gpointer backend_data, bson_iter_t* iter, JSqlStatement* statement, GHashTable* schema, GError** error);
// queries the IDs of entries that match the given selector
gboolean _backend_query_ids(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GArray** matches, GError** error);

// TCL
gboolean _backend_batch_start(gpointer backend_data, JSqlBatch* batch, GError** error);
gboolean _backend_batch_execute(gpointer backend_data, JSqlBatch* batch, GError** error);
gboolean _backend_batch_abort(gpointer backend_data, JSqlBatch* batch, GError** error);

#endif
