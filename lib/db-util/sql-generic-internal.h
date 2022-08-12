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

#define BACKEND_ID_TYPE J_DB_TYPE_UINT64

// each thread will keep its own DB connection
struct JThreadVariables
{
	void* db_connection; // the backend-specific database connection handle

	// caching prepared statements enables different server-side optimizations
	// each thread maintains its own cache because the statmenets might depend on the (thread-private) connection object
	GHashTable* query_cache; // sql(char*) -> (JSqlStatement*)
	GHashTable* schema_cache; // namespace_name -> GHashTable* (varname -> JDBType)
};

typedef struct JThreadVariables JThreadVariables;

struct JSqlStatement
{
	gpointer stmt; // the prepared statement of a DBMS

	// variable index of the input (only used for insert and update)
	// the where part can contain the same variable mutiple times
	// determining the position for inputs into where parts is simply done by coounting up during bson iteration
	// this is correct because the statement was generated using the same bson
	GHashTable* in_variables_index; // variable(char*) -> position as integer (directly stored to the pointer field)
	
	// column index of the output (only used for select)
	GHashTable* out_variables_index; // same structure as above and only used in generic_query so that the output columns can be iterated
	// the hash table may be null if the statement corresponds to a hard coded query (e.g., DELETE FROM %s%s_%s%s WHERE _id = ?)
	// this reduces overhead and the following code assumes the _id position for binds anyway
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
