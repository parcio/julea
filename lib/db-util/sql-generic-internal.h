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
 * Further information can be found in `doc/db-code.md`.
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

/**
 * \brief The JThreadVariables struct bundles the thread-local DB connection, the query cache and the schema cache.
 */
struct JThreadVariables
{
	/// The backend-specific database connection handle.
	gpointer db_connection;

	/**
	 * \brief Cache for prepared statements.
	 *
	 * sql or keyword(char*) -> (JSqlStatement*)
	 * Caching prepared statements enables different DB backend server-side optimizations.
	 * Each thread maintains its own cache because the statments likely depend on the (thread-private) DB connection object.
	 */
	GHashTable* query_cache;

	/**
	 * \brief Cache for schemas.
	 *
	 * namespace_name(char*) -> (GHashTable* (full variable name(char*) -> JDBType(directly as int)))
	 * A thread local schema cache.
	 * The schema information is stored in a hash table that maps the full field name to the respectove JDBType.
	 * The keys are correctly quoted and can be used as part of SQL strings.
	 */
	GHashTable* schema_cache;
};

typedef struct JThreadVariables JThreadVariables;

/**
 * \brief Manages the status of a batch.
 *
 * Batches are implemented using transactions.
 * An error of an operation inside a batch will cause a rollback.
 */
struct JSqlBatch
{
	/// The namespace of the operations inside the batch.
	const gchar* namespace;

	/// The semantics of the batched operations.
	JSemantics* semantics;

	/// TRUE if the batch is open.
	gboolean open;

	/// TRUE if there was an error and the batch was aborted.
	gboolean aborted;
};

typedef struct JSqlBatch JSqlBatch;

/**
 * \brief The JSqlStatement wraps a DB backend-specific prepared statement.
 *
 * The hash tables may be NULL and are only set when needed.
 */
struct JSqlStatement
{
	/// The prepared statement of a DB backend.
	gpointer stmt;

	/**
	 * \brief Indices of the output variables (i.e., columns).
	 *
	 * full name of the variable (gchar*) -> position (int directly stored to the pointer field)
	 * This field is not NULL iff the statement is a SELECT.
	 */
	GHashTable* out_variables_index;

	/**
	 * \brief Types of the variables contained in the query.
	 *
	 * full name of the variable (gchar*) -> type (JDBType)
	 * This field may either be a schema from the cache or a new hash table conatining fields from all schemas in a join.
	 */
	GHashTable* variable_types;
};

typedef struct JSqlStatement JSqlStatement;

// common

/// Holds a thread-private pointer to JThreadVariables.
extern GPrivate thread_variables_global;

/// Contains the specific string constants and functions for a DB backend.
extern JSQLSpecifics* specs;

/// Starting a batch requires locking if the DB backend does not support concurrent accesses.
G_LOCK_EXTERN(sql_backend_lock);

/**
 * \brief Free function for JThreadVariables.
 *
 * \param ptr Pointer to an allocated JThreadVariables struct.
 */
void thread_variables_fini(void* ptr);

/**
 * \brief Retrieve the thread-private JThreadVariables.
 *
 * The struct will be initialized if this function is called by a new thread for the first time.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return JThreadVariables* An initilized pointer to the thred-pribvate variables. The returned data schould NOT be freed.
 */
JThreadVariables* thread_variables_get(gpointer backend_data, GError** error);

/**
 * \brief Construct a JSqlStatement struct from the necessary information.
 *
 * \param query A SQL query string.
 * \param types_in The types of input variables in correct order. Needed by some DB backends for statement compilation.
 * \param types_out The types of output variables in correct order. Needed by some DB backends for statement compilation.
 * \param out_variables_index A map from output variable names to their position. See JSqlStatement for details.
 * \param variable_types Type information for the query. See JSqlStatement for details.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return JSqlStatement* The constructed statement or NULL on error.
 *
 * \attention If the function returns a valid pointer in_variables_index and out_variables_index are now owned by the new struct and the original pointers are set to NULL.
 */
JSqlStatement* j_sql_statement_new(gchar const* query, GArray* types_in, GArray* types_out, GHashTable* out_variables_index, GHashTable* variable_types, GError** error);

/**
 * \brief Destructor for a JSqlStatement.
 *
 * \param ptr Pointer to the Statement.
 */
void j_sql_statement_free(JSqlStatement* ptr);

/**
 * \brief Get the full field name from the different parts.
 *
 * The name is correctly quoted and suitable as key in schema hash tables and as name in queries.
 *
 * \param namespace The namespace of the operation.
 * \param table The name of the table.
 * \param field The name of a column in the table.
 * \return GString*
 */
GString* j_sql_get_full_field_name(const gchar* namespace, const gchar* table, const gchar* field);

// DQL

/**
 * \brief Retrieve the schema as bson document.
 *
 * This format is suitable for answering schema requests by clients.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param namespace The namespace of the schema.
 * \param name The schema name.
 * \param[in,out] schema A bson document to be filled with schema information as a list of variable name and type pairs.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean _backend_schema_get(gpointer backend_data, gchar const* namespace, gchar const* name, bson_t* schema, GError** error);

/**
 * \brief Get the schema as a HashTable for internal usage.
 *
 * Uses a transparent cache for schemas.
 *
 * \todo Currently _backend_schema_get is called and the returned bson is converted to a GHashTable.
 * A better way would be to use one common function for both so no conversion is neccessary.
 *
 * \param backend_data The backend-specific information to open a connection if needed.
 * \param namespace The namespace of the schema.
 * \param name The schema name.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return GHashTable* A valid GHashTable pointer on success or NULL otherwise. The hash table should by freed using g_hash_table_unref (or by using g_autoptr).
 */
GHashTable* get_schema(gpointer backend_data, gchar const* namespace, gchar const* name, GError** error);

/**
 * \brief Build the WHERE part of a SELECT statement from a selector.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param iter An initialized iterator over the relevant part of the selector bson document.
 * \param batch The batch of the operation.
 * \param[in,out] sql A GString to which the WHERE part of the query should be appended.
 * \param mode The mode of the selector (i.e., AND or OR).
 * \param arr_types_in An allocated GArray to which the found types will be appended.
 * \param schema The database schema in hash table format. \todo need to change this one for joins
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean build_query_condition_part(gpointer backend_data, JSqlBatch* batch, bson_iter_t* iter, GString* sql, JDBSelectorMode mode, GArray* arr_types_in, GHashTable* schema, GError** error);

/**
 * \brief Bind the variables in the WHERE part of a SELECT statement.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param iter An initialized iterator over the relevant part of the selector bson document. Should be retrieved the same way as for build_selector_query to ensure the same order of variables!
 * \param statement A JSqlStatement which
 * \param schema The database schema in hash table format. \todo need to change this one for joins
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean bind_selector_query(gpointer backend_data, const gchar* namespace, bson_iter_t* iter, JSqlStatement* statement, GHashTable* schema, GError** error);

/**
 * \brief Query the IDs of rows that match a selector.
 *
 * It is is used in the update and delete functions.
 *
 * \todo Update and delete could be done by adding the selection part to the respective query, making this function unnecessary.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param _batch A JSqlBatch object.
 * \param name The schema name.
 * \param selector A bson selector document sent by the client.
 * \param[out] matches A GArray of the matched IDs.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean _backend_query_ids(gpointer backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GArray** matches, GError** error);

// TCL

/**
 * \brief Start a batch. Starts a transaction in the DB backend.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param batch The batch to modify.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean _backend_batch_start(gpointer backend_data, JSqlBatch* batch, GError** error);

/**
 * \brief Execute a batch. Commits a transaction in the DB backend.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param batch The batch to modify.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean _backend_batch_execute(gpointer backend_data, JSqlBatch* batch, GError** error);

/**
 * \brief Abort a batch. Triggers a rollback in the DB backend.
 *
 * \param backend_data The backend-specific information to open a connection.
 * \param batch The batch to modify.
 * \param[out] error An uninitialized GError* for error code passing.
 * \return gboolean TRUE on success, FALSE otherwise.
 */
gboolean _backend_batch_abort(gpointer backend_data, JSqlBatch* batch, GError** error);

#endif
