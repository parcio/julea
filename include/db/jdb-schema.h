/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
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

/**
 * \file
 **/

#ifndef JULEA_DB_SCHEMA_H
#define JULEA_DB_SCHEMA_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

#include <glib.h>
#include <bson.h>
#include <julea.h>
#include <db/jdb-type.h>

struct JDBSchemaIndex;
typedef struct JDBSchemaIndex JDBSchemaIndex;

struct JDBSchema;
typedef struct JDBSchema JDBSchema;

/**
 * Allocates a new schema.
 *
 * \param[in] namespace the namespace of the schema
 * \param[in] name the name of the schema
 *
 * \pre namespace != NULL
 * \pre name != NULL
 *
 * \return the new schema or NULL on failure
 **/
JDBSchema* j_db_schema_new(gchar const* namespace, gchar const* name, GError** error);
/**
 * Increase the ref_count of the given schema.
 *
 * \param[in] schema the schema to increase the ref_count
 * \pre schema != NULL
 *
 * \return the schema or NULL on failure
 **/
JDBSchema* j_db_schema_ref(JDBSchema* schema, GError** error);
/**
 * Decrease the ref_count of the given schema - and automatically call free if ref_count is 0.  This is a noop if schema == NULL.
 *
 * \param[in] schema the schema to decrease the ref_count
 **/
void j_db_schema_unref(JDBSchema* schema);
/**
 * Add a field to the schema.
 *
 * \param[in] schema the schema to add a field to
 * \param[in] name the name of the variable to add
 * \param[in] type the type of the variable to add
 *
 * \pre schema != NULL
 * \pre name != NULL
 * \pre schema does not contain a variable with the given name
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_add_field(JDBSchema* schema, gchar const* name, JDBType type, GError** error);
/**
 * query a variable from the schema.
 *
 * \param[in] schema the schema to query
 * \param[in] name the name of the variable to query
 * \param[out] the type of the queried variable
 *
 * \pre schema != NULL
 * \pre name != NULL
 * \pre type != NULL
 * \pre schema contains a variable of the given type
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_get_field(JDBSchema* schema, gchar const* name, JDBType* type, GError** error);
/**
 * query all variables from the schema.
 *
 * \param[in] schema the schema to query
 * \param[out] name the names of all variables in the schema
 * \param[out] the the types of all variables in the schema
 *
 * \pre schema != NULL
 * \pre names != NULL
 * \pre *names is not initialized
 * \pre types != NULL
 * \pre *types is not initialized
 * \post *names is an initialized zero-terminated array of char*. The array must be freed by the caller using g_strfreev
 * \post *types is an initialized array of JDBType. The array has the same length as *names. This array is terminated with the value _J_DB_TYPE_COUNT
 *
 * \return TRUE on success, FALSE otherwise
 **/
guint32 j_db_schema_get_all_fields(JDBSchema* schema, gchar*** names, JDBType** types, GError** error);
/**
 * adds an index to the given schema.
 *
 * \param[in] schema the schema to add a index to
 * \param[in] names the names of the variables to put into an index group
 *
 * \pre schema != NULL
 * \pre names != NULL
 * \pre *names is a zero-terminated array of char*
 * \pre each **names is an previously defined variable-name
 * \post *names could be freed - or modified imediately by the caller
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_add_index(JDBSchema* schema, gchar const** names, GError** error);
/**
 * stores a schema in the backend.
 *
 * \param[in] schema the schema to store
 * \param[in] batch the batch to add this operation to
 *
 * \pre schema != NULL
 * \pre schema contains at least 1 variable
 * \pre batch != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_create(JDBSchema* schema, JBatch* batch, GError** error);
/**
 * querys a schema structure from the backend.
 *
 * \param[in] schema the schema to query
 * \param[in] batch the batch to add this operation to
 *
 * \pre schema != NULL
 * \pre schema exists in the backend
 * \pre batch != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_get(JDBSchema* schema, JBatch* batch, GError** error);
/**
 * deletes a schema structure from the backend.
 *
 * \param[in] schema the schema to delete
 * \param[in] batch the batch to add this operation to
 *
 * \pre schema != NULL
 * \pre schema exists in the backend
 * \pre batch != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_delete(JDBSchema* schema, JBatch* batch, GError** error);
/**
 * compares two schema with each other.
 *
 * \param[in] schema1
 * \param[in] schema2 the schema to compare with each other
 * \param[out] equal TRUE if schema1 and schema2 equals
 *
 * \pre schema1 != NULL
 * \pre schema2 != NULL
 * \pre equal != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_schema_equals(JDBSchema* schema1, JDBSchema* schema2, gboolean* equal, GError** error);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDBSchema, j_db_schema_unref)

#endif
