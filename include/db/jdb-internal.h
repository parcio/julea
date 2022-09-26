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

#ifndef JULEA_DB_INTERNAL_H
#define JULEA_DB_INTERNAL_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

#include <julea.h>

#include <db/jdb-entry.h>
#include <db/jdb-iterator.h>
#include <db/jdb-schema.h>
#include <db/jdb-selector.h>
#include <db-util/jbson.h>

G_BEGIN_DECLS

struct JDBEntry
{
	bson_t bson;
	bson_t id;

	JDBSchema* schema;

	gint ref_count;
};

struct JDBIterator
{
	bson_t bson;

	JDBSchema* schema;
	JDBSelector* selector;

	gpointer iterator;

	gint ref_count;

	gboolean valid;
	gboolean bson_valid;
};

struct JDBSchemaIndex
{
	GHashTable* variables;

	guint variable_count;
};

struct JDBSchema
{
	bson_t bson;
	bson_t bson_index;

	GHashTable* variables; //contains char*
	GArray* index; //contains GHashTable * which contain char*

	gchar* namespace;
	gchar* name;

	guint bson_index_count;
	gint ref_count;

	gboolean bson_initialized;
	gboolean bson_index_initialized;
	gboolean server_side;
};

struct JDBSelector
{
	bson_t selection; // the selection part of the query
	bson_t joins; // the join part of the query
	bson_t final; // the complete query as bson; build at first usage of a selector. the selector can be reused but not modified after final was built.
	gboolean final_valid; // TRUE iff final got built and the selector was not modified

	JDBSelectorMode mode;
	JDBSchema* schema; // Primary schema. This is used for joins.

	GHashTable* join_schema; // store the names of joined schemas. This hash table is used as a set and all values are NULL.

	guint selection_count;
	gint ref_count;
};

// Client-side wrappers for backend functions
gboolean j_db_internal_schema_create(JDBSchema* j_db_schema, JBatch* batch, GError** error);
gboolean j_db_internal_schema_get(JDBSchema* j_db_schema, JBatch* batch, GError** error);
gboolean j_db_internal_schema_delete(JDBSchema* j_db_schema, JBatch* batch, GError** error);
gboolean j_db_internal_insert(JDBEntry* j_db_entry, JBatch* batch, GError** error);
gboolean j_db_internal_update(JDBEntry* j_db_entry, JDBSelector* j_db_selector, JBatch* batch, GError** error);
gboolean j_db_internal_delete(JDBEntry* j_db_entry, JDBSelector* j_db_selector, JBatch* batch, GError** error);
gboolean j_db_internal_query(JDBSchema* j_db_schema, JDBSelector* j_db_selector, JDBIterator* j_db_iterator, JBatch* batch, GError** error);
gboolean j_db_internal_iterate(JDBIterator* j_db_iterator, GError** error);

// Client-side additional internal functions

/**
 * \brief Get the selector data represented as a single bson document.
 * 
 * The returned bson is suitable for requests to the DB backend.
 * For more details see `doc/db-code.md`.
 * 
 * \param selector 
 * \return bson_t* 
 */
bson_t* j_db_selector_get_bson(JDBSelector* selector);

/**
 * \brief Build the final field of the selector.
 * 
 * Appends the "t" and "j" section if joins are present.
 * In any case the "s" section will be created.
 *
 * \param selector a pointer of type JDBSelector.
 *
 * \pre selector != NULL
 * \pre selector->final_valid == FALSE
 *
 * \return TRUE on success, FALSE otherwise
 **/

gboolean j_db_selector_finalize(JDBSelector* selector, GError** error);

G_GNUC_INTERNAL JBackend* j_db_get_backend(void);

G_END_DECLS

#endif
