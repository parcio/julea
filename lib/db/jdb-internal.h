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
#include <db/jdb-selector.h>

struct JDBEntry
{
        gint ref_count;
        JDBSchema* schema;
        bson_t bson;
};
struct JDBIterator
{
        JDBSchema* schema;
        JDBSelector* selector;
        gpointer iterator;
        gint ref_count;
        gboolean valid;
        gboolean bson_valid;
        bson_t bson;
};
struct JDBSchemaIndex
{
        guint variable_count;
        GHashTable* variables;
};
struct JDBSchema
{
        gchar* namespace;
        gchar* name;
        gboolean bson_initialized;
        bson_t bson;
        gboolean bson_index_initialized;
        GHashTable* variables; //contains char*
        GArray* index; //contains GHashTable * which contain char*
        bson_t bson_index;
        guint bson_index_count;
        gint ref_count;
        gboolean server_side;
};
struct JDBSelector
{
        JDBSelectorMode mode;
        JDBSchema* schema;
        gint ref_count;
        bson_t bson;
        guint bson_count;
};
union JDBTypeValue
{
        guint32 val_uint32;
        gint32 val_sint32;
        guint64 val_uint64;
        gint64 val_sint64;
        gdouble val_float64;
        gfloat val_float32;
        gchar const* val_string;
        struct
        {
                gchar const* val_blob;
                guint32 val_blob_length;
        };
};

//client side wrappers for backend functions
gboolean j_db_internal_schema_create(gchar const* namespace, gchar const* name, bson_t const* schema, JBatch* batch, GError** error);
gboolean j_db_internal_schema_get(gchar const* namespace, gchar const* name, bson_t* schema, JBatch* batch, GError** error);
gboolean j_db_internal_schema_delete(gchar const* namespace, gchar const* name, JBatch* batch, GError** error);
gboolean j_db_internal_insert(gchar const* namespace, gchar const* name, bson_t const* metadata, JBatch* batch, GError** error);
gboolean j_db_internal_update(gchar const* namespace, gchar const* name, bson_t const* selector, bson_t const* metadata, JBatch* batch, GError** error);
gboolean j_db_internal_delete(gchar const* namespace, gchar const* name, bson_t const* selector, JBatch* batch, GError** error);
gboolean j_db_internal_query(gchar const* namespace, gchar const* name, bson_t const* selector, gpointer* iterator, JBatch* batch, GError** error);
gboolean j_db_internal_iterate(gpointer iterator, bson_t* metadata, GError** error);

//client side additional internal functions
bson_t* j_db_selector_get_bson(JDBSelector* selector);

#endif
