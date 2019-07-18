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

#ifndef JULEA_DB_SCHEMA_H
#define JULEA_DB_SCHEMA_H

#include <glib.h>
#include <bson.h>
#include <julea.h>
#include <db/jdb-type.h>

struct JDBSchema
{
	gchar* namespace;
	gchar* name;
	gboolean bson_initialized;
	bson_t bson;
	gboolean bson_index_initialized;
	bson_t bson_index;
	guint bson_index_count;
	gint ref_count;
	gboolean server_side;
};
typedef struct JDBSchema JDBSchema;

JDBSchema* j_db_schema_new(gchar const* namespace, gchar const* name, GError** error);

JDBSchema* j_db_schema_ref(JDBSchema* schema, GError** error);
void j_db_schema_unref(JDBSchema* schema);

gboolean j_db_schema_add_field(JDBSchema* schema, gchar const* name, JDBType type, GError** error);
gboolean j_db_schema_get_field(JDBSchema* schema, gchar const* name, JDBType* type, GError** error);
guint32 j_db_schema_get_all_fields(JDBSchema* schema, gchar*** names, JDBType** types, GError** error);

gboolean j_db_schema_add_index(JDBSchema* schema, gchar const** names, GError** error);

gboolean j_db_schema_create(JDBSchema* schema, JBatch* batch, GError** error);
gboolean j_db_schema_get(JDBSchema* schema, JBatch* batch, GError** error);
gboolean j_db_schema_delete(JDBSchema* schema, JBatch* batch, GError** error);

gboolean j_db_schema_equals(JDBSchema* schema1, JDBSchema* schema2, gboolean* equal, GError** error);

#endif
