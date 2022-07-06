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

#include <db-util/sql-generic.h>

struct JThreadVariables
{
	gboolean initialized;
	void* db_connection;
	GHashTable* namespaces;
};

typedef struct JThreadVariables JThreadVariables;

struct JSqlCacheNames
{
	GHashTable* names;
};

typedef struct JSqlCacheNames JSqlCacheNames;

struct JSqlCacheSQLQueries
{
	GHashTable* types; // variablename(char*) -> variabletype(JDBType)
	GHashTable* queries; //sql(char*) -> (JSqlCacheSQLPrepared*)
};

typedef struct JSqlCacheSQLQueries JSqlCacheSQLQueries;

struct JSqlCacheSQLPrepared
{
	GString* sql;
	gpointer stmt;
	guint variables_count;
	GHashTable* variables_index;
	gboolean initialized;
	gchar* namespace;
	gchar* name;
	gpointer backend_data;
};

typedef struct JSqlCacheSQLPrepared JSqlCacheSQLPrepared;

struct JSqlBatch
{
	const gchar* namespace;
	JSemantics* semantics;
	gboolean open;
	gboolean aborted;
};

typedef struct JSqlBatch JSqlBatch;

struct JSqlIterator
{
	char* namespace;
	char* name;
	GArray* arr;
	guint index;
};

typedef struct JSqlIterator JSqlIterator;

static void freeJSqlCacheNames(void* ptr);
static void thread_variables_fini(void* ptr);
static gboolean _backend_schema_get(gpointer backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error);

#endif
