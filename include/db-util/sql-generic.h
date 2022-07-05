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

#ifndef SQL_GENERIC_BACKEND_H
#define SQL_GENERIC_BACKEND_H

#include <glib.h>
#include <julea.h>
#include <julea-db.h>

/*
 * this file does not care which sql-database is actually in use, and uses only defines sql-syntax to allow fast and easy implementations for any new sql-database backend
*/

struct JSQLSpecifics
{
	// DB connection or something likewise
	void* db_connection;

	guint64 max_buf_size;
	gboolean single_threaded;

	struct
	{
		gboolean (*transaction_start)(gpointer db_connection, GError** error)
		gboolean (*transaction_commit)(gpointer db_connection, GError** error)
		gboolean (*transaction_abort)(gpointer db_connection, GError** error)
		gboolean (*j_sql_prepare)(gpointer db_connection, const char* sql, void* _stmt, GArray* types_in, GArray* types_out, GError** error)
		gboolean (*j_sql_finalize)(gpointer db_connection, void* _stmt, GError** error)
		gboolean (*j_sql_bind_null)(gpointer db_connection, void* _stmt, guint idx, GError** error)
		gboolean (*j_sql_bind_value)(gpointer db_connection, void* _stmt, guint idx, JDBType type, JDBTypeValue* value, GError** error)
		gboolean (*j_sql_column)(gpointer db_connection, void* _stmt, guint idx, JDBType type, JDBTypeValue* value, GError** error)
		gboolean (*j_sql_reset)(gpointer db_connection, void* _stmt, GError** error)
		gboolean (*j_sql_step)(gpointer db_connection, void* _stmt, gboolean* found, GError** error)
		gboolean (*j_sql_exec)(gpointer db_connection, const char* sql, GError** error)
		gboolean (*j_sql_step_and_reset_check_done)(gpointer db_connection, void* _stmt, GError** error)
	} sql_func;

	struct 
	{
		const gchar* autoincrement;
		const gchar* uint64_type;
		const gchar* select_last;
		const gchar* quote;
	} sql_string_constants;
};

typedef JSQLSpecifics struct JSQLSpecifics;

gboolean generic_batch_start(JSQLSpecifics* backend_data, gchar const* namespace, JSemantics* semantics, gpointer* _batch, GError** error);
gboolean generic_batch_execute(JSQLSpecifics* backend_data, gpointer _batch, GError** error):

gboolean generic_schema_create(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, bson_t const* schema, GError** error);
gboolean generic_schema_get(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, bson_t* schema, GError** error);
gboolean generic_schema_delete(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, GError** error);

gboolean generic_insert(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, bson_t const* metadata, bson_t* id, GError** error);
gboolean generic_update(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error);
gboolean generic_delete(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, bson_t const* selector, GError** error);
gboolean generic_query(JSQLSpecifics* backend_data, gpointer _batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error);
gboolean generic_iterate(JSQLSpecifics* backend_data, gpointer _iterator, bson_t* metadata, GError** error);

#endif
