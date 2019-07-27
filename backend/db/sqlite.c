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

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <sqlite3.h>

#include <julea.h>
#include <julea-internal.h>
#include <julea-db.h>

#include <core/jbson-wrapper.h>

static sqlite3* backend_db = NULL;

static gboolean
j_sql_finalize(void* _stmt, GError** error)
{
	sqlite3_stmt* stmt = _stmt;
	if (sqlite3_finalize(stmt) != SQLITE_OK)
	{
		g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_FINALIZE, "sql finalize failed error was '%s'", sqlite3_errmsg(backend_db));
		goto _error;
	}
	return TRUE;
_error:
	return FALSE;
}

static gboolean
j_sql_prepare(const char* sql, void* _stmt, GError** error)
{
	sqlite3_stmt** stmt = _stmt;
	if (sqlite3_prepare_v3(backend_db, sql, -1, SQLITE_PREPARE_PERSISTENT, stmt, NULL) != SQLITE_OK)
	{
		g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_PREPARE, "sql prepare failed error was '%s'", sqlite3_errmsg(backend_db));
		goto _error;
	}
	return TRUE;
_error:
	j_sql_finalize(*stmt, NULL);
	return FALSE;
}

static gboolean
j_sql_bind_null(void* _stmt, guint idx, GError** error)
{
	sqlite3_stmt* stmt = _stmt;
	if (sqlite3_bind_null(stmt, idx) != SQLITE_OK)
	{
		g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
		goto _error;
	}
	return TRUE;
_error:
	return FALSE;
}

static gboolean
j_sql_column(void* _stmt, guint idx, JDBType type, JDBType_value* value, GError** error)
{
	sqlite3_stmt* stmt = _stmt;
	switch (type)
	{
	case J_DB_TYPE_SINT32:
		value->val_sint32 = sqlite3_column_int64(stmt, idx);
		break;
	case J_DB_TYPE_UINT32:
		value->val_uint32 = sqlite3_column_int64(stmt, idx);
		break;
	case J_DB_TYPE_SINT64:
		value->val_sint64 = sqlite3_column_int64(stmt, idx);
		break;
	case J_DB_TYPE_UINT64:
		value->val_uint64 = sqlite3_column_int64(stmt, idx);
		break;
	case J_DB_TYPE_FLOAT32:
		value->val_float32 = sqlite3_column_double(stmt, idx);
		break;
	case J_DB_TYPE_FLOAT64:
		value->val_float64 = sqlite3_column_double(stmt, idx);
		break;
	case J_DB_TYPE_STRING:
		value->val_string = (const char*)sqlite3_column_text(stmt, idx);
		break;
	case J_DB_TYPE_BLOB:
		value->val_blob = sqlite3_column_blob(stmt, idx);
		value->val_blob_length = sqlite3_column_bytes(stmt, idx);
		break;
	case _J_DB_TYPE_COUNT:
	default:
		g_set_error_literal(error, J_SQL_ERROR, J_SQL_ERROR_INVALID_TYPE, "bson iter invalid type");
		goto _error;
	}
	return TRUE;
_error:
	return FALSE;
}

static gboolean
j_sql_bind_value(void* _stmt, guint idx, JDBType type, JDBType_value* value, GError** error)
{
	sqlite3_stmt* stmt = _stmt;
	switch (type)
	{
	case J_DB_TYPE_SINT32:
		if (sqlite3_bind_int64(stmt, idx, value->val_sint32) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_UINT32:
		if (sqlite3_bind_int64(stmt, idx, value->val_uint32) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_SINT64:
		if (sqlite3_bind_int64(stmt, idx, value->val_sint64) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_UINT64:
		if (sqlite3_bind_int64(stmt, idx, value->val_uint64) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_FLOAT32:
		if (sqlite3_bind_double(stmt, idx, value->val_float32) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_FLOAT64:
		if (sqlite3_bind_double(stmt, idx, value->val_float64) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_STRING:
		if (sqlite3_bind_text(stmt, idx, value->val_string, -1, NULL) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case J_DB_TYPE_BLOB:
		if (sqlite3_bind_blob(stmt, idx, value->val_blob, value->val_blob_length, NULL) != SQLITE_OK)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		break;
	case _J_DB_TYPE_COUNT:
	default:
		g_set_error_literal(error, J_SQL_ERROR, J_SQL_ERROR_INVALID_TYPE, "sql invalid type");
		goto _error;
	}
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_sql_reset(void* _stmt, GError** error)
{
	sqlite3_stmt* stmt = _stmt;
	if (sqlite3_reset(stmt) != SQLITE_OK)
	{
		g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_RESET, "sql reset failed error was '%s'", sqlite3_errmsg(backend_db));
		goto _error;
	}
	return TRUE;
_error:
	return FALSE;
}

static gboolean
j_sql_exec(const char* sql, GError** error)
{
	sqlite3_stmt* stmt;
	if (!j_sql_prepare(sql, &stmt, error))
		goto _error;
	if (sqlite3_step(stmt) != SQLITE_DONE)
	{
		g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_STEP, "sql step failed error was '%s'", sqlite3_errmsg(backend_db));
		goto _error;
	}
	if (!j_sql_finalize(stmt, error))
		goto _error;
	return TRUE;
_error:
	if (!j_sql_finalize(stmt, error))
		goto _error2;
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
static gboolean
j_sql_step(void* _stmt, gboolean* found, GError** error)
{
	sqlite3_stmt* stmt = _stmt;
	guint ret;
	ret = sqlite3_step(stmt);
	*found = ret == SQLITE_ROW;
	if (ret != SQLITE_ROW)
	{
		if (ret == SQLITE_CONSTRAINT)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_CONSTRAINT, "sql constraint failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
		else if (ret != SQLITE_DONE)
		{
			g_set_error(error, J_SQL_ERROR, J_SQL_ERROR_STEP, "sql step failed error was '%s'", sqlite3_errmsg(backend_db));
			goto _error;
		}
	}
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_sql_step_and_reset_check_done(void* _stmt, GError** error)
{
	gboolean sql_found;
	if (!j_sql_step(_stmt, &sql_found, error))
		goto _error;
	if (!j_sql_reset(_stmt, error))
		goto _error;
	return TRUE;
_error:
	if (!j_sql_reset(_stmt, error))
		goto _error2;
	return FALSE;
_error2:
	/*something failed very hard*/
	return FALSE;
}
#include "sql-generic.c"

static gboolean
backend_init(gchar const* path)
{
	g_autofree gchar* dirname = NULL;
	g_return_val_if_fail(path != NULL, FALSE);
	if (strncmp("memory", path, 5))
	{
		dirname = g_path_get_dirname(path);
		g_mkdir_with_parents(dirname, 0700);
		if (sqlite3_open(path, &backend_db) != SQLITE_OK)
			goto _error;
	}
	else
	{
		if (sqlite3_open(":memory:", &backend_db) != SQLITE_OK)
			goto _error;
	}
	if (!j_sql_exec("PRAGMA foreign_keys = ON", NULL))
		goto _error;
	if (!init_sql())
		goto _error;
	return (backend_db != NULL);
_error:
	sqlite3_close(backend_db);
	return FALSE;
}
static void
backend_fini(void)
{
	fini_sql();
	sqlite3_close(backend_db);
}
static JBackend sqlite_backend = {
	.type = J_BACKEND_TYPE_DB,
	.component = J_BACKEND_COMPONENT_CLIENT | J_BACKEND_COMPONENT_SERVER,
	.db = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_schema_create = backend_schema_create,
		.backend_schema_get = backend_schema_get,
		.backend_schema_delete = backend_schema_delete,
		.backend_insert = backend_insert,
		.backend_update = backend_update,
		.backend_delete = backend_delete,
		.backend_query = backend_query,
		.backend_iterate = backend_iterate,
		.backend_batch_start = backend_batch_start,
		.backend_batch_execute = backend_batch_execute,
	},
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &sqlite_backend;
}
