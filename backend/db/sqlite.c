/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
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

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <sqlite3.h>

#include <julea.h>
#include <julea-db.h>

#include <db-util/jbson.h>
#include <db-util/sql-generic.h>

struct JSQLiteData
{
	gchar* path;
	sqlite3* db;
};

typedef struct JSQLiteData JSQLiteData;

static gboolean
j_sql_finalize(gpointer backend_db, void* _stmt, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt = _stmt;

	if (G_UNLIKELY(sqlite3_finalize(stmt) != SQLITE_OK))
	{
		g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_FINALIZE, "sql finalize failed error was '%s'", sqlite3_errmsg((sqlite3*)backend_db));
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_sql_prepare(gpointer backend_db, const char* sql, void* _stmt, GArray* types_in, GArray* types_out, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt** stmt = _stmt;
	sqlite3* db = (sqlite3*)backend_db;

	(void)types_in;
	(void)types_out;

	if (G_UNLIKELY(sqlite3_prepare_v3(db, sql, -1, SQLITE_PREPARE_PERSISTENT, stmt, NULL) != SQLITE_OK))
	{
		g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_PREPARE, "sql prepare failed error was <%s> '%s'", sql, sqlite3_errmsg(db));
		goto _error;
	}

	return TRUE;

_error:
	j_sql_finalize(backend_db, *stmt, NULL);

	return FALSE;
}

static gboolean
j_sql_bind_null(gpointer backend_db, void* _stmt, guint idx, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt = _stmt;

	if (G_UNLIKELY(sqlite3_bind_null(stmt, idx) != SQLITE_OK))
	{
		g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg((sqlite3*)backend_db));
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_sql_column(gpointer backend_db, void* _stmt, guint idx, JDBType type, JDBTypeValue* value, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt = _stmt;

	(void)backend_db;

	memset(value, 0, sizeof(*value));

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
		case J_DB_TYPE_ID:
		default:
			g_set_error_literal(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_INVALID_TYPE, "sql invalid type");
			goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_sql_bind_value(gpointer backend_db, void* _stmt, guint idx, JDBType type, JDBTypeValue* value, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt = _stmt;
	sqlite3* db = (sqlite3*)backend_db;

	switch (type)
	{
		case J_DB_TYPE_SINT32:
			if (G_UNLIKELY(sqlite3_bind_int64(stmt, idx, value->val_sint32) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_UINT32:
			if (G_UNLIKELY(sqlite3_bind_int64(stmt, idx, value->val_uint32) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_SINT64:
			if (G_UNLIKELY(sqlite3_bind_int64(stmt, idx, value->val_sint64) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_UINT64:
			if (G_UNLIKELY(sqlite3_bind_int64(stmt, idx, value->val_uint64) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_FLOAT32:
			if (G_UNLIKELY(sqlite3_bind_double(stmt, idx, (gdouble)value->val_float32) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_FLOAT64:
			if (G_UNLIKELY(sqlite3_bind_double(stmt, idx, value->val_float64) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_STRING:
			if (G_UNLIKELY(sqlite3_bind_text(stmt, idx, value->val_string, -1, NULL) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_BLOB:
			if (G_UNLIKELY(sqlite3_bind_blob(stmt, idx, value->val_blob, value->val_blob_length, NULL) != SQLITE_OK))
			{
				g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_BIND, "sql bind failed error was '%s'", sqlite3_errmsg(db));
				goto _error;
			}
			break;
		case J_DB_TYPE_ID:
		default:
			g_set_error_literal(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_INVALID_TYPE, "sql invalid type");
			goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_sql_reset(gpointer backend_db, void* _stmt, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt = _stmt;

	if (G_UNLIKELY(sqlite3_reset(stmt) != SQLITE_OK))
	{
		g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_RESET, "sql reset failed error was '%s'", sqlite3_errmsg((sqlite3*)backend_db));
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_sql_step(gpointer backend_db, void* _stmt, gboolean* found, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt = _stmt;
	guint ret;

	ret = sqlite3_step(stmt);
	*found = ret == SQLITE_ROW;

	if (ret != SQLITE_ROW)
	{
		if (G_UNLIKELY(ret == SQLITE_CONSTRAINT))
		{
			g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_CONSTRAINT, "sql constraint failed error was '%s'", sqlite3_errmsg((sqlite3*)backend_db));
			goto _error;
		}
		else if (G_UNLIKELY(ret != SQLITE_DONE))
		{
			g_set_error(error, J_BACKEND_SQL_ERROR, J_BACKEND_SQL_ERROR_STEP, "sql step failed error was '%s'", sqlite3_errmsg((sqlite3*)backend_db));
			goto _error;
		}
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_sql_exec(gpointer backend_db, const char* sql, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_stmt* stmt;
	gboolean found;

	if (G_UNLIKELY(!j_sql_prepare(backend_db, sql, &stmt, NULL, NULL, error)))
	{
		goto _error2;
	}

	if (G_UNLIKELY(!j_sql_step(backend_db, stmt, &found, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_finalize(backend_db, stmt, error)))
	{
		goto _error2;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(!j_sql_finalize(backend_db, stmt, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}

static gboolean
j_sql_step_and_reset_check_done(gpointer backend_db, void* _stmt, GError** error)
{
	J_TRACE_FUNCTION(NULL);
	gboolean sql_found;

	if (G_UNLIKELY(!j_sql_step(backend_db, _stmt, &sql_found, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_sql_reset(backend_db, _stmt, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	if (G_UNLIKELY(!j_sql_reset(backend_db, _stmt, NULL)))
	{
		goto _error2;
	}

	return FALSE;

_error2:
	/*something failed very hard*/
	return FALSE;
}

static void*
j_sql_open(gpointer backend_data)
{
	J_TRACE_FUNCTION(NULL);

	JSQLiteData* bd = backend_data;

	sqlite3* backend_db = NULL;
	g_autofree gchar* dirname = NULL;

	g_return_val_if_fail(bd->path != NULL, FALSE);

	if (g_strcmp0(bd->path, ":memory:") != 0)
	{
		dirname = g_path_get_dirname(bd->path);
		g_mkdir_with_parents(dirname, 0700);
		if (G_UNLIKELY(sqlite3_open(bd->path, &backend_db) != SQLITE_OK))
		{
			goto _error;
		}
	}
	else
	{
		// The shared cache is disabled when SQLITE_OPEN_URI is not set.
		// Make sure it is by specifying it in the flags and using the file: prefix.
		if (G_UNLIKELY(sqlite3_open_v2("file:julea-db", &backend_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_MEMORY | SQLITE_OPEN_SHAREDCACHE, NULL) != SQLITE_OK))
		{
			goto _error;
		}
	}
	if (G_UNLIKELY(!j_sql_exec(backend_db, "PRAGMA foreign_keys = ON", NULL)))
	{
		goto _error;
	}

	return backend_db;

_error:
	sqlite3_close(backend_db);

	return NULL;
}

static void
j_sql_close(gpointer backend_db)
{
	J_TRACE_FUNCTION(NULL);

	sqlite3_close((sqlite3*)backend_db);
}

static gboolean
j_sql_start_transaction(gpointer backend_db, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	return j_sql_exec(backend_db, "BEGIN TRANSACTION", error);
}

static gboolean
j_sql_commit_transaction(gpointer backend_db, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	return j_sql_exec(backend_db, "COMMIT", error);
}

static gboolean
j_sql_abort_transaction(gpointer backend_db, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	return j_sql_exec(backend_db, "ROLLBACK", error);
}

static JSQLSpecifics specifics = {
	/*
	* sqlite supports multithread, but only for concurrent read. concurrent write requires manual retrys
	* to remove errors due to concurrent access.
	* if SQL_MODE is defined as SQL_MODE_SINGLE_THREAD, the sqlite-generic code uses a global lock to prevent concurrency errors.
	* otherwise there is no lock
	*/
	.single_threaded = TRUE,
	.backend_data = NULL,

	.func = {
		.connection_open = j_sql_open,
		.connection_close = j_sql_close,
		.transaction_start = j_sql_start_transaction,
		.transaction_commit = j_sql_commit_transaction,
		.transaction_abort = j_sql_abort_transaction,
		.statement_prepare = j_sql_prepare,
		.statement_finalize = j_sql_finalize,
		.statement_bind_null = j_sql_bind_null,
		.statement_bind_value = j_sql_bind_value,
		.statement_step = j_sql_step,
		.statement_step_and_reset_check_done = j_sql_step_and_reset_check_done,
		.statement_reset = j_sql_reset,
		.statement_column = j_sql_column,
		.sql_exec = j_sql_exec,
	},

	.sql = {
		.autoincrement = " ",
		.uint64_type = " UNSIGNED BIGINT ",
		.id_type = " INTEGER ",
		.select_last = " SELECT last_insert_rowid() ",
		.quote = "\"",
	},

};

static gboolean
backend_init(gchar const* _path, gpointer* backend_data)
{
	J_TRACE_FUNCTION(NULL);

	JSQLiteData* bd;

	bd = g_slice_new(JSQLiteData);
	bd->path = g_strdup(_path);
	bd->db = NULL;

	if (g_strcmp0(bd->path, ":memory:") == 0)
	{
		// Hold an extra reference to the shared in-memory database to make sure it is not freed.
		sqlite3_open_v2("file:julea-db", &(bd->db), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_MEMORY | SQLITE_OPEN_SHAREDCACHE, NULL);
	}

	*backend_data = bd;

	specifics.backend_data = bd;

	sql_generic_init(&specifics);

	return TRUE;
}

static void
backend_fini(gpointer backend_data)
{
	J_TRACE_FUNCTION(NULL);

	JSQLiteData* bd = backend_data;

	sql_generic_fini();

	if (bd->db != NULL)
	{
		sqlite3_close(bd->db);
	}

	g_free(bd->path);
	g_slice_free(JSQLiteData, bd);
}

static JBackend sqlite_backend = {
	.type = J_BACKEND_TYPE_DB,
	.component = J_BACKEND_COMPONENT_SERVER,
	.db = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_schema_create = sql_generic_schema_create,
		.backend_schema_get = sql_generic_schema_get,
		.backend_schema_delete = sql_generic_schema_delete,
		.backend_insert = sql_generic_insert,
		.backend_update = sql_generic_update,
		.backend_delete = sql_generic_delete,
		.backend_query = sql_generic_query,
		.backend_iterate = sql_generic_iterate,
		.backend_batch_start = sql_generic_batch_start,
		.backend_batch_execute = sql_generic_batch_execute,
	},
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	J_TRACE_FUNCTION(NULL);

	return &sqlite_backend;
}
