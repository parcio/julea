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

// FIXME clean up

const char* const JuleaBackendErrorFormat[] = {
	"Generic Backend Error%s",
	"%s:%s: batch not set%s",
	"%s:%s: bson append failed. db type was '%s'",
	"%s:%s: bson generic error",
	"%s:%s: bson invalid%s",
	"%s:%s: bson invalid type '%d'",
	"%s:%s: bson iter init failed%s",
	"%s:%s: bson iter recourse failed%s",
	"%s:%s: bson is missing required key '%s'",
	"%s:%s: db comparator invalid '%d'",
	"%s:%s: db invalid type '%d'",
	"%s:%s: no more elements to iterate%s",
	"%s:%s: iterator not set%s",
	"%s:%s: metadata is empty%s",
	"%s:%s: metadata not set%s",
	"%s:%s: name not set%s",
	"%s:%s: namespace not set%s",
	"%s:%s: no variable set to a not NULL value%s",
	"%s:%s: db operator invalid '%d'",
	"%s:%s: schema is empty%s",
	"%s:%s: schema not found%s",
	"%s:%s: schema not set%s",
	"%s:%s: selector is empty%s",
	"%s:%s: selector not set%s",
	"%s:%s: sql constraint error '%d' '%s'",
	"%s:%s: sql error '%d' '%s'",
	"%s:%s: some other thread modified critical variables without lock%s",
	"%s:%s: variable '%s' not defined in schema"
};

#ifdef JULEA_DEBUG
#define J_WARNING(format, ...)                                                   \
	do                                                                       \
	{                                                                        \
		g_warning("%s:%s: " format, G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
	} while (0)
#define J_INFO(format, ...)                                                   \
	do                                                                    \
	{                                                                     \
		g_info("%s:%s: " format, G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
	} while (0)
#define J_DEBUG(format, ...)                                                   \
	do                                                                     \
	{                                                                      \
		g_debug("%s:%s: " format, G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
	} while (0)
#define J_DEBUG_ERROR(format, ...)                                   \
	do                                                           \
	{                                                            \
		g_debug(format, G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
	} while (0)
#else
#define J_WARNING(format, ...)
#define J_INFO(format, ...)
#define J_DEBUG(format, ...)
#define J_DEBUG_ERROR(format, ...)
#endif

#define j_goto_error_backend(val, err_code, ...)                                                                                                  \
	do                                                                                                                                        \
	{                                                                                                                                         \
		_Pragma("GCC diagnostic push");                                                                                                   \
		_Pragma("GCC diagnostic ignored \"-Wformat-nonliteral\"");                                                                        \
		if (val)                                                                                                                          \
		{                                                                                                                                 \
			J_DEBUG_ERROR(JuleaBackendErrorFormat[err_code], ##__VA_ARGS__);                                                          \
			g_set_error(error, J_BACKEND_DB_ERROR, err_code, JuleaBackendErrorFormat[err_code], G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
			goto _error;                                                                                                              \
		}                                                                                                                                 \
		_Pragma("GCC diagnostic pop");                                                                                                    \
	} while (0)

#define j_goto_error_subcommand(val) \
	do                           \
	{                            \
		if (val)             \
			goto _error; \
	} while (0)

/*
* this file defines makros such that the generic-sql.h implementation can call the sqlite specific functions without knowing, that it is actually sqlite
*/

#define j_sql_statement_type sqlite3_stmt*
#define j_sql_done SQLITE_DONE
static sqlite3* backend_db = NULL;
#ifdef JULEA_DEBUG
#define j_sql_check(ret, flag)                                                              \
	do                                                                                  \
	{                                                                                   \
		if (ret != flag)                                                            \
		{                                                                           \
			J_CRITICAL("sql error '%d' '%s'", ret, sqlite3_errmsg(backend_db)); \
			abort();                                                            \
		}                                                                           \
	} while (0)
#else
#define j_sql_check(ret, flag)       \
	do                           \
	{                            \
		if (ret != flag)     \
			goto _error; \
	} while (0)
#endif
#define j_sql_constraint_check(ret, flag)                                                                                  \
	do                                                                                                                 \
	{                                                                                                                  \
		if (ret == SQLITE_CONSTRAINT)                                                                              \
			j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_SQL_CONSTRAINT, _ret_, sqlite3_errmsg(backend_db)); \
		else                                                                                                       \
			j_sql_check(ret, flag);                                                                            \
	} while (0)
#define j_sql_reset(stmt)                         \
	do                                        \
	{                                         \
		gint _ret_ = sqlite3_reset(stmt); \
		j_sql_check(_ret_, SQLITE_OK);    \
	} while (0)
#define j_sql_reset_constraint(stmt)                      \
	do                                                \
	{                                                 \
		gint _ret_ = sqlite3_reset(stmt);         \
		j_sql_constraint_check(_ret_, SQLITE_OK); \
	} while (0)
#define j_sql_step_and_reset_check_done(stmt)    \
	do                                       \
	{                                        \
		gint _ret_ = sqlite3_step(stmt); \
		j_sql_check(_ret_, SQLITE_DONE); \
		_ret_ = sqlite3_reset(stmt);     \
		j_sql_check(_ret_, SQLITE_OK);   \
	} while (0)
#define j_sql_step_and_reset_check_done_constraint(stmt)    \
	do                                                  \
	{                                                   \
		gint _ret_ = sqlite3_step(stmt);            \
		gint _ret2_ = sqlite3_reset(stmt);          \
		j_sql_constraint_check(_ret_, SQLITE_DONE); \
		j_sql_constraint_check(_ret2_, SQLITE_OK);  \
	} while (0)
#define j_sql_bind_null(stmt, idx)                         \
	do                                                 \
	{                                                  \
		gint _ret_ = sqlite3_bind_null(stmt, idx); \
		j_sql_check(_ret_, SQLITE_OK);             \
	} while (0)
#define j_sql_bind_int64(stmt, idx, val)                         \
	do                                                       \
	{                                                        \
		gint _ret_ = sqlite3_bind_int64(stmt, idx, val); \
		j_sql_check(_ret_, SQLITE_OK);                   \
	} while (0)
#define j_sql_bind_int(stmt, idx, val)                         \
	do                                                     \
	{                                                      \
		gint _ret_ = sqlite3_bind_int(stmt, idx, val); \
		j_sql_check(_ret_, SQLITE_OK);                 \
	} while (0)
#define j_sql_bind_blob(stmt, idx, val, val_len)                               \
	do                                                                     \
	{                                                                      \
		gint _ret_ = sqlite3_bind_blob(stmt, idx, val, val_len, NULL); \
		j_sql_check(_ret_, SQLITE_OK);                                 \
	} while (0)
#define j_sql_bind_double(stmt, idx, val)                         \
	do                                                        \
	{                                                         \
		gint _ret_ = sqlite3_bind_double(stmt, idx, val); \
		j_sql_check(_ret_, SQLITE_OK);                    \
	} while (0)
#define j_sql_bind_text(stmt, idx, val, val_len)                               \
	do                                                                     \
	{                                                                      \
		gint _ret_ = sqlite3_bind_text(stmt, idx, val, val_len, NULL); \
		j_sql_check(_ret_, SQLITE_OK);                                 \
	} while (0)
#define j_sql_prepare(sql, stmt)                                                                             \
	do                                                                                                   \
	{                                                                                                    \
		gint _ret_ = sqlite3_prepare_v3(backend_db, sql, -1, SQLITE_PREPARE_PERSISTENT, stmt, NULL); \
		J_DEBUG("PREPARE SQL '%s'", sql);                                                            \
		j_sql_check(_ret_, SQLITE_OK);                                                               \
	} while (0)
#define j_sql_finalize(stmt)                           \
	do                                             \
	{                                              \
		gint __ret__ = sqlite3_finalize(stmt); \
		j_sql_check(__ret__, SQLITE_OK);       \
	} while (0)
#define j_sql_loop(stmt, ret)                                 \
	while (1)                                             \
		if ((ret = sqlite3_step(stmt)) != SQLITE_ROW) \
		{                                             \
			j_sql_check(ret, SQLITE_DONE);        \
			break;                                \
		}                                             \
		else
#define j_sql_step(stmt, ret)                         \
	if ((ret = sqlite3_step(stmt)) != SQLITE_ROW) \
		j_sql_check(ret, SQLITE_DONE);        \
	else
#define j_sql_step_constraint(stmt, ret)                  \
	if ((ret = sqlite3_step(stmt)) != SQLITE_ROW)     \
		j_sql_check_constraint(ret, SQLITE_DONE); \
	else
#define j_sql_exec_or_error(sql, flag)                                                                                 \
	do                                                                                                             \
	{                                                                                                              \
		j_sql_statement_type _stmt_;                                                                           \
		gint _ret_ = sqlite3_prepare_v3(backend_db, sql, -1, 0, &_stmt_, NULL);                                \
		if (_ret_ != SQLITE_OK)                                                                                \
		{                                                                                                      \
			j_sql_finalize(_stmt_);                                                                        \
			j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_SQL_FAILED, _ret_, sqlite3_errmsg(backend_db)); \
		}                                                                                                      \
		_ret_ = sqlite3_step(_stmt_);                                                                          \
		if (_ret_ != SQLITE_DONE)                                                                              \
		{                                                                                                      \
			j_sql_finalize(_stmt_);                                                                        \
			j_goto_error_backend(TRUE, J_BACKEND_DB_ERROR_SQL_FAILED, _ret_, sqlite3_errmsg(backend_db)); \
		}                                                                                                      \
		j_sql_finalize(_stmt_);                                                                                \
	} while (0)
#define j_sql_exec_and_get_number(sql, number)                     \
	do                                                         \
	{                                                          \
		j_sql_statement_type _stmt0_;                      \
		gint _ret0_;                                       \
		j_sql_prepare(sql, &_stmt0_);                      \
		number = 0;                                        \
		_ret0_ = sqlite3_step(_stmt0_);                    \
		if (_ret0_ == SQLITE_ROW)                          \
			number = sqlite3_column_int64(_stmt0_, 0); \
		else                                               \
			j_sql_check(_ret0_, SQLITE_DONE);          \
		j_sql_finalize(_stmt0_);                           \
	} while (0)
#define j_sql_column_float32(stmt, idx) (gfloat) sqlite3_column_double(stmt, idx)
#define j_sql_column_float64(stmt, idx) (gdouble) sqlite3_column_double(stmt, idx)
#define j_sql_column_sint32(stmt, idx) (gint32) sqlite3_column_int(stmt, idx)
#define j_sql_column_sint64(stmt, idx) (gint64) sqlite3_column_int64(stmt, idx)
#define j_sql_column_text(stmt, idx) (const char*)sqlite3_column_text(stmt, idx)
#define j_sql_column_uint32(stmt, idx) (guint32) sqlite3_column_int(stmt, idx)
#define j_sql_column_uint64(stmt, idx) (guint64) sqlite3_column_int64(stmt, idx)
#define j_sql_column_blob(stmt, idx) (const char*)sqlite3_column_blob(stmt, idx)
#define j_sql_column_blob_len(stmt, idx) (guint64) sqlite3_column_bytes(stmt, idx)

#include "sql-generic.c"

static gboolean
backend_init(gchar const* path)
{
	GError** error = NULL;
	guint ret;
	g_autofree gchar* dirname = NULL;
	g_return_val_if_fail(path != NULL, FALSE);
	if (strncmp(":memory:", path, 7))
	{
		J_DEBUG("init useing path=%s", path);
		dirname = g_path_get_dirname(path);
		g_mkdir_with_parents(dirname, 0700);
		ret = sqlite3_open(path, &backend_db);
		j_sql_check(ret, SQLITE_OK);
	}
	else
	{
		J_DEBUG("init useing path=%s", ":memory:");
		ret = sqlite3_open(":memory:", &backend_db);
		j_sql_check(ret, SQLITE_OK);
	}
	j_sql_exec_or_error("PRAGMA foreign_keys = ON", SQLITE_DONE);
	ret = init_sql();
	j_goto_error_subcommand(!ret);
	return (backend_db != NULL);
_error:
	sqlite3_close(backend_db);
	return FALSE;
}
static void
backend_fini(void)
{
	gint ret;
	fini_sql();
	ret = sqlite3_close(backend_db);
	j_sql_check(ret, SQLITE_OK);
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
