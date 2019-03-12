/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
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

#define _POSIX_C_SOURCE 200809L

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <sqlite3.h>

#include <julea.h>

struct JSQLiteBatch
{
	gchar* namespace;
	JSemanticsSafety safety;
};

typedef struct JSQLiteBatch JSQLiteBatch;

static sqlite3* backend_db = NULL;

static
gboolean
backend_batch_start (gchar const* namespace, JSemanticsSafety safety, gpointer* data)
{
	JSQLiteBatch* batch = NULL;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	if (sqlite3_exec(backend_db, "BEGIN TRANSACTION;", NULL, NULL, NULL) == SQLITE_OK)
	{
		batch = g_slice_new(JSQLiteBatch);

		batch->namespace = g_strdup(namespace);
		batch->safety = safety;
	}

	*data = batch;

	return (batch != NULL);
}

static
gboolean
backend_batch_execute (gpointer data)
{
	gboolean ret = FALSE;

	JSQLiteBatch* batch = data;

	g_return_val_if_fail(data != NULL, FALSE);

	// FIXME do something with batch->safety

	if (sqlite3_exec(backend_db, "COMMIT;", NULL, NULL, NULL) == SQLITE_OK)
	{
		ret = TRUE;
	}

	g_free(batch->namespace);
	g_slice_free(JSQLiteBatch, batch);

	return ret;
}

static
gboolean
backend_put (gpointer data, gchar const* key, bson_t const* value)
{
	JSQLiteBatch* batch = data;
	sqlite3_stmt* stmt;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	sqlite3_prepare_v2(backend_db, "INSERT INTO julea (namespace, key, value) VALUES (?, ?, ?);", -1, &stmt, NULL);
	sqlite3_bind_text(stmt, 1, batch->namespace, -1, NULL);
	sqlite3_bind_text(stmt, 2, key, -1, NULL);
	sqlite3_bind_blob(stmt, 3, bson_get_data(value), value->len, NULL);

	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	// FIXME
	return TRUE;
}

static
gboolean
backend_delete (gpointer data, gchar const* key)
{
	JSQLiteBatch* batch = data;
	sqlite3_stmt* stmt;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	sqlite3_prepare_v2(backend_db, "DELETE FROM julea WHERE namespace = ? AND key = ?;", -1, &stmt, NULL);
	sqlite3_bind_text(stmt, 1, batch->namespace, -1, NULL);
	sqlite3_bind_text(stmt, 2, key, -1, NULL);

	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	// FIXME
	return TRUE;
}

static
gboolean
backend_get (gchar const* namespace, gchar const* key, bson_t* result_out)
{
	sqlite3_stmt* stmt;
	g_autofree gchar* nskey = NULL;
	gint ret;
	gconstpointer result = NULL;
	gsize result_len;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

	sqlite3_prepare_v2(backend_db, "SELECT value FROM julea WHERE namespace = ? AND key = ?;", -1, &stmt, NULL);
	sqlite3_bind_text(stmt, 1, namespace, -1, NULL);
	sqlite3_bind_text(stmt, 2, key, -1, NULL);

	ret = sqlite3_step(stmt);

	if (ret == SQLITE_ROW)
	{
		bson_t tmp[1];

		result = sqlite3_column_blob(stmt, 0);
		result_len = sqlite3_column_bytes(stmt, 0);

		// FIXME check whether copies can be avoided
		bson_init_static(tmp, result, result_len);
		bson_copy_to(tmp, result_out);
	}

	sqlite3_finalize(stmt);

	return (result != NULL);
}

static
gboolean
backend_get_all (gchar const* namespace, gpointer* data)
{
	sqlite3_stmt* stmt = NULL;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	if (sqlite3_prepare_v2(backend_db, "SELECT key, value FROM julea WHERE namespace = ?;", -1, &stmt, NULL) == SQLITE_OK)
	{
		sqlite3_bind_text(stmt, 1, namespace, -1, NULL);
	}

	*data = stmt;

	return (stmt != NULL);
}

static
gboolean
backend_get_by_prefix (gchar const* namespace, gchar const* prefix, gpointer* data)
{
	sqlite3_stmt* stmt = NULL;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	if (sqlite3_prepare_v2(backend_db, "SELECT key, value FROM julea WHERE namespace = ? AND key LIKE ? || '%';", -1, &stmt, NULL) == SQLITE_OK)
	{
		sqlite3_bind_text(stmt, 1, namespace, -1, NULL);
		sqlite3_bind_text(stmt, 2, prefix, -1, NULL);
	}

	*data = stmt;

	return (stmt != NULL);
}

static
gboolean
backend_iterate (gpointer data, bson_t* result_out)
{
	sqlite3_stmt* stmt = data;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

	if (sqlite3_step(stmt) == SQLITE_ROW)
	{
		guchar const* key;
		gconstpointer value;
		gsize len;

		key = sqlite3_column_text(stmt, 0);
		value = sqlite3_column_blob(stmt, 1);
		len = sqlite3_column_bytes(stmt, 1);

		(void)key;

		bson_init_static(result_out, value, len);

		return TRUE;
	}

	sqlite3_finalize(stmt);

	return FALSE;
}

static
gboolean
backend_init (gchar const* path)
{
	g_autofree gchar* dirname = NULL;

	g_return_val_if_fail(path != NULL, FALSE);

	dirname = g_path_get_dirname(path);
	g_mkdir_with_parents(dirname, 0700);

	if (sqlite3_open(path, &backend_db) != SQLITE_OK)
	{
		goto error;
	}

	if (sqlite3_exec(backend_db, "CREATE TABLE IF NOT EXISTS julea (namespace TEXT NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL);", NULL, NULL, NULL) != SQLITE_OK)
	{
		goto error;
	}

	if (sqlite3_exec(backend_db, "CREATE UNIQUE INDEX IF NOT EXISTS julea_namespace_key ON julea (namespace, key);", NULL, NULL, NULL) != SQLITE_OK)
	{
		goto error;
	}

	return (backend_db != NULL);

error:
	sqlite3_close(backend_db);

	return FALSE;
}

static
void
backend_fini (void)
{
	if (backend_db != NULL)
	{
		sqlite3_close(backend_db);
	}
}

static
JBackend sqlite_backend = {
	.type = J_BACKEND_TYPE_KV,
	.component = J_BACKEND_COMPONENT_SERVER,
	.kv = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_batch_start = backend_batch_start,
		.backend_batch_execute = backend_batch_execute,
		.backend_put = backend_put,
		.backend_delete = backend_delete,
		.backend_get = backend_get,
		.backend_get_all = backend_get_all,
		.backend_get_by_prefix = backend_get_by_prefix,
		.backend_iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &sqlite_backend;
}
