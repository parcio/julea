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

#include <lmdb.h>

#include <julea.h>

struct JLMDBBatch
{
	MDB_txn* txn;
	gchar* namespace;
	JSemantics* semantics;
};

typedef struct JLMDBBatch JLMDBBatch;

struct JLMDBIterator
{
	MDB_cursor* cursor;
	MDB_txn* txn;
	gboolean first;
	gchar* prefix;
	gsize namespace_len;
};

typedef struct JLMDBIterator JLMDBIterator;

static MDB_env* backend_env = NULL;
static MDB_dbi backend_dbi;

static
gboolean
backend_batch_start (gchar const* namespace, JSemantics* semantics, gpointer* data)
{
	JLMDBBatch* batch = NULL;
	MDB_txn* txn;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	if (mdb_txn_begin(backend_env, NULL, 0, &txn) == 0)
	{
		batch = g_slice_new(JLMDBBatch);
		batch->txn = txn;
		batch->namespace = g_strdup(namespace);
		batch->semantics = j_semantics_ref(semantics);
	}

	*data = batch;

	return (batch != NULL);
}

static
gboolean
backend_batch_execute (gpointer data)
{
	gboolean ret = FALSE;

	JLMDBBatch* batch = data;

	g_return_val_if_fail(data != NULL, FALSE);

	// FIXME do something with batch->semantics

	if (mdb_txn_commit(batch->txn) == 0)
	{
		ret = TRUE;
	}

	// FIXME free txn

	j_semantics_unref(batch->semantics);
	g_free(batch->namespace);
	g_slice_free(JLMDBBatch, batch);

	return ret;
}

static
gboolean
backend_put (gpointer data, gchar const* key, gconstpointer value, guint32 len)
{
	JLMDBBatch* batch = data;
	MDB_val m_key;
	MDB_val m_value;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	m_key.mv_size = strlen(nskey) + 1;
	m_key.mv_data = nskey;
	m_value.mv_size = len;
	m_value.mv_data = value;

	return (mdb_put(batch->txn, backend_dbi, &m_key, &m_value, 0) == 0);
}

static
gboolean
backend_delete (gpointer data, gchar const* key)
{
	JLMDBBatch* batch = data;
	MDB_val m_key;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	m_key.mv_size = strlen(nskey) + 1;
	m_key.mv_data = nskey;

	return (mdb_del(batch->txn, backend_dbi, &m_key, NULL) == 0);
}

static
gboolean
backend_get (gpointer data, gchar const* key, gpointer* value, guint32* len)
{
	gboolean ret = FALSE;

	JLMDBBatch* batch = data;
	MDB_val m_key;
	MDB_val m_value;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	m_key.mv_size = strlen(nskey) + 1;
	m_key.mv_data = nskey;

	if (mdb_get(batch->txn, backend_dbi, &m_key, &m_value) == 0)
	{
		// FIXME check whether copies can be avoided
		*value = g_memdup(m_value.mv_data, m_value.mv_size);
		*len = m_value.mv_size;

		ret = TRUE;
	}

	return ret;
}

static
gboolean
backend_get_all (gchar const* namespace, gpointer* data)
{
	JLMDBIterator* iterator = NULL;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	iterator = g_slice_new(JLMDBIterator);
	iterator->first = TRUE;
	iterator->prefix = g_strdup_printf("%s:", namespace);
	iterator->namespace_len = strlen(namespace) + 1;

	mdb_txn_begin(backend_env, NULL, 0, &(iterator->txn));
	mdb_cursor_open(iterator->txn, backend_dbi, &(iterator->cursor));

	*data = iterator;

	return (iterator != NULL);
}

static
gboolean
backend_get_by_prefix (gchar const* namespace, gchar const* prefix, gpointer* data)
{
	JLMDBIterator* iterator = NULL;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	iterator = g_slice_new(JLMDBIterator);
	iterator->first = TRUE;
	iterator->prefix = g_strdup_printf("%s:%s", namespace, prefix);
	iterator->namespace_len = strlen(namespace) + 1;

	mdb_txn_begin(backend_env, NULL, 0, &(iterator->txn));
	mdb_cursor_open(iterator->txn, backend_dbi, &(iterator->cursor));

	*data = iterator;

	return (iterator != NULL);
}

static
gboolean
backend_iterate (gpointer data, gchar const** key, gconstpointer* value, guint32* len)
{
	JLMDBIterator* iterator = data;
	MDB_cursor_op cursor_op = MDB_NEXT;
	MDB_val m_key;
	MDB_val m_value;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	if (iterator->first)
	{
		// FIXME check +1
		m_key.mv_size = strlen(iterator->prefix) + 1;
		m_key.mv_data = iterator->prefix;

		cursor_op = MDB_SET_RANGE;

		iterator->first = FALSE;
	}

	if (mdb_cursor_get(iterator->cursor, &m_key, &m_value, cursor_op) == 0)
	{
		if (!g_str_has_prefix(m_key.mv_data, iterator->prefix))
		{
			// FIXME check whether we can completely terminate
			goto out;
		}

		*key = (gchar const*)m_key.mv_data + iterator->namespace_len;
		*value = m_value.mv_data;
		*len = m_value.mv_size;

		return TRUE;
	}

out:
	mdb_txn_commit(iterator->txn);

	g_free(iterator->prefix);
	g_slice_free(JLMDBIterator, iterator);

	return FALSE;
}

static
gboolean
backend_init (gchar const* path)
{
	MDB_txn* txn;

	g_return_val_if_fail(path != NULL, FALSE);

	g_mkdir_with_parents(path, 0700);

	if (mdb_env_create(&backend_env) == 0)
	{
		// FIXME grow mapsize dynamically (default is 10 MiB)
		if (mdb_env_set_mapsize(backend_env, (gsize)4 * 1024 * 1024 * 1024) != 0)
		{
			goto error;
		}

		if (mdb_env_open(backend_env, path, 0, 0600) != 0)
		{
			goto error;
		}

		if (mdb_txn_begin(backend_env, NULL, 0, &txn) != 0)
		{
			goto error;
		}

		if (mdb_dbi_open(txn, NULL, 0, &backend_dbi) != 0)
		{
			goto error;
		}

		if (mdb_txn_commit(txn) != 0)
		{
			goto error;
		}
	}

	return (backend_env != NULL);

error:
	mdb_env_close(backend_env);

	return FALSE;
}

static
void
backend_fini (void)
{
	if (backend_env != NULL)
	{
		mdb_env_close(backend_env);
	}
}

static
JBackend lmdb_backend = {
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
	return &lmdb_backend;
}
