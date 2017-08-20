/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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
	JSemanticsSafety safety;
};

typedef struct JLMDBBatch JLMDBBatch;

struct JLMDBIterator
{
	MDB_cursor* cursor;
	MDB_txn* txn;
	gboolean first;
	gchar* prefix;
};

typedef struct JLMDBIterator JLMDBIterator;

static MDB_env* backend_env = NULL;
static MDB_dbi backend_dbi;

static
gboolean
backend_batch_start (gchar const* namespace, JSemanticsSafety safety, gpointer* data)
{
	JLMDBBatch* batch;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	batch = g_slice_new(JLMDBBatch);

	batch->namespace = g_strdup(namespace);
	batch->safety = safety;

	if (mdb_txn_begin(backend_env, NULL, 0, &(batch->txn)) != 0)
	{
		// FIXME
	}

	*data = batch;

	return (batch != NULL);
}

static
gboolean
backend_batch_execute (gpointer data)
{
	JLMDBBatch* batch = data;

	g_return_val_if_fail(data != NULL, FALSE);

	if (batch->safety == J_SEMANTICS_SAFETY_STORAGE)
	{
	}

	if (mdb_txn_commit(batch->txn) != 0)
	{
		// FIXME
	}

	g_free(batch->namespace);
	g_slice_free(JLMDBBatch, batch);

	// FIXME
	return TRUE;
}

static
gboolean
backend_put (gpointer data, gchar const* key, bson_t const* value)
{
	JLMDBBatch* batch = data;
	MDB_val m_key;
	MDB_val m_value;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	m_key.mv_size = strlen(nskey) + 1;
	m_key.mv_data = nskey;
	m_value.mv_size = value->len;
	m_value.mv_data = (gchar*)bson_get_data(value);

	mdb_put(batch->txn, backend_dbi, &m_key, &m_value, 0);

	// FIXME
	return TRUE;
}

static
gboolean
backend_delete (gpointer data, gchar const* key)
{
	JLMDBBatch* batch = data;
	MDB_val m_key;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	m_key.mv_size = strlen(nskey) + 1;
	m_key.mv_data = nskey;

	mdb_del(batch->txn, backend_dbi, &m_key, NULL);

	// FIXME
	return TRUE;
}

static
gboolean
backend_get (gchar const* namespace, gchar const* key, bson_t* result_out)
{
	MDB_txn* txn;
	MDB_val m_key;
	MDB_val m_value;
	g_autofree gchar* nskey = NULL;
	gint ret;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

	// FIXME
	mdb_txn_begin(backend_env, NULL, 0, &txn);

	nskey = g_strdup_printf("%s:%s", namespace, key);

	m_key.mv_size = strlen(nskey) + 1;
	m_key.mv_data = nskey;

	ret = mdb_get(txn, backend_dbi, &m_key, &m_value);

	if (ret == 0)
	{
		bson_t tmp[1];

		// FIXME check whether copies can be avoided
		bson_init_static(tmp, m_value.mv_data, m_value.mv_size);
		bson_copy_to(tmp, result_out);
	}

	// FIXME
	mdb_txn_commit(txn);

	return (ret == 0);
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

	mdb_txn_begin(backend_env, NULL, 0, &(iterator->txn));
	mdb_cursor_open(iterator->txn, backend_dbi, &(iterator->cursor));

	*data = iterator;

	return (iterator != NULL);
}

static
gboolean
backend_iterate (gpointer data, bson_t* result_out)
{
	JLMDBIterator* iterator = data;
	MDB_cursor_op cursor_op = MDB_NEXT;
	MDB_val m_key;
	MDB_val m_value;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

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

		bson_init_static(result_out, m_value.mv_data, m_value.mv_size);

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
		if (mdb_env_open(backend_env, path, 0, 0600) != 0)
		{
			mdb_env_close(backend_env);
			backend_env = NULL;
			goto end;
		}

		if (mdb_txn_begin(backend_env, NULL, 0, &txn) != 0)
		{
			// FIXME
		}

		if (mdb_dbi_open(txn, NULL, 0, &backend_dbi) != 0)
		{
			// FIXME
		}

		if (mdb_txn_commit(txn) != 0)
		{
			// FIXME
		}
	}

end:
	return (backend_env != NULL);
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
	.type = J_BACKEND_TYPE_META,
	.meta = {
		.init = backend_init,
		.fini = backend_fini,
		.batch_start = backend_batch_start,
		.batch_execute = backend_batch_execute,
		.put = backend_put,
		.delete = backend_delete,
		.get = backend_get,
		.get_all = backend_get_all,
		.get_by_prefix = backend_get_by_prefix,
		.iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	if (type == J_BACKEND_TYPE_META)
	{
		backend = &lmdb_backend;
	}

	return backend;
}
