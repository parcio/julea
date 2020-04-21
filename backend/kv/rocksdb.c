/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2020 Michael Kuhn
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

#include <rocksdb/c.h>

#include <julea.h>

struct JRocksDBBatch
{
	rocksdb_writebatch_t* batch;
	gchar* namespace;
	JSemantics* semantics;
};

typedef struct JRocksDBBatch JRocksDBBatch;

struct JRocksDBData
{
	rocksdb_t* db;

	rocksdb_readoptions_t* read_options;
	rocksdb_writeoptions_t* write_options;
	rocksdb_writeoptions_t* write_options_sync;
};

typedef struct JRocksDBData JRocksDBData;

struct JRocksDBIterator
{
	rocksdb_iterator_t* iterator;
	gboolean first;
	gchar* prefix;
	gsize namespace_len;
};

typedef struct JRocksDBIterator JRocksDBIterator;

static gboolean
backend_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* backend_batch)
{
	JRocksDBBatch* batch;

	(void)backend_data;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(backend_batch != NULL, FALSE);

	batch = g_slice_new(JRocksDBBatch);

	batch->batch = rocksdb_writebatch_create();
	batch->namespace = g_strdup(namespace);
	batch->semantics = j_semantics_ref(semantics);

	*backend_batch = batch;

	return (batch != NULL);
}

static gboolean
backend_batch_execute(gpointer backend_data, gpointer backend_batch)
{
	JRocksDBBatch* batch = backend_batch;
	JRocksDBData* bd = backend_data;

	g_autofree gchar* rocksdb_error = NULL;

	rocksdb_writeoptions_t* write_options = bd->write_options;

	g_return_val_if_fail(backend_batch != NULL, FALSE);

	if (j_semantics_get(batch->semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_STORAGE)
	{
		write_options = bd->write_options_sync;
	}

	rocksdb_write(bd->db, write_options, batch->batch, &rocksdb_error);

	j_semantics_unref(batch->semantics);
	g_free(batch->namespace);
	rocksdb_writebatch_destroy(batch->batch);
	g_slice_free(JRocksDBBatch, batch);

	return (rocksdb_error == NULL);
}

static gboolean
backend_put(gpointer backend_data, gpointer backend_batch, gchar const* key, gconstpointer value, guint32 len)
{
	JRocksDBBatch* batch = backend_batch;
	g_autofree gchar* nskey = NULL;

	(void)backend_data;

	g_return_val_if_fail(backend_batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);
	rocksdb_writebatch_put(batch->batch, nskey, strlen(nskey) + 1, value, len);

	return TRUE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer backend_batch, gchar const* key)
{
	JRocksDBBatch* batch = backend_batch;
	g_autofree gchar* nskey = NULL;

	(void)backend_data;

	g_return_val_if_fail(backend_batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);
	rocksdb_writebatch_delete(batch->batch, nskey, strlen(nskey) + 1);

	return TRUE;
}

static gboolean
backend_get(gpointer backend_data, gpointer backend_batch, gchar const* key, gpointer* value, guint32* len)
{
	JRocksDBBatch* batch = backend_batch;
	JRocksDBData* bd = backend_data;
	g_autofree gchar* nskey = NULL;
	g_autofree gpointer result = NULL;
	gsize result_len;

	g_return_val_if_fail(backend_batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);
	result = rocksdb_get(bd->db, bd->read_options, nskey, strlen(nskey) + 1, &result_len, NULL);

	if (result != NULL)
	{
		*value = g_memdup(result, result_len);
		*len = result_len;
	}

	return (result != NULL);
}

static gboolean
backend_get_all(gpointer backend_data, gchar const* namespace, gpointer* backend_iterator)
{
	JRocksDBData* bd = backend_data;
	JRocksDBIterator* iterator = NULL;
	rocksdb_iterator_t* it;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(backend_iterator != NULL, FALSE);

	it = rocksdb_create_iterator(bd->db, bd->read_options);

	if (it != NULL)
	{
		iterator = g_slice_new(JRocksDBIterator);
		iterator->iterator = it;
		iterator->first = TRUE;
		iterator->prefix = g_strdup_printf("%s:", namespace);
		iterator->namespace_len = strlen(namespace) + 1;

		*backend_iterator = iterator;
	}

	return (iterator != NULL);
}

static gboolean
backend_get_by_prefix(gpointer backend_data, gchar const* namespace, gchar const* prefix, gpointer* backend_iterator)
{
	JRocksDBData* bd = backend_data;
	JRocksDBIterator* iterator = NULL;
	rocksdb_iterator_t* it;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(backend_iterator != NULL, FALSE);

	it = rocksdb_create_iterator(bd->db, bd->read_options);

	if (it != NULL)
	{
		iterator = g_slice_new(JRocksDBIterator);
		iterator->iterator = it;
		iterator->first = TRUE;
		iterator->prefix = g_strdup_printf("%s:%s", namespace, prefix);
		iterator->namespace_len = strlen(namespace) + 1;

		*backend_iterator = iterator;
	}

	return (iterator != NULL);
}

static gboolean
backend_iterate(gpointer backend_data, gpointer backend_iterator, gchar const** key, gconstpointer* value, guint32* len)
{
	JRocksDBIterator* iterator = backend_iterator;

	(void)backend_data;

	g_return_val_if_fail(backend_iterator != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	if (iterator->first)
	{
		rocksdb_iter_seek(iterator->iterator, iterator->prefix, strlen(iterator->prefix));
		iterator->first = FALSE;
	}
	else
	{
		rocksdb_iter_next(iterator->iterator);
	}

	if (rocksdb_iter_valid(iterator->iterator))
	{
		gchar const* key_;
		gsize tmp;

		key_ = rocksdb_iter_key(iterator->iterator, &tmp);

		if (!g_str_has_prefix(key_, iterator->prefix))
		{
			goto out;
		}

		*key = key_ + iterator->namespace_len;
		*value = rocksdb_iter_value(iterator->iterator, &tmp);
		*len = tmp;

		return TRUE;
	}

out:
	g_free(iterator->prefix);
	rocksdb_iter_destroy(iterator->iterator);
	g_slice_free(JRocksDBIterator, iterator);

	return FALSE;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	JRocksDBData* bd;
	rocksdb_options_t* options;
	g_autofree gchar* dirname = NULL;
	gint const compressions[] = { rocksdb_lz4_compression, rocksdb_snappy_compression, rocksdb_no_compression };

	g_return_val_if_fail(path != NULL, FALSE);

	dirname = g_path_get_dirname(path);
	g_mkdir_with_parents(dirname, 0700);

	bd = g_slice_new(JRocksDBData);
	bd->read_options = rocksdb_readoptions_create();
	bd->write_options = rocksdb_writeoptions_create();
	bd->write_options_sync = rocksdb_writeoptions_create();
	rocksdb_writeoptions_set_sync(bd->write_options_sync, 1);

	options = rocksdb_options_create();
	rocksdb_options_set_create_if_missing(options, 1);

	for (guint i = 0; i < G_N_ELEMENTS(compressions); i++)
	{
		g_autofree gchar* error = NULL;

		rocksdb_options_set_compression(options, compressions[i]);
		bd->db = rocksdb_open(options, path, &error);

		if (bd->db != NULL)
		{
			break;
		}
	}

	rocksdb_options_destroy(options);

	*backend_data = bd;

	return (bd->db != NULL);
}

static void
backend_fini(gpointer backend_data)
{
	JRocksDBData* bd = backend_data;

	rocksdb_readoptions_destroy(bd->read_options);
	rocksdb_writeoptions_destroy(bd->write_options);
	rocksdb_writeoptions_destroy(bd->write_options_sync);

	if (bd->db != NULL)
	{
		rocksdb_close(bd->db);
	}

	g_slice_free(JRocksDBData, bd);
}

static JBackend rocksdb_backend = {
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
		.backend_iterate = backend_iterate }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &rocksdb_backend;
}
