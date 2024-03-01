/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2024 Michael Kuhn
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

#include <gdbm.h>

#include <julea.h>

struct JGDBMBatch
{
	gchar* namespace;
	JSemantics* semantics;
};

typedef struct JGDBMBatch JGDBMBatch;

struct JGDBMData
{
	GDBM_FILE dbf;
	GMutex mutex[1];
};

typedef struct JGDBMData JGDBMData;

struct JGDBMIterator
{
	datum key;
	datum value;
	gboolean first;
	gchar* prefix;
	gsize namespace_len;
};

typedef struct JGDBMIterator JGDBMIterator;

static gboolean
backend_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* data)
{
	JGDBMBatch* batch = NULL;

	(void)backend_data;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	batch = g_new(JGDBMBatch, 1);
	batch->namespace = g_strdup(namespace);
	batch->semantics = j_semantics_ref(semantics);

	*data = batch;

	return (batch != NULL);
}

static gboolean
backend_batch_execute(gpointer backend_data, gpointer data)
{
	JGDBMBatch* batch = data;

	(void)backend_data;

	g_return_val_if_fail(data != NULL, FALSE);

	/// \todo do something with batch->semantics

	j_semantics_unref(batch->semantics);
	g_free(batch->namespace);
	g_free(batch);

	return TRUE;
}

static gboolean
backend_put(gpointer backend_data, gpointer data, gchar const* key, gconstpointer value, guint32 len)
{
	gboolean ret;

	JGDBMData* bd = backend_data;
	JGDBMBatch* batch = data;
	datum g_key;
	datum g_value;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	g_key.dptr = nskey;
	g_key.dsize = strlen(nskey) + 1;
	g_value.dptr = value;
	g_value.dsize = len;

	g_mutex_lock(bd->mutex);
	ret = (gdbm_store(bd->dbf, g_key, g_value, GDBM_REPLACE) == 0);
	g_mutex_unlock(bd->mutex);

	return ret;
}

static gboolean
backend_delete(gpointer backend_data, gpointer data, gchar const* key)
{
	gboolean ret;

	JGDBMData* bd = backend_data;
	JGDBMBatch* batch = data;
	datum g_key;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	g_key.dptr = nskey;
	g_key.dsize = strlen(nskey) + 1;

	g_mutex_lock(bd->mutex);
	ret = (gdbm_delete(bd->dbf, g_key) == 0);
	g_mutex_unlock(bd->mutex);

	return ret;
}

static gboolean
backend_get(gpointer backend_data, gpointer data, gchar const* key, gpointer* value, guint32* len)
{
	gboolean ret = FALSE;

	JGDBMData* bd = backend_data;
	JGDBMBatch* batch = data;
	datum g_key;
	datum g_value;
	g_autofree gchar* nskey = NULL;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	nskey = g_strdup_printf("%s:%s", batch->namespace, key);

	g_key.dptr = nskey;
	g_key.dsize = strlen(nskey) + 1;

	g_mutex_lock(bd->mutex);
	g_value = gdbm_fetch(bd->dbf, g_key);
	g_mutex_unlock(bd->mutex);

	if (g_value.dptr != NULL)
	{
		*value = g_value.dptr;
		*len = g_value.dsize;

		ret = TRUE;
	}

	return ret;
}

static gboolean
backend_get_all(gpointer backend_data, gchar const* namespace, gpointer* data)
{
	JGDBMIterator* iterator = NULL;

	(void)backend_data;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	iterator = g_new(JGDBMIterator, 1);
	iterator->first = TRUE;
	iterator->prefix = g_strdup_printf("%s:", namespace);
	iterator->namespace_len = strlen(namespace) + 1;
	iterator->key.dptr = NULL;
	iterator->value.dptr = NULL;

	*data = iterator;

	return (iterator != NULL);
}

static gboolean
backend_get_by_prefix(gpointer backend_data, gchar const* namespace, gchar const* prefix, gpointer* data)
{
	JGDBMIterator* iterator = NULL;

	(void)backend_data;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	iterator = g_new(JGDBMIterator, 1);
	iterator->first = TRUE;
	iterator->prefix = g_strdup_printf("%s:%s", namespace, prefix);
	iterator->namespace_len = strlen(namespace) + 1;
	iterator->key.dptr = NULL;
	iterator->value.dptr = NULL;

	*data = iterator;

	return (iterator != NULL);
}

static gboolean
backend_iterate(gpointer backend_data, gpointer data, gchar const** key, gconstpointer* value, guint32* len)
{
	JGDBMData* bd = backend_data;
	JGDBMIterator* iterator = data;
	datum next_key;

	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	if (iterator->first)
	{
		g_mutex_lock(bd->mutex);
		iterator->key = gdbm_firstkey(bd->dbf);
		g_mutex_unlock(bd->mutex);

		iterator->first = FALSE;
	}
	else
	{
		g_mutex_lock(bd->mutex);
		next_key = gdbm_nextkey(bd->dbf, iterator->key);
		g_mutex_unlock(bd->mutex);

		g_free(iterator->key.dptr);
		g_free(iterator->value.dptr);

		iterator->key = next_key;
		iterator->value.dptr = NULL;
	}

	while (iterator->key.dptr != NULL)
	{
		if (!g_str_has_prefix(iterator->key.dptr, iterator->prefix))
		{
			g_mutex_lock(bd->mutex);
			next_key = gdbm_nextkey(bd->dbf, iterator->key);
			g_mutex_unlock(bd->mutex);

			g_free(iterator->key.dptr);
			iterator->key = next_key;

			continue;
		}

		g_mutex_lock(bd->mutex);
		iterator->value = gdbm_fetch(bd->dbf, iterator->key);
		g_mutex_unlock(bd->mutex);

		if (iterator->value.dptr == NULL)
		{
			/// \todo FIXME
		}

		*key = iterator->key.dptr + iterator->namespace_len;
		*value = iterator->value.dptr;
		*len = iterator->value.dsize;

		return TRUE;
	}

	g_free(iterator->prefix);
	g_free(iterator);

	return FALSE;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	JGDBMData* bd;
	g_autofree gchar* dirname = NULL;

	g_return_val_if_fail(path != NULL, FALSE);

	dirname = g_path_get_dirname(path);
	g_mkdir_with_parents(dirname, 0700);

	bd = g_new(JGDBMData, 1);
	bd->dbf = gdbm_open(path, 0, GDBM_WRCREAT, 0600, NULL);

	if (bd->dbf == NULL)
	{
		g_critical("GDBM error %d", gdbm_errno);
		goto error;
	}

	g_mutex_init(bd->mutex);

	*backend_data = bd;

	return (bd->dbf != NULL);

error:
	g_free(bd);

	return FALSE;
}

static void
backend_fini(gpointer backend_data)
{
	JGDBMData* bd = backend_data;

	if (bd->dbf != NULL)
	{
		gdbm_close(bd->dbf);
	}

	g_mutex_clear(bd->mutex);

	g_free(bd);
}

static JBackend gdbm_backend = {
	.type = J_BACKEND_TYPE_KV,
	.component = J_BACKEND_COMPONENT_SERVER,
	.flags = 0,
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
	return &gdbm_backend;
}
