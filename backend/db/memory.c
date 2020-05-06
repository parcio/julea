/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019-2020 Michael Kuhn
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

#include <julea.h>

struct JMemoryData
{
	bson_t* schema_cache;
	bson_t* entry_cache;
	guint32 entry_counter;

	GMutex lock[1];
};

typedef struct JMemoryData JMemoryData;

static gboolean
backend_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* batch, GError** error)
{
	(void)backend_data;
	(void)semantics;
	(void)namespace;
	(void)batch;
	(void)error;

	// Return something != NULL
	*batch = batch;

	return TRUE;
}

static gboolean
backend_batch_execute(gpointer backend_data, gpointer batch, GError** error)
{
	(void)backend_data;
	(void)batch;
	(void)error;

	return TRUE;
}

static gboolean
backend_schema_create(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* schema, GError** error)
{
	JMemoryData* bd = backend_data;

	(void)batch;
	(void)name;
	(void)schema;
	(void)error;

	g_mutex_lock(bd->lock);

	if (bd->schema_cache != NULL)
	{
		bson_destroy(bd->schema_cache);
	}

	bd->schema_cache = bson_copy(schema);

	g_mutex_unlock(bd->lock);

	return TRUE;
}

static gboolean
backend_schema_get(gpointer backend_data, gpointer batch, gchar const* name, bson_t* schema, GError** error)
{
	gboolean ret = FALSE;
	JMemoryData* bd = backend_data;

	(void)batch;
	(void)name;
	(void)schema;
	(void)error;

	g_mutex_lock(bd->lock);

	if (bd->schema_cache != NULL)
	{
		// FIXME schema is uninitialized if backend is running on the server but initialized if running on the client
		// bson_copy_to requires the destination to be uninitialized
		//bson_destroy(schema);
		bson_copy_to(bd->schema_cache, schema);
		ret = TRUE;
	}

	g_mutex_unlock(bd->lock);

	return ret;
}

static gboolean
backend_schema_delete(gpointer backend_data, gpointer batch, gchar const* name, GError** error)
{
	JMemoryData* bd = backend_data;

	(void)batch;
	(void)name;
	(void)error;

	g_mutex_lock(bd->lock);

	if (bd->schema_cache != NULL)
	{
		bson_destroy(bd->schema_cache);
		bd->schema_cache = NULL;
	}

	g_mutex_unlock(bd->lock);

	return TRUE;
}

static gboolean
backend_insert(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* metadata, bson_t* id, GError** error)
{
	JMemoryData* bd = backend_data;

	(void)batch;
	(void)name;
	(void)metadata;
	(void)id;
	(void)error;

	g_mutex_lock(bd->lock);

	if (bd->entry_cache != NULL)
	{
		bson_destroy(bd->entry_cache);
	}

	bd->entry_cache = bson_copy(metadata);
	bd->entry_counter++;

	g_mutex_unlock(bd->lock);

	return TRUE;
}

static gboolean
backend_update(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error)
{
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)selector;
	(void)metadata;
	(void)error;

	return TRUE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* selector, GError** error)
{
	JMemoryData* bd = backend_data;

	(void)batch;
	(void)name;
	(void)selector;
	(void)error;

	g_mutex_lock(bd->lock);

	if (bd->entry_cache != NULL)
	{
		bson_destroy(bd->entry_cache);
		bd->entry_cache = NULL;
	}

	bd->entry_counter--;

	g_mutex_unlock(bd->lock);

	return TRUE;
}

static gboolean
backend_query(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	JMemoryData* bd = backend_data;
	guint32* counter;

	(void)batch;
	(void)name;
	(void)selector;
	(void)error;

	counter = g_new(guint32, 1);
	*iterator = counter;

	g_mutex_lock(bd->lock);

	if (bd->entry_cache == NULL)
	{
		*counter = 0;
	}
	else if (selector != NULL)
	{
		*counter = 1;
	}
	else
	{
		*counter = bd->entry_counter;
	}

	g_mutex_unlock(bd->lock);

	return TRUE;
}

static gboolean
backend_iterate(gpointer backend_data, gpointer iterator, bson_t* metadata, GError** error)
{
	gboolean ret = TRUE;
	guint32* counter = iterator;
	JMemoryData* bd = backend_data;

	(void)iterator;
	(void)metadata;
	(void)error;

	g_mutex_lock(bd->lock);

	if (*counter <= 0 || bd->entry_cache == NULL)
	{
		g_free(counter);
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		ret = FALSE;
		goto end;
	}

	(*counter)--;
	// bson_copy_to requires the destination to be uninitialized
	bson_destroy(metadata);
	bson_copy_to(bd->entry_cache, metadata);

end:
	g_mutex_unlock(bd->lock);

	return ret;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	JMemoryData* bd;

	(void)path;

	bd = g_slice_new(JMemoryData);
	bd->schema_cache = NULL;
	bd->entry_cache = NULL;
	bd->entry_counter = 0;
	g_mutex_init(bd->lock);

	*backend_data = bd;

	return TRUE;
}

static void
backend_fini(gpointer backend_data)
{
	JMemoryData* bd = backend_data;

	if (bd->schema_cache != NULL)
	{
		bson_destroy(bd->schema_cache);
	}

	if (bd->entry_cache != NULL)
	{
		bson_destroy(bd->entry_cache);
	}

	g_mutex_clear(bd->lock);
	g_slice_free(JMemoryData, bd);
}

static JBackend memory_backend = {
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
		.backend_batch_execute = backend_batch_execute }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &memory_backend;
}
