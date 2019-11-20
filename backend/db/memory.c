/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Michael Kuhn
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

#include <julea.h>

static bson_t* schema_cache = NULL;
static bson_t* entry_cache = NULL;
static guint32 entry_counter = 0;

G_LOCK_DEFINE_STATIC(global_lock);

static
gboolean
backend_batch_start (gchar const* namespace, JSemantics* semantics, gpointer* batch, GError** error)
{
	(void)error;
	(void)batch;
	(void)semantics;
	(void)namespace;

	// Return something != NULL
	*batch = batch;

	return TRUE;
}

static
gboolean
backend_batch_execute (gpointer batch, GError** error)
{
	(void)error;
	(void)batch;

	return TRUE;
}

static
gboolean
backend_schema_create (gpointer batch, gchar const* name, bson_t const* schema, GError** error)
{
	(void)error;
	(void)batch;
	(void)name;
	(void)schema;

	G_LOCK(global_lock);

	if (schema_cache != NULL)
	{
		bson_destroy(schema_cache);
	}

	schema_cache = bson_copy(schema);

	G_UNLOCK(global_lock);

	return TRUE;
}

static
gboolean
backend_schema_get (gpointer batch, gchar const* name, bson_t* schema, GError** error)
{
	gboolean ret = FALSE;

	(void)error;
	(void)batch;
	(void)name;
	(void)schema;

	G_LOCK(global_lock);

	if (schema_cache != NULL)
	{
		// FIXME schema is uninitialized if backend is running on the server but initialized if running on the client
		// bson_copy_to requires the destination to be uninitialized
		//bson_destroy(schema);
		bson_copy_to(schema_cache, schema);
		ret = TRUE;
	}

	G_UNLOCK(global_lock);

	return ret;
}

static
gboolean
backend_schema_delete (gpointer batch, gchar const* name, GError** error)
{
	(void)error;
	(void)batch;
	(void)name;

	G_LOCK(global_lock);

	if (schema_cache != NULL)
	{
		bson_destroy(schema_cache);
		schema_cache = NULL;
	}

	G_UNLOCK(global_lock);

	return TRUE;
}

static
gboolean
backend_insert (gpointer batch, gchar const* name, bson_t const* metadata, bson_t* id, GError** error)
{
	(void)error;
	(void)batch;
	(void)name;
	(void)metadata;
	(void)id;

	G_LOCK(global_lock);

	if (entry_cache != NULL)
	{
		bson_destroy(entry_cache);
	}

	entry_cache = bson_copy(metadata);
	entry_counter++;

	G_UNLOCK(global_lock);

	return TRUE;
}

static
gboolean
backend_update (gpointer batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error)
{
	(void)error;
	(void)batch;
	(void)name;
	(void)selector;
	(void)metadata;

	return TRUE;
}

static
gboolean
backend_delete (gpointer batch, gchar const* name, bson_t const* selector, GError** error)
{
	(void)error;
	(void)batch;
	(void)name;
	(void)selector;

	G_LOCK(global_lock);

	if (entry_cache != NULL)
	{
		bson_destroy(entry_cache);
		entry_cache = NULL;
	}

	entry_counter--;

	G_UNLOCK(global_lock);

	return TRUE;
}

static
gboolean
backend_query (gpointer batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	guint32* counter;

	(void)error;
	(void)batch;
	(void)name;
	(void)selector;

	counter = g_new(guint32, 1);
	*iterator = counter;

	G_LOCK(global_lock);

	if (entry_cache == NULL)
	{
		*counter = 0;
	}
	else if (selector != NULL)
	{
		*counter = 1;
	}
	else
	{
		*counter = entry_counter;
	}

	G_UNLOCK(global_lock);

	return TRUE;
}

static
gboolean
backend_iterate (gpointer iterator, bson_t* metadata, GError** error)
{
	gboolean ret = TRUE;
	guint32* counter = iterator;

	(void)error;
	(void)iterator;
	(void)metadata;

	G_LOCK(global_lock);

	if (*counter <= 0 || entry_cache == NULL)
	{
		g_free(counter);
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		ret = FALSE;
		goto end;
	}

	(*counter)--;
	// bson_copy_to requires the destination to be uninitialized
	bson_destroy(metadata);
	bson_copy_to(entry_cache, metadata);

end:
	G_UNLOCK(global_lock);

	return ret;
}

static
gboolean
backend_init (gchar const* path)
{
	(void)path;

	return TRUE;
}

static
void
backend_fini (void)
{
	if (schema_cache != NULL)
	{
		bson_destroy(schema_cache);
	}

	if (entry_cache != NULL)
	{
		bson_destroy(entry_cache);
	}
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
		.backend_batch_execute = backend_batch_execute
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &memory_backend;
}
