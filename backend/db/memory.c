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

G_LOCK_DEFINE_STATIC(global_lock);

static
bson_t* schema_cache = NULL;

static
bson_t* entry_cache = NULL;

static
guint entry_counter = 0;

static
gboolean
backend_batch_start (gchar const* namespace, JSemantics* semantics, gpointer* batch, GError** error)
{
	(void)error;
	(void)batch;
	(void)semantics;
	(void)namespace;

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
	if (schema_cache)
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
	(void)error;
	(void)batch;
	(void)name;
	(void)schema;

	if (schema_cache)
	{
		bson_copy_to(schema_cache, schema);
		return TRUE;
	}

	return FALSE;
}

static
gboolean
backend_schema_delete (gpointer batch, gchar const* name, GError** error)
{
	(void)error;
	(void)batch;
	(void)name;

	G_LOCK(global_lock);
	if (schema_cache)
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
	if (entry_cache)
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
	if (entry_cache)
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
	guint32* query_counter_local = *iterator = g_new(guint32, 1);

	(void)error;
	(void)batch;
	(void)name;
	(void)selector;
	(void)iterator;

	if (!entry_cache)
	{
		*query_counter_local = 0;
	}
	else if (selector)
	{
		*query_counter_local = 1;
	}
	else
	{
		*query_counter_local = entry_counter;
	}

	if (!*query_counter_local)
	{
		g_free(query_counter_local);
		*iterator = NULL;
		return FALSE;
	}

	return TRUE;
}
static
gboolean
backend_iterate (gpointer iterator, bson_t* metadata, GError** error)
{
	guint32* query_counter_local = iterator;

	(void)error;
	(void)iterator;
	(void)metadata;

	if ((*query_counter_local <= 0) || (!entry_cache))
	{
		g_free(iterator);
		g_set_error_literal(error, J_BACKEND_DB_ERROR, J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS, "no more elements");
		return FALSE;
	}

	(*query_counter_local)--;
	bson_copy_to(entry_cache, metadata);

	return TRUE;
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
}

static JBackend null_backend = {
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
	return &null_backend;
}
