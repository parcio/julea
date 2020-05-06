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

static gboolean
backend_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* batch, GError** error)
{
	(void)backend_data;
	(void)namespace;
	(void)semantics;
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
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)schema;
	(void)error;

	return TRUE;
}

static gboolean
backend_schema_get(gpointer backend_data, gpointer batch, gchar const* name, bson_t* schema, GError** error)
{
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)schema;
	(void)error;

	return TRUE;
}

static gboolean
backend_schema_delete(gpointer backend_data, gpointer batch, gchar const* name, GError** error)
{
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)error;

	return TRUE;
}

static gboolean
backend_insert(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* metadata, bson_t* id, GError** error)
{
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)metadata;
	(void)id;
	(void)error;

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
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)selector;
	(void)error;

	return TRUE;
}

static gboolean
backend_query(gpointer backend_data, gpointer batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error)
{
	(void)backend_data;
	(void)batch;
	(void)name;
	(void)selector;
	(void)iterator;
	(void)error;

	return TRUE;
}

static gboolean
backend_iterate(gpointer backend_data, gpointer iterator, bson_t* metadata, GError** error)
{
	(void)backend_data;
	(void)iterator;
	(void)metadata;
	(void)error;

	return TRUE;
}

static gboolean
backend_init(gchar const* path, gpointer* backend_data)
{
	(void)path;
	(void)backend_data;

	return TRUE;
}

static void
backend_fini(gpointer backend_data)
{
	(void)backend_data;
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
		.backend_batch_execute = backend_batch_execute }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &null_backend;
}
