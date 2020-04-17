/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2020 Michael Kuhn
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
backend_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* backend_batch)
{
	(void)backend_data;
	(void)semantics;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(backend_batch != NULL, FALSE);

	// Return something != NULL
	*backend_batch = backend_batch;

	return TRUE;
}

static gboolean
backend_batch_execute(gpointer backend_data, gpointer backend_batch)
{
	(void)backend_data;

	g_return_val_if_fail(backend_batch != NULL, FALSE);

	return TRUE;
}

static gboolean
backend_put(gpointer backend_data, gpointer backend_batch, gchar const* key, gconstpointer value, guint32 len)
{
	(void)backend_data;
	(void)len;

	g_return_val_if_fail(backend_batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);

	return TRUE;
}

static gboolean
backend_delete(gpointer backend_data, gpointer backend_batch, gchar const* key)
{
	(void)backend_data;

	g_return_val_if_fail(backend_batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	return TRUE;
}

static gboolean
backend_get(gpointer backend_data, gpointer backend_batch, gchar const* key, gpointer* value, guint32* len)
{
	(void)backend_data;

	g_return_val_if_fail(backend_batch != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	*value = NULL;
	*len = 0;

	return TRUE;
}

static gboolean
backend_get_all(gpointer backend_data, gchar const* namespace, gpointer* backend_iterator)
{
	(void)backend_data;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(backend_iterator != NULL, FALSE);

	*backend_iterator = NULL;

	return TRUE;
}

static gboolean
backend_get_by_prefix(gpointer backend_data, gchar const* namespace, gchar const* prefix, gpointer* backend_iterator)
{
	(void)backend_data;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(backend_iterator != NULL, FALSE);

	*backend_iterator = NULL;

	return TRUE;
}

static gboolean
backend_iterate(gpointer backend_data, gpointer backend_iterator, gchar const** key, gconstpointer* value, guint32* len)
{
	(void)backend_data;

	g_return_val_if_fail(backend_iterator != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(len != NULL, FALSE);

	*key = "";
	*value = "";
	*len = 1;

	return FALSE;
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
	.type = J_BACKEND_TYPE_KV,
	.component = J_BACKEND_COMPONENT_CLIENT | J_BACKEND_COMPONENT_SERVER,
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
	return &null_backend;
}
