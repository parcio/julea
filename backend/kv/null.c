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

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <julea.h>
#include <julea-internal.h>

static bson_t empty[1];

static
gboolean
backend_batch_start (gchar const* namespace, JSemanticsSafety safety, gpointer* data)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	(void)safety;

	// Return something != NULL
	*data = empty;

	return TRUE;
}

static
gboolean
backend_batch_execute (gpointer data)
{
	g_return_val_if_fail(data != NULL, FALSE);

	return TRUE;
}

static
gboolean
backend_put (gpointer data, gchar const* key, bson_t const* value)
{
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	return TRUE;
}

static
gboolean
backend_delete (gpointer data, gchar const* key)
{
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	return TRUE;
}

static
gboolean
backend_get (gchar const* namespace, gchar const* key, bson_t* result_out)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

	bson_copy_to(empty, result_out);

	return TRUE;
}

static
gboolean
backend_get_all (gchar const* namespace, gpointer* data)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	*data = NULL;

	return TRUE;
}

static
gboolean
backend_get_by_prefix (gchar const* namespace, gchar const* prefix, gpointer* data)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(prefix != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	*data = NULL;

	return TRUE;
}

static
gboolean
backend_iterate (gpointer data, bson_t* result_out)
{
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(result_out != NULL, FALSE);

	bson_init_static(result_out, bson_get_data(empty), empty->len);

	return FALSE;
}

static
gboolean
backend_init (gchar const* path)
{
	(void)path;

	bson_init(empty);

	return TRUE;
}

static
void
backend_fini (void)
{
	bson_destroy(empty);
}

static
JBackend null_backend = {
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
		.backend_iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &null_backend;
}
