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

#include <leveldb/c.h>

#include <jbackend.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

static leveldb_t* backend_db;

static
gboolean
backend_batch_start (gchar const* namespace, gpointer* data)
{
	leveldb_writebatch_t* batch;

	j_trace_enter(G_STRFUNC);

	batch = leveldb_writebatch_create();
	*data = batch;

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_batch_execute (gpointer data)
{
	gboolean ret = FALSE;

	leveldb_writebatch_t* batch = data;

	j_trace_enter(G_STRFUNC);

	leveldb_write(backend_db, NULL, batch, NULL);
	leveldb_writebatch_destroy(batch);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_put (gchar const* key, bson_t const* value, gpointer data)
{
	leveldb_writebatch_t* batch = data;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	leveldb_writebatch_put(batch, key, strlen(key) + 1, (gchar*)bson_get_data(value), value->len);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_delete (gchar const* key, gpointer data)
{
	leveldb_writebatch_t* batch = data;

	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	leveldb_writebatch_delete(batch, key, strlen(key) + 1);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

static
gboolean
backend_get (gchar const* namespace, gchar const* key, bson_t* result_out)
{
	gboolean ret = FALSE;

	bson_t tmp[1];

	gpointer result;
	gsize result_len;

	j_trace_enter(G_STRFUNC);

	result = leveldb_get(backend_db, NULL, key, strlen(key) + 1, &result_len, NULL);
	bson_init_static(tmp, result, result_len);
	bson_copy_to(tmp, result_out);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_get_all (gchar const* namespace, gpointer* data)
{
	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_get_by_value (gchar const* namespace, bson_t const* value, gpointer* data)
{
	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_iterate (gpointer data, bson_t const** result_out)
{
	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC);

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
backend_init (gchar const* path)
{
	leveldb_options_t* options;

	j_trace_enter(G_STRFUNC);

	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	leveldb_options_set_compression(options, leveldb_snappy_compression);

	backend_db = leveldb_open(options, path, NULL);

	leveldb_options_destroy(options);

	j_trace_leave(G_STRFUNC);

	return (backend_db != NULL);
}

static
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);

	leveldb_close(backend_db);

	j_trace_leave(G_STRFUNC);
}

static
JBackend leveldb_backend = {
	.type = J_BACKEND_TYPE_META,
	.u.meta = {
		.init = backend_init,
		.fini = backend_fini,
		.thread_init = NULL,
		.thread_fini = NULL,
		.batch_start = backend_batch_start,
		.batch_execute = backend_batch_execute,
		.put = backend_put,
		.delete = backend_delete,
		.get = backend_get,
		.get_all = backend_get_all,
		.get_by_value = backend_get_by_value,
		.iterate = backend_iterate
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	j_trace_enter(G_STRFUNC);

	if (type == J_BACKEND_TYPE_META)
	{
		backend = &leveldb_backend;
	}

	j_trace_leave(G_STRFUNC);

	return backend;
}
