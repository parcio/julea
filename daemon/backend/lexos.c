/*
 * Copyright (c) 2013 Sandra Schröder
 * Copyright (c) 2013 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <julea-config.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <gmodule.h>

#include <leveldb/c.h>

#include <lexos.h>

#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <jtrace-internal.h>

#include "backend.h"
#include "backend-internal.h"

ObjectStore* object_store = NULL;

leveldb_t* db;
leveldb_options_t* options;
leveldb_readoptions_t* roptions;
leveldb_writeoptions_t* woptions;

G_MODULE_EXPORT
gboolean
backend_create (JBackendItem* bf, gchar const* store, gchar const* collection, gchar const* item)
{
	Object* object = NULL;
	gchar* path;
	guint64 object_id;
	gchar* err = NULL;

	j_trace_enter(G_STRFUNC);

	path = g_build_filename(store, collection, item, NULL);

	j_trace_file_begin(path, J_TRACE_FILE_CREATE);
	object = lobject_create(object_store, NULL);
	j_trace_file_end(path, J_TRACE_FILE_CREATE, 0, 0);

	if (object == NULL)
	{
		goto end;
	}

	object_id = lobject_get_id(object);
	leveldb_put(db, woptions, path, strlen(path), (gpointer)&object_id, sizeof(object_id), &err);

	if (err != NULL)
	{
		leveldb_free(err);
		goto end;
	}

	bf->path = path;
	bf->user_data = object;

end:
	j_trace_leave(G_STRFUNC);

	return (object != NULL);
}

G_MODULE_EXPORT
gboolean
backend_delete (JBackendItem* bf)
{
	gboolean ret = TRUE;
	gchar* err = NULL;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	/*FIXME GError*/
	lobject_delete(bf->user_data, NULL);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	leveldb_delete(db, woptions, bf->path, strlen(bf->path), &err);

	if (err != NULL)
	{
		ret = FALSE;
		leveldb_free(err);
		goto end;
	}

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

G_MODULE_EXPORT
gboolean
backend_open (JBackendItem* bf, gchar const* store, gchar const* collection, gchar const* item)
{
	Object* object = NULL;
	gchar* path;
	guint64 object_id;
	gchar* err = NULL;
	gchar* value = NULL;
	gsize value_len;

	j_trace_enter(G_STRFUNC);

	path = g_build_filename(store, collection, item, NULL);

	value = leveldb_get(db, roptions, path, strlen(path), &value_len, &err);
	memcpy(&object_id, value, value_len);
	leveldb_free(value);

	if (err != NULL)
	{
		leveldb_free(err);
		goto end;
	}

	j_trace_file_begin(path, J_TRACE_FILE_OPEN);
	object = lobject_open(object_store, object_id, NULL);
	j_trace_file_end(path, J_TRACE_FILE_OPEN, 0, 0);

	if (object == NULL)
	{
		goto end;
	}

	bf->path = path;
	bf->user_data = object;

end:
	j_trace_leave(G_STRFUNC);

	return (object != NULL);
}

G_MODULE_EXPORT
gboolean
backend_close (JBackendItem* bf)
{
	Object* object = bf->user_data;

	j_trace_enter(G_STRFUNC);

	if (object != NULL)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
		//FIXME GError
		lobject_close(object, LOBJECT_NO_SYNC, NULL);
		j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);
	}

	g_free(bf->path);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

G_MODULE_EXPORT
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size)
{
	Object* object = bf->user_data;

	j_trace_enter(G_STRFUNC);

	if (object != NULL)
	{
		/*struct stat buf;

		j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
		fstat(fd, &buf);
		j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);
		*/

		*modification_time = 0; // evtl. zu object_header hinzufügen
		*size = lobject_get_size(object);
	}

	j_trace_leave(G_STRFUNC);

	return (object != NULL);
}

G_MODULE_EXPORT
gboolean
backend_sync (JBackendItem* bf)
{
	/*gint fd = GPOINTER_TO_INT(bf->user_data);

	j_trace_enter(G_STRFUNC);

	if (fd != -1)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
		fsync(fd);
		j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);
	}

	j_trace_leave(G_STRFUNC);

	return (fd != -1);
	*/
	return TRUE;
}

G_MODULE_EXPORT
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	Object* object = bf->user_data;

	j_trace_enter(G_STRFUNC);

	if (object != NULL)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
		lobject_read(object,offset,length,buffer);
		j_trace_file_end(bf->path, J_TRACE_FILE_READ, length, offset);

		if (bytes_read != NULL)
		{
			*bytes_read = length; //check: bytes read in ZFS
		}
	}

	j_trace_leave(G_STRFUNC);

	return (object != NULL);
}

G_MODULE_EXPORT
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	Object* object = bf->user_data;

	j_trace_enter(G_STRFUNC);

	if (object != NULL)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
		lobject_write(object, offset, length, (gpointer)buffer, LOBJECT_NO_SYNC);/*FIXME what about sync?*/
		j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, length, offset);

		if (bytes_written != NULL)
		{
			*bytes_written = length; //check: bytes written in ZFS
		}
	}

	j_trace_leave(G_STRFUNC);

	return (object != NULL);
}

G_MODULE_EXPORT
gboolean
backend_init (gchar const* path)
{
	gboolean ret = TRUE;
	gchar* err = NULL;

	j_trace_enter(G_STRFUNC);

	if ((object_store = lstore_open(path, NULL)) == NULL)
	{
		goto end;
	}

	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);

	roptions = leveldb_readoptions_create();
	woptions = leveldb_writeoptions_create();

	db = leveldb_open(options, "julea-lexos", &err);

	if (err != NULL)
	{
		leveldb_free(err);
		ret = FALSE;
		goto end;
	}

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

G_MODULE_EXPORT
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);

	lstore_close(object_store, NULL);
	leveldb_close(db);

	j_trace_leave(G_STRFUNC);
}
