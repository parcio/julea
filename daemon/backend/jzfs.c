/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#define _XOPEN_SOURCE 500

#include <julea-config.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <gmodule.h>

#include <jzfs.h>

#include <leveldb/c.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>

#include <jtrace-internal.h>

#include "backend.h"
#include "backend-internal.h"

static gchar* jd_backend_path = NULL;
static JZFSPool* pool = NULL;
static JZFSObjectSet* object_set = NULL;
leveldb_t *db;
leveldb_options_t *options;
leveldb_readoptions_t *roptions;
leveldb_writeoptions_t *woptions;

G_MODULE_EXPORT
gboolean
backend_create (JBackendItem* bf, gchar const* store, gchar const* collection, gchar const* item)
{
	//gchar* parent;
	gchar* path;
	JZFSObject* object;
	int key_len;
	int value_len;
	char *err = NULL;
	char *key = NULL;
	char *value = NULL;

	printf("in backend_create\n");
	j_trace_enter(G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);

	j_trace_file_begin(path, J_TRACE_FILE_CREATE);
	object = j_zfs_object_create(object_set);
	j_trace_file_end(path, J_TRACE_FILE_CREATE, 0, 0);

	guint64 object_id = j_zfs_get_object_id(object);
	printf("Object created.\n");

	//write object_id in db:
	key = path;
	key_len = strlen(key);
	value = g_strdup_printf("%" G_GUINT64_FORMAT, object_id);
	value_len = strlen(value);

	woptions = leveldb_writeoptions_create();
	leveldb_put(db, woptions, key, key_len, value, value_len, &err);
	if(err != NULL) {
		fprintf(stderr, "Write failed.\n");
		return(1);
	}
	leveldb_free(err);
	err = NULL;
	printf("Wrote in db: \n key: %s, value: %s\n", key, value);
	g_free(value);

	bf->path = path;
	bf->user_data = object;

	j_trace_leave(G_STRFUNC);

	return (object != 0);
}

G_MODULE_EXPORT
gboolean
backend_delete (JBackendItem* bf)
{
	printf("in backend_delete\n");
	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	j_zfs_object_destroy(bf->user_data);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

G_MODULE_EXPORT
gboolean
backend_open (JBackendItem* bf, gchar const* store, gchar const* collection, gchar const* item)
{
	gchar* path;
	char *err = NULL;
	JZFSObject* object;
	guint64 object_id;
	char *value;
	char *key;
	int key_len;
	size_t read_len;
	printf("in backend_open\n");

	j_trace_enter(G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);
	key = path;
	key_len = strlen(key);

	//read from db
	roptions = leveldb_readoptions_create();
	value = leveldb_get(db, roptions, key, key_len, &read_len, &err);

	if(err != NULL) {
		fprintf(stderr, "Read failed.\n");
		return(1);
	}

	printf("Read from db:\n key: %s, value: %s \n", key, value);
	leveldb_free(err);
	err = NULL;
	object_id = g_ascii_strtoull(value, NULL, 10);

	j_trace_file_begin(path, J_TRACE_FILE_OPEN);
	object = j_zfs_object_open(object_set, object_id);
	j_trace_file_end(path, J_TRACE_FILE_OPEN, 0, 0);

	bf->path = path;
	bf->user_data = object;

	j_trace_leave(G_STRFUNC);

	return (object != 0);
}

G_MODULE_EXPORT
gboolean
backend_close (JBackendItem* bf)
{
	JZFSObject* object = bf->user_data;
	printf("in backend_close\n");
	j_trace_enter(G_STRFUNC);

	if(object != 0)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
		j_zfs_object_close(object);
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
	JZFSObject* object = bf->user_data;
	printf("in backend_status\n");
	j_trace_enter(G_STRFUNC);

	if (object != 0)
	{
		/*struct stat buf;

		j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
		fstat(fd, &buf);
		j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);
		*/

		*modification_time = 0; // evtl. zu object_header hinzufügen
		*size = j_zfs_object_get_size(object);
	}

	j_trace_leave(G_STRFUNC);

	return (object != 0);
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
	JZFSObject* object = bf->user_data;
	printf("in backend_read\n");
	j_trace_enter(G_STRFUNC);

	if (object != 0)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
		j_zfs_object_read(object, buffer, length, offset);
		j_trace_file_end(bf->path, J_TRACE_FILE_READ, length, offset);

		if (bytes_read != NULL)
		{
			*bytes_read = length; //check: bytes read in ZFS
		}
	}

	j_trace_leave(G_STRFUNC);

	return (object != 0);
}

G_MODULE_EXPORT
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	JZFSObject* object = bf->user_data;
	printf("in backend_write\n");
	j_trace_enter(G_STRFUNC);

	if(object != 0)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
		j_zfs_object_write(object, buffer, length, offset);
		j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, length, offset);

		if (bytes_written != NULL)
		{
			*bytes_written = length; //check: bytes written in ZFS
		}
	}

	j_trace_leave(G_STRFUNC);

	return (object != 0);
}

G_MODULE_EXPORT
gboolean
backend_init (gchar const* path)
{
	/*gchar* poolname = "jzfs";
	gchar* object_set_name = "object_set";

	j_trace_enter(G_STRFUNC);
	j_zfs_init();
	pool = j_zfs_pool_open(poolname);
	object_set = j_zfs_object_set_create(pool, object_set_name);

	if (object_set == NULL)
	{
		printf("zfs_open\n");
		object_set = j_zfs_object_set_open(pool, object_set_name);
	}

	jd_backend_path = g_strdup(path);

	j_trace_leave(G_STRFUNC);*/
	jd_backend_path = g_strdup(path);

	return TRUE;
}

G_MODULE_EXPORT
void
backend_fini (void)
{
	/*j_trace_enter(G_STRFUNC);

	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);
	j_zfs_fini();


	j_trace_leave(G_STRFUNC);*/
	//close and destroy db

	g_free(jd_backend_path);
}

G_MODULE_EXPORT
gboolean
backend_thread_init (void)
{
	gchar* poolname = "jzfs";
	gchar* object_set_name = "object_set";
	char *err = NULL;

	j_trace_enter(G_STRFUNC);
	j_zfs_init();
	pool = j_zfs_pool_open(poolname);
	object_set = j_zfs_object_set_create(pool, object_set_name);

	if (object_set == NULL)
	{
		printf("zfs_open\n");
		object_set = j_zfs_object_set_open(pool, object_set_name);
	}

	//jd_backend_path = g_strdup(path);
	//Open db
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	db = leveldb_open(options, "testdb", &err);
	if(err != NULL) {
		fprintf(stderr, "Open failed.\n");
	}
	leveldb_free(err);
	err = NULL;

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

G_MODULE_EXPORT
void
backend_thread_fini (void)
{
	char *err = NULL;
	j_trace_enter(G_STRFUNC);

	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);
	j_zfs_fini();

	//close and destroy db
	leveldb_close(db);
	leveldb_destroy_db(options, "testdb", &err);
	if (err != NULL) {
		fprintf(stderr, "Destroy failed.\n");
	}
	leveldb_free(err);
	err = NULL;

	//g_free(jd_backend_path);

	j_trace_leave(G_STRFUNC);
}
