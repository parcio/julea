#define _XOPEN_SOURCE 500

#include <leveldb/c.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <gmodule.h>

#include <lexos.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>

#include <jtrace-internal.h>

#include "backend.h"
#include "backend-internal.h"

ObjectStore* objectstore = NULL;

static gchar* jd_backend_path = NULL; //FIXME: required?

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
	Object* object;
	guint64 object_id; 
	int key_len;
	int value_len;
	char *err = NULL;
	char *key = NULL;
	char *value = NULL;

	printf("in backend_create\n");
	j_trace_enter(G_STRFUNC);

	path = g_build_filename(jd_backend_path, store, collection, item, NULL);

	j_trace_file_begin(path, J_TRACE_FILE_CREATE);
	object = lobject_create(objectstore);
	j_trace_file_end(path, J_TRACE_FILE_CREATE, 0, 0);

	object_id = lobject_get_id(object);
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
	lobject_delete(bf->user_data,NULL); /*FIXME GError*/
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
	Object* object;
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
	object = lobject_open(objectstore, object_id);
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
	Object* object = bf->user_data;
	printf("in backend_close\n");
	j_trace_enter(G_STRFUNC);

	if(object != 0)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
		lobject_close(object,NULL); //FIXME GError
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
		*size = lobject_get_size(object);
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
	//JZFSObject* object = bf->user_data;
	Object* object = bf->user_data;
	printf("in backend_read\n");
	j_trace_enter(G_STRFUNC);

	if (object != 0)
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

	return (object != 0);
}

G_MODULE_EXPORT
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	Object* object = bf->user_data;
	printf("in backend_write\n");
	j_trace_enter(G_STRFUNC);

	if(object != 0)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
		lobject_write(object,offset,length,(void*)buffer,LOBJECT_NO_SYNC);/*FIXME what about sync?*/
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
void
backend_init (gchar const* path)
{
	//gchar* poolname = "jzfs";
	//gchar* object_set_name = "object_set";

	j_trace_enter(G_STRFUNC);
	//j_zfs_init();
	//pool = j_zfs_pool_open(poolname);
	//object_set = j_zfs_object_set_create(pool, object_set_name);
	objectstore = lstore_open(path,NULL);

	/*if (object_set == NULL)
	{
		printf("zfs_open\n");
		//object_set = j_zfs_object_set_open(pool, object_set_name);
	}*/

	//jd_backend_path = g_strdup(path);

	j_trace_leave(G_STRFUNC);
	jd_backend_path = g_strdup(path);
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
	j_trace_enter(G_STRFUNC);

	lstore_close(objectstore, NULL);

	j_trace_leave(G_STRFUNC);

	g_free(jd_backend_path);
}