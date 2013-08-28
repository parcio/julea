#define _XOPEN_SOURCE 500

#include <leveldb/c.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <gmodule.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>

#include <jtrace-internal.h>

#include "backend.h"
#include "backend-internal.h"

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