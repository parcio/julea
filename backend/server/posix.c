/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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
#include <glib/gstdio.h>
#include <gmodule.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <jbackend.h>
#include <jtrace-internal.h>

struct JBackendFile
{
	JBackendItem item;
	guint ref_count;
};

typedef struct JBackendFile JBackendFile;

static GHashTable* jd_backend_file_cache = NULL;
static gchar* jd_backend_path = NULL;

G_LOCK_DEFINE_STATIC(jd_backend_file_cache);

static
void
jd_backend_files_free (gpointer data)
{
	GHashTable* files = data;

	g_hash_table_destroy(files);
}

static GPrivate jd_backend_files = G_PRIVATE_INIT(jd_backend_files_free);

static gboolean backend_close (JBackendItem*);

static
void
backend_file_unref (gpointer data)
{
	JBackendFile* file = data;

	G_LOCK(jd_backend_file_cache);

	if (g_atomic_int_dec_and_test(&(file->ref_count)))
	{
		g_hash_table_remove(jd_backend_file_cache, file->item.path);

		backend_close(&(file->item));

		g_slice_free(JBackendFile, file);
	}

	G_UNLOCK(jd_backend_file_cache);
}

static
GHashTable*
jd_backend_files_get_thread (void)
{
	GHashTable* files;

	files = g_private_get(&jd_backend_files);

	if (G_UNLIKELY(files == NULL))
	{
		files = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, backend_file_unref);
		g_private_replace(&jd_backend_files, files);
	}

	return files;
}

static
JBackendFile*
backend_file_get (GHashTable* files, gchar const* key)
{
	JBackendFile* file = NULL;

	if ((file = g_hash_table_lookup(files, key)) != NULL)
	{
		goto end;
	}

	G_LOCK(jd_backend_file_cache);

	if ((file = g_hash_table_lookup(jd_backend_file_cache, key)) != NULL)
	{
		g_atomic_int_inc(&(file->ref_count));
		g_hash_table_insert(files, file->item.path, file);
		G_UNLOCK(jd_backend_file_cache);
	}

	/* Attention: The caller must call backend_file_add() if NULL is returned! */

end:
	return file;
}

static
void
backend_file_add (GHashTable* files, JBackendItem* backend_item)
{
	JBackendFile* file;

	file = g_slice_new(JBackendFile);
	file->item = *backend_item;
	file->ref_count = 1;

	g_hash_table_insert(jd_backend_file_cache, file->item.path, file);
	g_hash_table_insert(files, file->item.path, file);

	G_UNLOCK(jd_backend_file_cache);
}

static
gboolean
backend_create (JBackendItem* bf, gchar const* namespace, gchar const* path)
{
	GHashTable* files = jd_backend_files_get_thread();

	JBackendFile* file;
	gchar* parent;
	gchar* full_path;
	gint fd;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);

	if ((file = backend_file_get(files, full_path)) != NULL)
	{
		g_free(full_path);

		// FIXME
		fd = 0;
		*bf = file->item;

		goto end;
	}

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);

	parent = g_path_get_dirname(full_path);
	g_mkdir_with_parents(parent, 0700);
	g_free(parent);

	fd = open(full_path, O_RDWR | O_CREAT, 0600);

	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	bf->path = full_path;
	bf->user_data = GINT_TO_POINTER(fd);

	backend_file_add(files, bf);

end:
	return (fd != -1);
}

static
gboolean
backend_delete (JBackendItem* bf)
{
	GHashTable* files = jd_backend_files_get_thread();

	gboolean ret;

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = (g_unlink(bf->path) == 0);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_hash_table_remove(files, bf->path);

	return ret;
}

static
gboolean
backend_open (JBackendItem* bf, gchar const* namespace, gchar const* path)
{
	GHashTable* files = jd_backend_files_get_thread();

	JBackendFile* file;
	gchar* full_path;
	gint fd;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);

	if ((file = backend_file_get(files, full_path)) != NULL)
	{
		g_free(full_path);

		// FIXME
		fd = 0;
		*bf = file->item;

		goto end;
	}

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	fd = open(full_path, O_RDWR);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	bf->path = full_path;
	bf->user_data = GINT_TO_POINTER(fd);

	backend_file_add(files, bf);

end:
	return (fd != -1);
}

static
gboolean
backend_close (JBackendItem* bf)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	// FIXME do not goto end if called from backend_file_unref
	if (jd_backend_files_get_thread() != NULL)
	{
		goto end;
	}

	if (fd != -1)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
		close(fd);
		j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);
	}

	g_free(bf->path);

end:
	return (fd != -1);
}

static
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	if (fd != -1)
	{
		struct stat buf;

		j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
		fstat(fd, &buf);
		j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);

		if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
		{
			*modification_time = buf.st_mtime * G_USEC_PER_SEC;

#ifdef HAVE_STMTIM_TVNSEC
			*modification_time += buf.st_mtim.tv_nsec / 1000;
#endif
		}

		if (flags & J_ITEM_STATUS_SIZE)
		{
			*size = buf.st_size;
		}
	}

	return (fd != -1);
}

static
gboolean
backend_sync (JBackendItem* bf)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	if (fd != -1)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
		fsync(fd);
		j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);
	}

	return (fd != -1);
}

static
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);
	gsize nbytes;

	if (fd != -1)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
		nbytes = pread(fd, buffer, length, offset);
		j_trace_file_end(bf->path, J_TRACE_FILE_READ, nbytes, offset);

		if (bytes_read != NULL)
		{
			*bytes_read = nbytes;
		}
	}

	return (fd != -1);
}

static
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);
	gsize nbytes;

	if (fd != -1)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
		nbytes = pwrite(fd, buffer, length, offset);
		j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, nbytes, offset);

		if (bytes_written != NULL)
		{
			*bytes_written = nbytes;
		}
	}

	return (fd != -1);
}

static
gboolean
backend_init (gchar const* path)
{
	jd_backend_path = g_strdup(path);
	jd_backend_file_cache = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);

	g_mkdir_with_parents(path, 0700);

	return TRUE;
}

static
void
backend_fini (void)
{
	g_assert(g_hash_table_size(jd_backend_file_cache) == 0);
	g_hash_table_destroy(jd_backend_file_cache);

	g_free(jd_backend_path);
}

static
JBackend posix_backend = {
	.type = J_BACKEND_TYPE_DATA,
	.u.data = {
		.init = backend_init,
		.fini = backend_fini,
		.create = backend_create,
		.delete = backend_delete,
		.open = backend_open,
		.close = backend_close,
		.status = backend_status,
		.sync = backend_sync,
		.read = backend_read,
		.write = backend_write
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (JBackendType type)
{
	JBackend* backend = NULL;

	if (type == J_BACKEND_TYPE_DATA)
	{
		backend = &posix_backend;
	}

	return backend;
}
