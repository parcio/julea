/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#include <julea.h>

struct JBackendFile
{
	gchar* path;
	gint fd;
	guint ref_count;
};

typedef struct JBackendFile JBackendFile;

static GHashTable* jd_backend_file_cache = NULL;
static gchar* jd_backend_path = NULL;

G_LOCK_DEFINE_STATIC(jd_backend_file_cache);

static void
jd_backend_files_free(gpointer data)
{
	GHashTable* files = data;

	g_hash_table_destroy(files);
}

// FIXME not deleted?
static GPrivate jd_backend_files = G_PRIVATE_INIT(jd_backend_files_free);

static void
backend_file_unref(gpointer data)
{
	JBackendFile* file = data;

	g_return_if_fail(file != NULL);

	G_LOCK(jd_backend_file_cache);

	if (g_atomic_int_dec_and_test(&(file->ref_count)))
	{
		g_hash_table_remove(jd_backend_file_cache, file->path);

		j_trace_file_begin(file->path, J_TRACE_FILE_CLOSE);
		close(file->fd);
		j_trace_file_end(file->path, J_TRACE_FILE_CLOSE, 0, 0);

		g_free(file->path);
		g_slice_free(JBackendFile, file);
	}

	G_UNLOCK(jd_backend_file_cache);
}

static GHashTable*
jd_backend_files_get_thread(void)
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

static JBackendFile*
backend_file_get(GHashTable* files, gchar const* key)
{
	JBackendFile* file;

	if ((file = g_hash_table_lookup(files, key)) != NULL)
	{
		goto end;
	}

	G_LOCK(jd_backend_file_cache);

	if ((file = g_hash_table_lookup(jd_backend_file_cache, key)) != NULL)
	{
		g_atomic_int_inc(&(file->ref_count));
		g_hash_table_insert(files, file->path, file);
		G_UNLOCK(jd_backend_file_cache);
	}

	/* Attention: The caller must call backend_file_add() if NULL is returned! */

end:
	return file;
}

static void
backend_file_add(GHashTable* files, JBackendFile* file)
{
	g_hash_table_insert(jd_backend_file_cache, file->path, file);
	g_hash_table_insert(files, file->path, file);

	G_UNLOCK(jd_backend_file_cache);
}

static gboolean
backend_create(gchar const* namespace, gchar const* path, gpointer* data)
{
	GHashTable* files = jd_backend_files_get_thread();

	JBackendFile* file = NULL;
	g_autofree gchar* parent = NULL;
	gchar* full_path;
	gint fd;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);

	if ((file = backend_file_get(files, full_path)) != NULL)
	{
		g_free(full_path);

		// FIXME
		fd = file->fd;

		goto end;
	}

	j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);

	parent = g_path_get_dirname(full_path);
	g_mkdir_with_parents(parent, 0700);

	fd = open(full_path, O_RDWR | O_CREAT, 0600);

	j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);

	file = g_slice_new(JBackendFile);
	file->path = full_path;
	file->fd = fd;
	file->ref_count = 1;

	backend_file_add(files, file);

end:
	*data = file;

	return (fd != -1);
}

static gboolean
backend_open(gchar const* namespace, gchar const* path, gpointer* data)
{
	GHashTable* files = jd_backend_files_get_thread();

	JBackendFile* file = NULL;
	gchar* full_path;
	gint fd;

	full_path = g_build_filename(jd_backend_path, namespace, path, NULL);

	if ((file = backend_file_get(files, full_path)) != NULL)
	{
		g_free(full_path);

		// FIXME
		fd = file->fd;

		goto end;
	}

	j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
	fd = open(full_path, O_RDWR);
	j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);

	file = g_slice_new(JBackendFile);
	file->path = full_path;
	file->fd = fd;
	file->ref_count = 1;

	backend_file_add(files, file);

end:
	*data = file;

	return (fd != -1);
}

static gboolean
backend_delete(gpointer data)
{
	JBackendFile* file = data;
	GHashTable* files = jd_backend_files_get_thread();
	gboolean ret;

	j_trace_file_begin(file->path, J_TRACE_FILE_DELETE);
	ret = (g_unlink(file->path) == 0);
	j_trace_file_end(file->path, J_TRACE_FILE_DELETE, 0, 0);

	g_hash_table_remove(files, file->path);

	return ret;
}

static gboolean
backend_close(gpointer data)
{
	JBackendFile* file = data;
	GHashTable* files = jd_backend_files_get_thread();
	gboolean ret;

	ret = g_hash_table_remove(files, file->path);

	return ret;
}

static gboolean
backend_status(gpointer data, gint64* modification_time, guint64* size)
{
	JBackendFile* file = data;
	gboolean ret = TRUE;
	struct stat buf;

	if (modification_time != NULL || size != NULL)
	{
		j_trace_file_begin(file->path, J_TRACE_FILE_STATUS);
		ret = (fstat(file->fd, &buf) == 0);
		j_trace_file_end(file->path, J_TRACE_FILE_STATUS, 0, 0);

		if (ret && modification_time != NULL)
		{
			*modification_time = buf.st_mtime * G_USEC_PER_SEC;

#ifdef HAVE_STMTIM_TVNSEC
			*modification_time += buf.st_mtim.tv_nsec / 1000;
#endif
		}

		if (ret && size != NULL)
		{
			*size = buf.st_size;
		}
	}

	return ret;
}

static gboolean
backend_sync(gpointer data)
{
	JBackendFile* file = data;
	gboolean ret;

	j_trace_file_begin(file->path, J_TRACE_FILE_SYNC);
	ret = (fsync(file->fd) == 0);
	j_trace_file_end(file->path, J_TRACE_FILE_SYNC, 0, 0);

	return ret;
}

static gboolean
backend_read(gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	JBackendFile* file = data;

	gsize nbytes_total = 0;

	j_trace_file_begin(file->path, J_TRACE_FILE_READ);

	while (nbytes_total < length)
	{
		gssize nbytes;

		nbytes = pread(file->fd, (gchar*)buffer + nbytes_total, length - nbytes_total, offset + nbytes_total);

		if (nbytes == 0)
		{
			break;
		}
		else if (nbytes < 0)
		{
			if (errno != EINTR)
			{
				break;
			}
		}

		nbytes_total += nbytes;
	}

	j_trace_file_end(file->path, J_TRACE_FILE_READ, nbytes_total, offset);

	if (bytes_read != NULL)
	{
		*bytes_read = nbytes_total;
	}

	return (nbytes_total == length);
}

static gboolean
backend_write(gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	JBackendFile* file = data;

	gsize nbytes_total = 0;

	j_trace_file_begin(file->path, J_TRACE_FILE_WRITE);

	while (nbytes_total < length)
	{
		gssize nbytes;

		nbytes = pwrite(file->fd, (gchar const*)buffer + nbytes_total, length - nbytes_total, offset + nbytes_total);

		if (nbytes <= 0)
		{
			if (errno != EINTR)
			{
				break;
			}
		}

		nbytes_total += nbytes;
	}

	j_trace_file_end(file->path, J_TRACE_FILE_WRITE, nbytes_total, offset);

	if (bytes_written != NULL)
	{
		*bytes_written = nbytes_total;
	}

	return (nbytes_total == length);
}

static gboolean
backend_init(gchar const* path)
{
	jd_backend_path = g_strdup(path);
	jd_backend_file_cache = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);

	g_mkdir_with_parents(path, 0700);

	return TRUE;
}

static void
backend_fini(void)
{
	g_assert(g_hash_table_size(jd_backend_file_cache) == 0);
	g_hash_table_destroy(jd_backend_file_cache);

	g_free(jd_backend_path);
}

static JBackend posix_backend = {
	.type = J_BACKEND_TYPE_OBJECT,
	.component = J_BACKEND_COMPONENT_SERVER,
	.object = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_create = backend_create,
		.backend_delete = backend_delete,
		.backend_open = backend_open,
		.backend_close = backend_close,
		.backend_status = backend_status,
		.backend_sync = backend_sync,
		.backend_read = backend_read,
		.backend_write = backend_write }
};

G_MODULE_EXPORT
JBackend*
backend_info(void)
{
	return &posix_backend;
}
