/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

#define _POSIX_C_SOURCE 200809L

#include <julea-config.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <gmodule.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <jtrace-internal.h>

#include "backend.h"
#include "backend-internal.h"

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
backend_file_unref (gpointer data)
{
	JBackendFile* file = data;

	G_LOCK(jd_backend_file_cache);

	if (g_atomic_int_dec_and_test(&(file->ref_count)))
	{
		g_hash_table_remove(jd_backend_file_cache, file->item.path);

		backend_close(&(file->item), NULL);

		g_slice_free(JBackendFile, file);
	}

	G_UNLOCK(jd_backend_file_cache);
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

G_MODULE_EXPORT
gboolean
backend_create (JBackendItem* bf, gchar const* path, gpointer data)
{
	GHashTable* files = data;

	JBackendFile* file;
	gchar* parent;
	gchar* full_path;
	gint fd;

	j_trace_enter(G_STRFUNC);

	full_path = g_build_filename(jd_backend_path, path, NULL);

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
	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gboolean
backend_delete (JBackendItem* bf, gpointer data)
{
	GHashTable* files = data;

	gboolean ret;

	j_trace_enter(G_STRFUNC);

	j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
	ret = (g_unlink(bf->path) == 0);
	j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);

	g_hash_table_remove(files, bf->path);

	j_trace_leave(G_STRFUNC);

	return ret;
}

G_MODULE_EXPORT
gboolean
backend_open (JBackendItem* bf, gchar const* path, gpointer data)
{
	GHashTable* files = data;

	JBackendFile* file;
	gchar* full_path;
	gint fd;

	j_trace_enter(G_STRFUNC);

	full_path = g_build_filename(jd_backend_path, path, NULL);

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
	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gboolean
backend_close (JBackendItem* bf, gpointer data)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	j_trace_enter(G_STRFUNC);

	if (data != NULL)
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
	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gboolean
backend_status (JBackendItem* bf, JItemStatusFlags flags, gint64* modification_time, guint64* size, gpointer data)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	(void)data;

	j_trace_enter(G_STRFUNC);

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

	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gboolean
backend_sync (JBackendItem* bf, gpointer data)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);

	(void)data;

	j_trace_enter(G_STRFUNC);

	if (fd != -1)
	{
		j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
		fsync(fd);
		j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);
	}

	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gboolean
backend_read (JBackendItem* bf, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read, gpointer data)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);
	gsize nbytes;

	(void)data;

	j_trace_enter(G_STRFUNC);

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

	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gboolean
backend_write (JBackendItem* bf, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written, gpointer data)
{
	gint fd = GPOINTER_TO_INT(bf->user_data);
	gsize nbytes;

	(void)data;

	j_trace_enter(G_STRFUNC);

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

	j_trace_leave(G_STRFUNC);

	return (fd != -1);
}

G_MODULE_EXPORT
gpointer
backend_thread_init (void)
{
	GHashTable* files;

	files = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, backend_file_unref);

	return files;
}

G_MODULE_EXPORT
void
backend_thread_fini (gpointer data)
{
	GHashTable* files = data;

	g_return_if_fail(data != NULL);

	g_hash_table_destroy(files);
}

G_MODULE_EXPORT
gboolean
backend_init (gchar const* path)
{
	j_trace_enter(G_STRFUNC);

	jd_backend_path = g_strdup(path);
	jd_backend_file_cache = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);

	g_mkdir_with_parents(path, 0700);

	j_trace_leave(G_STRFUNC);

	return TRUE;
}

G_MODULE_EXPORT
void
backend_fini (void)
{
	j_trace_enter(G_STRFUNC);

	g_assert(g_hash_table_size(jd_backend_file_cache) == 0);
	g_hash_table_destroy(jd_backend_file_cache);

	g_free(jd_backend_path);

	j_trace_leave(G_STRFUNC);
}
