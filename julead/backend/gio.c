/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include "gio.h"

static gchar* julead_backend_gio_path = NULL;

void
julead_backend_gio_init (gchar const* path)
{
	GFile* file;

	julead_backend_gio_path = g_strdup(path);

	file = g_file_new_for_path(path);
	g_file_make_directory_with_parents(file, NULL, NULL);
	g_object_unref(file);
}

void
julead_backend_gio_deinit (void)
{
	g_free(julead_backend_gio_path);
}

gpointer
julead_backend_gio_open (gchar const* store, gchar const* collection, gchar const* item)
{
	GFile* file;
	GFile* parent;
	GFileIOStream* stream;
	gchar* path;

	path = g_build_filename(julead_backend_gio_path, store, collection, item, NULL);
	file = g_file_new_for_path(path);

	parent = g_file_get_parent(file);
	g_file_make_directory_with_parents(parent, NULL, NULL);
	g_object_unref(parent);

	stream = g_file_open_readwrite(file, NULL, NULL);

	if (stream == NULL)
	{
		stream = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, NULL);
	}

	g_object_unref(file);
	g_free(path);

	return stream;
}

void
julead_backend_gio_close (gpointer item)
{
	GFileIOStream* stream = item;

	g_io_stream_close(G_IO_STREAM(stream), NULL, NULL);
}

guint64
julead_backend_gio_read (gpointer item, gpointer buffer, guint64 length, guint64 offset)
{
	GFileIOStream* stream = item;
	GInputStream* input;
	gsize bytes_read;

	input = g_io_stream_get_input_stream(G_IO_STREAM(stream));
	g_seekable_seek(G_SEEKABLE(stream), offset, G_SEEK_SET, NULL, NULL);
	g_input_stream_read_all(input, buffer, length, &bytes_read, NULL, NULL);

	return bytes_read;
}

guint64
julead_backend_gio_write (gpointer item, gconstpointer buffer, guint64 length, guint64 offset)
{
	GFileIOStream* stream = item;
	GOutputStream* output;
	gsize bytes_written;

	output = g_io_stream_get_output_stream(G_IO_STREAM(stream));
	g_seekable_seek(G_SEEKABLE(stream), offset, G_SEEK_SET, NULL, NULL);
	g_output_stream_write_all(output, buffer, length, &bytes_written, NULL, NULL);

	return bytes_written;
}
