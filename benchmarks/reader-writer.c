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

#define _POSIX_C_SOURCE 200809L

#include <julea-config.h>

#include <glib.h>
#include <glib-unix.h>

#include <julea.h>

#include <jhelper-internal.h>
#include <juri.h>

#include <string.h>

#include <mpi.h>

static gboolean opt_julea = FALSE;
static gboolean opt_mpi = FALSE;
static gboolean opt_posix = FALSE;

static gint opt_block_count = 1000000;
static gint opt_block_size = 4096;

static gboolean opt_julea_batch = FALSE;
static gchar* opt_julea_semantics = NULL;
static gchar* opt_julea_template = NULL;

static gboolean opt_mpi_atomic = FALSE;
static gchar* opt_mpi_path = NULL;

static gchar* opt_posix_path = NULL;

static gint process_id = 0;
static gint process_num = 1;

static gint posix_fd = -1;
static MPI_File mpi_file[1];
static JItem* julea_item;

static
gchar const*
get_name (void)
{
	static gchar* name = NULL;

	if (G_UNLIKELY(name == NULL))
	{
		if (opt_posix)
		{
			name = g_build_filename(opt_posix_path, "reader-writer", NULL);
		}
		else if (opt_mpi)
		{
			name = g_build_filename(opt_mpi_path, "reader-writer", NULL);
		}
		else if (opt_julea)
		{
			name = g_strdup("julea://reader-writer/reader-writer/reader-writer");
		}
	}

	return name;
}

static
void
create_file (void)
{
	gchar const* path;

	path = get_name();

	if (opt_posix)
	{
		if (process_id != 0)
		{
			return;
		}

		posix_fd = open(path, O_RDWR | O_CREAT, 0600);
		close(posix_fd);
	}
	else if (opt_mpi)
	{
		MPI_File_open(MPI_COMM_WORLD, (gchar*)path, MPI_MODE_RDWR | MPI_MODE_CREATE, MPI_INFO_NULL, mpi_file);
		MPI_File_close(mpi_file);
	}
	else if (opt_julea)
	{
		JBatch* batch;
		JCollection* collection;
		JStore* store;
		JURI* uri;

		if (process_id != 0)
		{
			return;
		}

		uri = j_uri_new(path);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		j_get_store(&store, j_uri_get_store_name(uri), batch);
		j_batch_execute(batch);

		j_store_get_collection(store, &collection, j_uri_get_collection_name(uri), batch);
		j_batch_execute(batch);

		julea_item = j_item_new(j_uri_get_item_name(uri));

		j_collection_create_item(collection, julea_item, batch);

		j_item_unref(julea_item);
		j_collection_unref(collection);
		j_store_unref(store);

		j_batch_execute(batch);
		j_batch_unref(batch);
	}
}

static
void
open_file (void)
{
	gchar const* path;

	path = get_name();

	if (opt_posix)
	{
		posix_fd = open(path, O_RDWR);

		if (posix_fd == -1)
		{
			g_error("Process %d can not write.\n", process_id);
		}
	}
	else if (opt_mpi)
	{
		gint ret;

		ret = MPI_File_open(MPI_COMM_WORLD, (gchar*)path, MPI_MODE_RDWR, MPI_INFO_NULL, mpi_file);

		if (ret != MPI_SUCCESS)
		{
			g_error("Process %d can not write.\n", process_id);
		}

		if (opt_mpi_atomic)
		{
			MPI_File_set_atomicity(mpi_file[0], 1);
		}
	}
	else if (opt_julea)
	{
		JURI* uri;

		uri = j_uri_new(path);
		j_uri_get(uri, NULL);
		julea_item = j_uri_get_item(uri);

		if (julea_item == NULL)
		{
			g_error("Process %d can not write.\n", process_id);
		}
	}
}

static
void
write_file (void)
{
	gchar buf[opt_block_size];
	guint iteration;

	if (opt_posix)
	{
		for (iteration = 0; iteration < (guint)opt_block_count; iteration += 1000)
		{
			for (guint i = 0; i < 1000; i++)
			{
				guint64 bytes;

				bytes = pwrite(posix_fd, buf, opt_block_size, opt_block_size * (iteration + i));
				g_assert(bytes == (guint)opt_block_size);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
	else if (opt_mpi)
	{
		MPI_Status status[1];

		for (iteration = 0; iteration < (guint)opt_block_count; iteration += 1000)
		{
			for (guint i = 0; i < 1000; i++)
			{
				gint bytes;

				MPI_File_write_at(mpi_file[0], opt_block_size * (iteration + i), buf, opt_block_size, MPI_CHAR, status);
				MPI_Get_count(status, MPI_CHAR, &bytes);
				g_assert(bytes == opt_block_size);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
	else if (opt_julea)
	{
		JBatch* batch;
		JSemantics* semantics;

		semantics = j_helper_parse_semantics(opt_julea_template, opt_julea_semantics);
		batch = j_batch_new(semantics);

		for (iteration = 0; iteration < (guint)opt_block_count; iteration += 1000)
		{
			guint64 bytes[1000];

			for (guint i = 0; i < 1000; i++)
			{
				j_item_write(julea_item, buf, opt_block_size, opt_block_size * (iteration + i), &(bytes[i]), batch);

				if (!opt_julea_batch)
				{
					j_batch_execute(batch);
				}
			}

			if (opt_julea_batch)
			{
				j_batch_execute(batch);
			}

			for (guint i = 0; i < 1000; i++)
			{
				g_assert_cmpuint(bytes[i], ==, (guint)opt_block_size);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
}

static
void
read_file (void)
{
	gchar buf[opt_block_size];
	guint iteration;

	if (opt_posix)
	{
		for (iteration = 0; iteration < (guint)opt_block_count; iteration += 1000)
		{
			for (guint i = 0; i < 1000; i++)
			{
				guint64 bytes;

				bytes = pread(posix_fd, buf, opt_block_size, opt_block_size * (iteration + i));
				g_assert(bytes == (guint)opt_block_size);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
	else if (opt_mpi)
	{
		MPI_Status status[1];

		for (iteration = 0; iteration < (guint)opt_block_count; iteration += 1000)
		{
			for (guint i = 0; i < 1000; i++)
			{
				gint bytes;

				MPI_File_read_at(mpi_file[0], opt_block_size * (iteration + i), buf, opt_block_size, MPI_CHAR, status);
				MPI_Get_count(status, MPI_CHAR, &bytes);
				g_assert(bytes == opt_block_size);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
	else if (opt_julea)
	{
		JBatch* batch;
		JSemantics* semantics;

		semantics = j_helper_parse_semantics(opt_julea_template, opt_julea_semantics);
		batch = j_batch_new(semantics);

		for (iteration = 0; iteration < (guint)opt_block_count; iteration += 1000)
		{
			guint64 bytes[1000];

			for (guint i = 0; i < 1000; i++)
			{
				j_item_read(julea_item, buf, opt_block_size, opt_block_size * (iteration + i), &(bytes[i]), batch);

				if (!opt_julea_batch)
				{
					j_batch_execute(batch);
				}
			}

			if (opt_julea_batch)
			{
				j_batch_execute(batch);
			}

			for (guint i = 0; i < 1000; i++)
			{
				g_assert_cmpuint(bytes[i], ==, (guint)opt_block_size);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
}

static
void
close_file (void)
{
	if (opt_posix)
	{
		close(posix_fd);
	}
	else if (opt_mpi)
	{
		MPI_File_close(mpi_file);
	}
	else if (opt_julea)
	{
		j_item_unref(julea_item);
	}
}

static
void
delete_file (void)
{
	gchar const* path;

	path = get_name();

	if (opt_posix)
	{
		if (process_id != 0)
		{
			return;
		}

		unlink(path);
	}
	else if (opt_mpi)
	{
		MPI_File_delete((gchar*)path, MPI_INFO_NULL);
	}
	else if (opt_julea)
	{
		JBatch* batch;
		JCollection* collection;
		JURI* uri;

		if (process_id != 0)
		{
			return;
		}

		uri = j_uri_new(path);
		j_uri_get(uri, NULL);

		collection = j_uri_get_collection(uri);
		julea_item = j_uri_get_item(uri);

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		j_collection_delete_item(collection, julea_item, batch);

		j_batch_execute(batch);
		j_batch_unref(batch);
	}
}

static
void
do_write (gdouble* result)
{
	GTimer* timer;

	create_file();

	MPI_Barrier(MPI_COMM_WORLD);

	open_file();

	timer = g_timer_new();

	MPI_Barrier(MPI_COMM_WORLD);

	write_file();

	close_file();

	*result = g_timer_elapsed(timer, NULL);

	MPI_Barrier(MPI_COMM_WORLD);

	g_timer_destroy(timer);

	delete_file();
}

static
void
do_read (gdouble* result)
{
	GTimer* timer;

	create_file();

	MPI_Barrier(MPI_COMM_WORLD);

	open_file();

	timer = g_timer_new();

	MPI_Barrier(MPI_COMM_WORLD);

	read_file();

	close_file();

	*result = g_timer_elapsed(timer, NULL);

	MPI_Barrier(MPI_COMM_WORLD);

	g_timer_destroy(timer);

	delete_file();
}

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;
	GOptionGroup* group;

	gdouble read_result;
	gdouble write_result;

	GOptionEntry entries[] = {
		{ "julea", 0, 0, G_OPTION_ARG_NONE, &opt_julea, "Use JULEA I/O", NULL },
		{ "mpi", 0, 0, G_OPTION_ARG_NONE, &opt_mpi, "Use MPI-I/O", NULL },
		{ "posix", 0, 0, G_OPTION_ARG_NONE, &opt_posix, "Use POSIX I/O", NULL },
		{ "block-count", 0, 0, G_OPTION_ARG_INT, &opt_block_count, "Block count", "1000000" },
		{ "block-size", 0, 0, G_OPTION_ARG_INT, &opt_block_size, "Block size", "4096" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GOptionEntry entries_julea[] = {
		{ "julea-batch", 0, 0, G_OPTION_ARG_NONE, &opt_julea_batch, "Use batch mode", NULL },
		{ "julea-semantics", 0, 0, G_OPTION_ARG_STRING, &opt_julea_semantics, "Semantics to use", NULL },
		{ "julea-template", 0, 0, G_OPTION_ARG_STRING, &opt_julea_template, "Semantics template to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GOptionEntry entries_mpi[] = {
		{ "mpi-atomic", 0, 0, G_OPTION_ARG_NONE, &opt_mpi_atomic, "Use atomic mode", NULL },
		{ "mpi-path", 0, 0, G_OPTION_ARG_STRING, &opt_mpi_path, "Path to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GOptionEntry entries_posix[] = {
		{ "posix-path", 0, 0, G_OPTION_ARG_STRING, &opt_posix_path, "Path to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	gint thread_provided;

	MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thread_provided);

	g_assert(thread_provided == MPI_THREAD_SERIALIZED);

	MPI_Comm_rank(MPI_COMM_WORLD, &process_id);
	MPI_Comm_size(MPI_COMM_WORLD, &process_num);

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	group = g_option_group_new("julea", "JULEA Options", "Show JULEA help options", NULL, NULL);
	g_option_group_add_entries(group, entries_julea);
	g_option_context_add_group(context, group);

	group = g_option_group_new("mpi", "MPI-I/O Options", "Show MPI-I/O help options", NULL, NULL);
	g_option_group_add_entries(group, entries_mpi);
	g_option_context_add_group(context, group);

	group = g_option_group_new("posix", "POSIX Options", "Show POSIX help options", NULL, NULL);
	g_option_group_add_entries(group, entries_posix);
	g_option_context_add_group(context, group);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		g_option_context_free(context);

		if (error)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	if ((!opt_julea && !opt_mpi && !opt_posix)
	    || (opt_mpi && opt_mpi_path == NULL)
	    || (opt_posix && opt_posix_path == NULL)
	)
	{
		gchar* help;

		help = g_option_context_get_help(context, TRUE, NULL);
		g_option_context_free(context);

		g_print("%s", help);
		g_free(help);

		return 1;
	}

	g_option_context_free(context);

	if (process_num != 2)
	{
		MPI_Abort(MPI_COMM_WORLD, 1);
	}

	if (opt_julea)
	{
		j_init();
	}

	if (process_id == 0)
	{
		do_write(&write_result);
	}
	else if (process_id == 1)
	{
		do_read(&read_result);
	}

	if (opt_julea)
	{
		j_fini();
	}

	MPI_Sendrecv(&read_result, 1, MPI_DOUBLE, 0, 0, &read_result, 1, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

	if (process_id == 0)
	{
		gchar* read_str;
		gchar* size_str;
		gchar* total_size_str;
		gchar* write_str;
		guint64 bytes;

		bytes = (guint64)opt_block_size * opt_block_count;
		size_str = g_format_size(opt_block_size);
		total_size_str = g_format_size(bytes);
		read_str = g_format_size(bytes / read_result);
		write_str = g_format_size(bytes / write_result);

		g_print("Size:  %s * %d = %s\n", size_str, opt_block_count, total_size_str);
		g_print("Write: %.3fs = %s/s = %" G_GUINT64_FORMAT "\n", write_result, write_str, (guint64)(bytes / write_result));
		g_print("Read:  %.3fs = %s/s = %" G_GUINT64_FORMAT "\n", read_result, read_str, (guint64)(bytes / read_result));

		g_free(read_str);
		g_free(size_str);
		g_free(total_size_str);
		g_free(write_str);
	}

	MPI_Finalize();

	return 0;
}
