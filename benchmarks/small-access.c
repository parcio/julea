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

#include <string.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

static gboolean opt_julea = FALSE;
static gboolean opt_mpi_io = FALSE;
static gboolean opt_posix = FALSE;

static gint opt_block_size = 4096;
static gboolean opt_shared = FALSE;

static gchar* opt_julea_semantics = NULL;
static gchar* opt_julea_template = NULL;

static gchar* opt_mpi_io_path = NULL;

static gchar* opt_posix_path = NULL;

static guint kill_thread = 0;

static guint stat_read = 0;
static guint stat_update = 0;
static guint stat_write = 0;

static gint process_id = 0;
static gint process_num = 1;

static
gboolean
handle_signal (gpointer data)
{
	GMainLoop* main_loop = data;

	if (g_main_loop_is_running(main_loop))
	{
		g_main_loop_quit(main_loop);
	}

	return FALSE;
}

static
gboolean
print_statistics (G_GNUC_UNUSED gpointer user_data)
{
	static guint counter = 0;

	guint read;
	guint update;
	guint write;

	read = g_atomic_int_and(&stat_read, 0);
	update = g_atomic_int_and(&stat_update, 0);
	write = g_atomic_int_and(&stat_write, 0);

	if (counter == 0 && process_id == 0)
	{
#ifdef HAVE_MPI
		g_print("process     write      read    update     total\n");
#else
		g_print("     write      read    update     total\n");
#endif
	}

#ifdef HAVE_MPI
	g_print("%7d%10d%10d%10d%10d\n", process_id, write, read, update, write + read + update);
#else
	g_print("%10d%10d%10d%10d\n", write, read, update, write + read + update);
#endif

	counter = (counter + 1) % 20;

	return TRUE;
}

static
guint64
get_offset (guint64 iteration)
{
	if (opt_shared)
	{
		return opt_block_size * iteration;
	}
	else
	{
		return opt_block_size * ((process_num * iteration) + process_id);
	}
}

static
gchar const*
get_name (void)
{
	static gchar* name = NULL;

	if (G_UNLIKELY(name == NULL))
	{
		if (opt_shared)
		{
			name = g_strdup("benchmark");
		}
		else
		{
			name = g_strdup_printf("benchmark%d", process_id);
		}
	}

	return name;
}

static
gpointer
thread_julea (G_GNUC_UNUSED gpointer data)
{
	gchar buf[opt_block_size];

	JBatch* batch;
	JCollection* collection;
	JItem* item;
	JSemantics* semantics;
	JStore* store;
	guint64 iteration = 0;

	j_init(NULL, NULL);

	if (!opt_shared || process_id == 0)
	{
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		store = j_store_new("benchmark");
		collection = j_collection_new("benchmark");
		item = j_item_new(get_name());

		j_create_store(store, batch);
		j_store_create_collection(store, collection, batch);
		j_collection_create_item(collection, item, batch);

		j_item_unref(item);
		j_collection_unref(collection);
		j_store_unref(store);

		j_batch_execute(batch);
		j_batch_unref(batch);
	}

#ifdef HAVE_MPI
	MPI_Barrier(MPI_COMM_WORLD);
#endif

	semantics = j_helper_parse_semantics(opt_julea_template, opt_julea_semantics);
	batch = j_batch_new(semantics);

	j_get_store(&store, "benchmark", batch);
	j_batch_execute(batch);

	j_store_get_collection(store, &collection, "benchmark", batch);
	j_batch_execute(batch);

	j_collection_get_item(collection, &item, get_name(), batch);
	j_batch_execute(batch);

	if (item == NULL)
	{
		g_error("ERROR %d\n", process_id);
	}

	while (TRUE)
	{
		if (g_atomic_int_get(&kill_thread) == 1)
		{
			goto end;
		}

		for (guint i = 0; i < 1000; i++)
		{
			guint64 bytes;

			j_item_write(item, buf, opt_block_size, get_offset(iteration + i), &bytes, batch);
			j_batch_execute(batch);

			g_atomic_int_add(&stat_write, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		for (guint i = 0; i < 1000; i++)
		{
			guint64 bytes;
			gint other_process = (process_id + 1) % process_num;

			j_item_read(item, buf, opt_block_size, opt_block_size * ((process_num * (iteration + i)) + other_process), &bytes, batch);
			j_batch_execute(batch);

			g_atomic_int_add(&stat_read, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		for (guint i = 0; i < 1000; i++)
		{
			guint64 bytes;
			gint other_process = (process_id + 1) % process_num;

			j_item_write(item, buf, opt_block_size, opt_block_size * ((process_num * (iteration + i)) + other_process), &bytes, batch);
			j_batch_execute(batch);

			g_atomic_int_add(&stat_update, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		iteration += 1000;
	}

end:
	j_batch_unref(batch);

	j_semantics_unref(semantics);

	if (!opt_shared || process_id == 0)
	{
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		j_collection_delete_item(collection, item, batch);
		j_store_delete_collection(store, collection, batch);
		j_delete_store(store, batch);

		j_batch_execute(batch);
		j_batch_unref(batch);
	}

	j_store_unref(store);
	j_collection_unref(collection);
	j_item_unref(item);

	j_fini();

	return NULL;
}

static
gpointer
thread_mpi_io (G_GNUC_UNUSED gpointer data)
{
#ifdef HAVE_MPI
	gchar buf[opt_block_size];

	MPI_Comm comm;
	MPI_File file[1];
	MPI_Status status[1];
	gchar* path;
	guint64 iteration = 0;
	gint ret;

	comm = (opt_shared) ? MPI_COMM_WORLD : MPI_COMM_SELF;

	if (!opt_shared || process_id == 0)
	{
		path = g_build_filename(opt_mpi_io_path, get_name(), NULL);
		MPI_File_open(comm, path, MPI_MODE_RDWR | MPI_MODE_CREATE, MPI_INFO_NULL, file);

		MPI_File_close(file);
		g_free(path);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	path = g_build_filename(opt_posix_path, get_name(), NULL);
	ret = MPI_File_open(comm, path, MPI_MODE_RDWR, MPI_INFO_NULL, file);

	if (ret != MPI_SUCCESS)
	{
		g_error("ERROR %d\n", process_id);
	}

	while (TRUE)
	{
		if (g_atomic_int_get(&kill_thread) == 1)
		{
			goto end;
		}

		for (guint i = 0; i < 1000; i++)
		{
			gint bytes;

			MPI_File_write_at(file[0], get_offset(iteration + i), buf, opt_block_size, MPI_CHAR, status);
			MPI_Get_count(status, MPI_CHAR, &bytes);

			g_atomic_int_add(&stat_write, bytes);
		}

		MPI_Barrier(MPI_COMM_WORLD);

		for (guint i = 0; i < 1000; i++)
		{
			gint bytes;
			gint other_process = (process_id + 1) % process_num;

			MPI_File_write_at(file[0], opt_block_size * ((process_num * (iteration + i)) + other_process), buf, opt_block_size, MPI_CHAR, status);
			MPI_Get_count(status, MPI_CHAR, &bytes);

			g_atomic_int_add(&stat_read, bytes);
		}

		MPI_Barrier(MPI_COMM_WORLD);

		for (guint i = 0; i < 1000; i++)
		{
			gint bytes;
			gint other_process = (process_id + 1) % process_num;

			MPI_File_write_at(file[0], opt_block_size * ((process_num * (iteration + i)) + other_process), buf, opt_block_size, MPI_CHAR, status);
			MPI_Get_count(status, MPI_CHAR, &bytes);

			g_atomic_int_add(&stat_update, bytes);
		}

		MPI_Barrier(MPI_COMM_WORLD);

		iteration += 1000;
	}

end:
	if (!opt_shared || process_id == 0)
	{
		unlink(path);
	}

	MPI_File_close(file);
	g_free(path);

#endif
	return NULL;
}

static
gpointer
thread_posix (G_GNUC_UNUSED gpointer data)
{
	gchar buf[opt_block_size];

	gchar* path;
	gint fd;
	guint64 iteration = 0;

	if (!opt_shared || process_id == 0)
	{
		path = g_build_filename(opt_posix_path, get_name(), NULL);
		fd = open(path, O_RDWR | O_CREAT, 0600);

		close(fd);
		g_free(path);
	}

#ifdef HAVE_MPI
	MPI_Barrier(MPI_COMM_WORLD);
#endif

	path = g_build_filename(opt_posix_path, get_name(), NULL);
	fd = open(path, O_RDWR);

	if (fd == -1)
	{
		g_error("ERROR %d\n", process_id);
	}

	while (TRUE)
	{
		if (g_atomic_int_get(&kill_thread) == 1)
		{
			goto end;
		}

		for (guint i = 0; i < 1000; i++)
		{
			guint64 bytes;

			bytes = pwrite(fd, buf, opt_block_size, get_offset(iteration + i));

			g_atomic_int_add(&stat_write, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		for (guint i = 0; i < 1000; i++)
		{
			guint64 bytes;
			gint other_process = (process_id + 1) % process_num;

			bytes = pread(fd, buf, opt_block_size, opt_block_size * ((process_num * (iteration + i)) + other_process));

			g_atomic_int_add(&stat_read, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		for (guint i = 0; i < 1000; i++)
		{
			guint64 bytes;
			gint other_process = (process_id + 1) % process_num;

			bytes = pwrite(fd, buf, opt_block_size, opt_block_size * ((process_num * (iteration + i)) + other_process));

			g_atomic_int_add(&stat_update, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		iteration += 1000;
	}

end:
	if (!opt_shared || process_id == 0)
	{
		unlink(path);
	}

	close(fd);
	g_free(path);

	return NULL;
}

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;
	GOptionGroup* group;

	GOptionEntry entries[] = {
		{ "julea", 0, 0, G_OPTION_ARG_NONE, &opt_julea, "Use JULEA I/O", NULL },
#ifdef HAVE_MPI
		{ "mpi-io", 0, 0, G_OPTION_ARG_NONE, &opt_mpi_io, "Use MPI-I/O", NULL },
#endif
		{ "posix", 0, 0, G_OPTION_ARG_NONE, &opt_posix, "Use POSIX I/O", NULL },
		{ "block-size", 0, 0, G_OPTION_ARG_INT, &opt_block_size, "Block size", "4096" },
		{ "shared", 0, 0, G_OPTION_ARG_NONE, &opt_shared, "Use shared access", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GOptionEntry entries_julea[] = {
		{ "julea-semantics", 0, 0, G_OPTION_ARG_STRING, &opt_julea_semantics, "Semantics to use", NULL },
		{ "julea-template", 0, 0, G_OPTION_ARG_STRING, &opt_julea_template, "Semantics template to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GOptionEntry entries_mpi_io[] = {
		{ "mpi-io-path", 0, 0, G_OPTION_ARG_STRING, &opt_mpi_io_path, "Path to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GOptionEntry entries_posix[] = {
		{ "posix-path", 0, 0, G_OPTION_ARG_STRING, &opt_posix_path, "Path to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GMainLoop* main_loop;
	GThread* thread = NULL;

#ifdef HAVE_MPI
	{
		gint thread_provided;

		MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thread_provided);

		if (thread_provided != MPI_THREAD_SERIALIZED)
		{
			return 1;
		}

		MPI_Comm_rank(MPI_COMM_WORLD, &process_id);
		MPI_Comm_size(MPI_COMM_WORLD, &process_num);
	}
#endif

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	group = g_option_group_new("julea", "JULEA Options", "Show JULEA help options", NULL, NULL);
	g_option_group_add_entries(group, entries_julea);
	g_option_context_add_group(context, group);

	group = g_option_group_new("mpi-io", "MPI-I/O Options", "Show MPI-I/O help options", NULL, NULL);
	g_option_group_add_entries(group, entries_mpi_io);
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

	if ((!opt_julea && !opt_mpi_io && !opt_posix)
	    || (opt_mpi_io && opt_mpi_io_path == NULL)
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

	if (opt_julea)
	{
		thread = g_thread_new("test", thread_julea, NULL);
	}
	else if (opt_mpi_io)
	{
		thread = g_thread_new("test", thread_mpi_io, NULL);
	}
	else if (opt_posix)
	{
		thread = g_thread_new("test", thread_posix, NULL);
	}

	main_loop = g_main_loop_new(NULL, FALSE);
	g_timeout_add_seconds(1, print_statistics, NULL);

	g_unix_signal_add(SIGHUP, handle_signal, main_loop);
	g_unix_signal_add(SIGINT, handle_signal, main_loop);
	g_unix_signal_add(SIGTERM, handle_signal, main_loop);

	g_main_loop_run(main_loop);

	g_atomic_int_set(&kill_thread, 1);

	if (thread != NULL)
	{
		g_thread_join(thread);
	}

	g_main_loop_unref(main_loop);

#ifdef HAVE_MPI
	MPI_Finalize();
#endif

	return 0;
}
