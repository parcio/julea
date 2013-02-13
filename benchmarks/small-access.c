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

#include <julea-config.h>

#include <glib.h>
#include <glib-unix.h>

#include <julea.h>

#include <jsemantics-internal.h>

#include <string.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

static gchar* opt_semantics = NULL;
static gchar* opt_template = NULL;

static JSemantics* semantics = NULL;

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
gpointer
benchmark_thread (G_GNUC_UNUSED gpointer data)
{
	guint64 const block_size = 4096;
	gchar buf[block_size];

	JBatch* batch;
	JCollection* collection;
	JItem* item;
	JStore* store;
	guint64 offset = 0;

#ifdef HAVE_MPI
	MPI_Barrier(MPI_COMM_WORLD);
#endif

	batch = j_batch_new(semantics);

	j_get_store(&store, "benchmark", batch);
	j_batch_execute(batch);

	j_store_get_collection(store, &collection, "benchmark", batch);
	j_batch_execute(batch);

	j_collection_get_item(collection, &item, "benchmark", batch);
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

			j_item_write(item, buf, block_size, block_size * ((process_num * (offset + i)) + process_id), &bytes, batch);
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

			j_item_read(item, buf, block_size, block_size * ((process_num * (offset + i)) + other_process), &bytes, batch);
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

			j_item_write(item, buf, block_size, block_size * ((process_num * (offset + i)) + other_process), &bytes, batch);
			j_batch_execute(batch);

			g_atomic_int_add(&stat_update, bytes);
		}

#ifdef HAVE_MPI
		MPI_Barrier(MPI_COMM_WORLD);
#endif

		offset += 1000;
	}

end:
	j_store_unref(store);
	j_collection_unref(collection);
	j_item_unref(item);

	j_batch_unref(batch);

	return NULL;
}

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "semantics", 0, 0, G_OPTION_ARG_STRING, &opt_semantics, "Semantics to use", NULL },
		{ "template", 0, 0, G_OPTION_ARG_STRING, &opt_template, "Semantics template to use", "default" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	JBatch* batch;
	JCollection* collection;
	JItem* item;
	JStore* store;
	GMainLoop* main_loop;
	GThread* thread;

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

	g_option_context_free(context);

	semantics = j_semantics_parse(opt_template, opt_semantics);

	j_init(&argc, &argv);

	if (process_id == 0)
	{
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		store = j_store_new("benchmark");
		collection = j_collection_new("benchmark");
		item = j_item_new("benchmark");

		j_create_store(store, batch);
		j_store_create_collection(store, collection, batch);
		j_collection_create_item(collection, item, batch);

		j_batch_execute(batch);
	}

	thread = g_thread_new("test", benchmark_thread, NULL);

	main_loop = g_main_loop_new(NULL, FALSE);
	g_timeout_add_seconds(1, print_statistics, NULL);

	g_unix_signal_add(SIGHUP, handle_signal, main_loop);
	g_unix_signal_add(SIGINT, handle_signal, main_loop);
	g_unix_signal_add(SIGTERM, handle_signal, main_loop);

	g_main_loop_run(main_loop);

	g_atomic_int_set(&kill_thread, 1);

	g_thread_join(thread);

	g_main_loop_unref(main_loop);

	if (process_id == 0)
	{
		j_collection_delete_item(collection, item, batch);
		j_store_delete_collection(store, collection, batch);
		j_delete_store(store, batch);

		j_item_unref(item);
		j_collection_unref(collection);
		j_store_unref(store);

		j_batch_execute(batch);

		j_batch_unref(batch);
	}

	j_fini();

#ifdef HAVE_MPI
	MPI_Finalize();
#endif

	return 0;
}
