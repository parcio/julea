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

#include <bson.h>
#include <mongo.h>

#include <string.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

struct MongoNS
{
	gchar* db;
	gchar* collection;
	gchar* full;
};

typedef struct MongoNS MongoNS;

struct ThreadData
{
	guint id;
	mongo connection[1];
	MongoNS ns;
	GRand* rand;
};

typedef struct ThreadData ThreadData;

static guint kill_threads = 0;
static guint thread_id = 0;

static guint stat_delete = 0;
static guint stat_read = 0;
static guint stat_update = 0;
static guint stat_write = 0;

static gchar* opt_host = NULL;
static gboolean opt_sync = FALSE;
static gint opt_threads = 1;

static gint process_id = 0;

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

	guint delete;
	guint read;
	guint update;
	guint write;

	delete = g_atomic_int_and(&stat_delete, 0);
	read = g_atomic_int_and(&stat_read, 0);
	update = g_atomic_int_and(&stat_update, 0);
	write = g_atomic_int_and(&stat_write, 0);

	if (counter == 0 && process_id == 0)
	{
#ifdef HAVE_MPI
		printf("process    delete      read    update     write     total\n");
#else
		printf("    delete      read    update     write     total\n");
#endif
	}

#ifdef HAVE_MPI
	printf("%7d%10d%10d%10d%10d%10d\n", process_id, delete, read, update, write, delete + read + update + write);
#else
	printf("%10d%10d%10d%10d%10d\n", delete, read, update, write, delete + read + update + write);
#endif

	counter = (counter + 1) % 20;

	return TRUE;
}

static
void
benchmark_write (ThreadData* thread_data)
{
	bson index[1];
	bson** objects;
	mongo_write_concern write_concern[1];

	guint32 const num = g_rand_int_range(thread_data->rand, 1, 5000);

	gint ret;

	mongo_write_concern_init(write_concern);

	write_concern->w = 1;

	if (opt_sync)
	{
		write_concern->j = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(index);
	bson_append_int(index, "Number", 1);
	bson_finish(index);

	mongo_create_index(thread_data->connection, thread_data->ns.full, index, 0, NULL);
	bson_destroy(index);

	objects = g_new(bson*, num);

	for (guint i = 0; i < num; i++)
	{
		objects[i] = g_slice_new(bson);
		bson_init(objects[i]);

		bson_append_int(objects[i], "Number", num);
		bson_append_int(objects[i], "Index", i);

		bson_append_string(objects[i], "Name", "Lorem ipsum dolor sit amet.");

		bson_append_start_object(objects[i], "Status");
		bson_append_long(objects[i], "ModificationTime", g_get_real_time());
		bson_append_long(objects[i], "Size", num);
		bson_append_finish_object(objects[i]);

		bson_append_start_object(objects[i], "Credentials");
		bson_append_int(objects[i], "User", num);
		bson_append_int(objects[i], "Group", num);
		bson_append_finish_object(objects[i]);

		bson_finish(objects[i]);
	}

	ret = mongo_insert_batch(thread_data->connection, thread_data->ns.full, (bson const**)objects, num, write_concern, 0);

	if (ret != MONGO_OK)
	{
		bson error[1];

		mongo_cmd_get_last_error(thread_data->connection, thread_data->ns.db, error);
		bson_print(error);
		bson_destroy(error);
	}

	g_assert(ret == MONGO_OK);

	for (guint i = 0; i < num; i++)
	{
		bson_destroy(objects[i]);
		g_slice_free(bson, objects[i]);
	}

	g_free(objects);

	mongo_write_concern_destroy(write_concern);

	g_atomic_int_add(&stat_write, num);
}

static
void
benchmark_read (ThreadData* thread_data)
{
	bson query[1];
	mongo_cursor* cursor;

	guint32 const num = g_rand_int_range(thread_data->rand, 1, 5000);

	guint count = 0;

	bson_init(query);
	bson_append_int(query, "Number", num);
	bson_finish(query);

	cursor = mongo_find(thread_data->connection, thread_data->ns.full, query, NULL, num, 0, 0);

	bson_destroy(query);

	while (mongo_cursor_next(cursor) == MONGO_OK)
	{
		count++;
		mongo_cursor_bson(cursor);
	}

	mongo_cursor_destroy(cursor);

	g_atomic_int_add(&stat_read, count);
}

static
void
benchmark_update (ThreadData* thread_data)
{
	bson cond[1];
	bson error[1];
	bson op[1];
	bson_iterator iterator[1];
	mongo_write_concern write_concern[1];

	guint32 const num = g_rand_int_range(thread_data->rand, 1, 5000);

	guint count;
	gint ret;

	mongo_write_concern_init(write_concern);

	write_concern->w = 1;

	if (opt_sync)
	{
		write_concern->j = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(cond);
	bson_append_int(cond, "Number", num);
	bson_finish(cond);

	bson_init(op);

	bson_append_start_object(op, "$set");
	bson_append_string(op, "Name", "At vero eos et accusam et justo duo dolores et ea rebum.");
	bson_append_long(op, "Status.ModificationTime", g_get_real_time());
	bson_append_finish_object(op);

	bson_append_start_object(op, "$inc");
	bson_append_long(op, "Status.Size", 1);
	bson_append_finish_object(op);

	bson_finish(op);

	ret = mongo_update(thread_data->connection, thread_data->ns.full, cond, op, MONGO_UPDATE_MULTI, write_concern);

	if (ret != MONGO_OK)
	{
		mongo_cmd_get_last_error(thread_data->connection, thread_data->ns.db, error);
		bson_print(error);
		bson_destroy(error);
	}

	g_assert(ret == MONGO_OK);

	mongo_cmd_get_last_error(thread_data->connection, thread_data->ns.db, error);
	bson_find(iterator, error, "n");
	count = bson_iterator_int(iterator);
	bson_destroy(error);

	bson_destroy(cond);
	bson_destroy(op);

	mongo_write_concern_destroy(write_concern);

	g_atomic_int_add(&stat_update, count);
}

static
void
benchmark_delete (ThreadData* thread_data)
{
	bson cond[1];
	bson error[1];
	bson_iterator iterator[1];
	mongo_write_concern write_concern[1];

	guint32 const num = g_rand_int_range(thread_data->rand, 1, 5000);

	guint count;
	gint ret;

	mongo_write_concern_init(write_concern);

	write_concern->w = 1;

	if (opt_sync)
	{
		write_concern->j = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(cond);
	bson_append_int(cond, "Number", num);
	bson_finish(cond);

	ret = mongo_remove(thread_data->connection, thread_data->ns.full, cond, write_concern);
	g_assert(ret == MONGO_OK);

	mongo_cmd_get_last_error(thread_data->connection, thread_data->ns.db, error);
	bson_find(iterator, error, "n");
	count = bson_iterator_int(iterator);
	bson_destroy(error);

	bson_destroy(cond);

	mongo_write_concern_destroy(write_concern);

	g_atomic_int_add(&stat_delete, count);
}

static
gpointer
benchmark_thread (G_GNUC_UNUSED gpointer data)
{
	ThreadData thread_data;

	thread_data.id = g_atomic_int_add(&thread_id, 1);

	thread_data.ns.db = g_strdup_printf("benchmark%d", process_id);
	thread_data.ns.collection = g_strdup_printf("benchmark%d", thread_data.id);
	thread_data.ns.full = g_strdup_printf("%s.%s", thread_data.ns.db, thread_data.ns.collection);

	thread_data.rand = g_rand_new_with_seed(42 + thread_data.id);

	mongo_init(thread_data.connection);

	if (mongo_client(thread_data.connection, opt_host, 27017) != MONGO_OK)
	{
		goto end;
	}

	mongo_cmd_drop_collection(thread_data.connection, thread_data.ns.db, thread_data.ns.collection, NULL);
	mongo_cmd_drop_db(thread_data.connection, thread_data.ns.db);

	while (TRUE)
	{
		guint32 op;

		if (g_atomic_int_get(&kill_threads) == 1)
		{
			goto end;
		}

		op = g_rand_int_range(thread_data.rand, 0, 4);

		switch (op)
		{
			case 0:
				benchmark_delete(&thread_data);
				break;
			case 1:
				benchmark_read(&thread_data);
				break;
			case 2:
				benchmark_update(&thread_data);
				break;
			case 3:
				benchmark_write(&thread_data);
				break;
			default:
				g_warn_if_reached();
				break;
		}
	}

end:
	mongo_cmd_drop_collection(thread_data.connection, thread_data.ns.db, thread_data.ns.collection, NULL);
	mongo_cmd_drop_db(thread_data.connection, thread_data.ns.db);

	if (mongo_check_connection(thread_data.connection) == MONGO_OK)
	{
		mongo_disconnect(thread_data.connection);
	}

	mongo_destroy(thread_data.connection);

	g_rand_free(thread_data.rand);

	g_free(thread_data.ns.full);
	g_free(thread_data.ns.collection);
	g_free(thread_data.ns.db);

	return NULL;
}

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "host", 0, 0, G_OPTION_ARG_STRING, &opt_host, "MongoDB host", "localhost" },
		{ "sync", 0, 0, G_OPTION_ARG_NONE, &opt_sync, "Whether to sync", NULL },
		{ "threads", 0, 0, G_OPTION_ARG_INT, &opt_threads, "Number of threads to use", "1" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	GMainLoop* main_loop;
	GThread** threads;

#ifdef HAVE_MPI
	{
		gint thread_provided;

		MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &thread_provided);

		g_assert(thread_provided == MPI_THREAD_FUNNELED);

		MPI_Comm_rank(MPI_COMM_WORLD, &process_id);
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

	if (opt_host == NULL)
	{
		opt_host = g_strdup("localhost");
	}

	threads = g_new(GThread*, opt_threads);

	for (gint i = 0; i < opt_threads; i++)
	{
		threads[i] = g_thread_new("test", benchmark_thread, NULL);
	}

	main_loop = g_main_loop_new(NULL, FALSE);
	g_timeout_add_seconds(1, print_statistics, NULL);

	g_unix_signal_add(SIGHUP, handle_signal, main_loop);
	g_unix_signal_add(SIGINT, handle_signal, main_loop);
	g_unix_signal_add(SIGTERM, handle_signal, main_loop);

	g_main_loop_run(main_loop);

	g_atomic_int_set(&kill_threads, 1);

	for (gint i = 0; i < opt_threads; i++)
	{
		g_thread_join(threads[i]);
	}

	g_main_loop_unref(main_loop);

	g_free(threads);

	g_free(opt_host);

#ifdef HAVE_MPI
	MPI_Finalize();
#endif

	return 0;
}
