/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <bson.h>
#include <mongo.h>

#include <string.h>

static guint stat_delete = 0;
static guint stat_read = 0;
static guint stat_update = 0;
static guint stat_write = 0;

static gboolean opt_sync = FALSE;
static gint opt_threads = 1;

static
gboolean
print_statistics (gpointer user_data)
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

	if (counter == 0)
	{
		printf("    delete      read    update     write     total\n");
	}

	printf("%10d%10d%10d%10d%10d\n", delete, read, update, write, delete + read + update + write);

	counter = (counter + 1) % 20;

	return TRUE;
}

static
void
benchmark_write (mongo* connection)
{
	bson index[1];
	bson** objects;
	mongo_write_concern write_concern[1];

	guint32 const num = (g_random_int() % 2000) + 1;

	gint ret;

	mongo_write_concern_init(write_concern);

	if (opt_sync)
	{
		write_concern->j = 1;
		write_concern->w = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(index);
	bson_append_int(index, "Number", 1);
	bson_finish(index);

	mongo_create_index(connection, "JULEA.Benchmark", index, 0, NULL);
	bson_destroy(index);

	objects = g_new(bson*, num);

	for (guint i = 0; i < num; i++)
	{
		objects[i] = g_slice_new(bson);
		bson_init(objects[i]);

		bson_append_int(objects[i], "Number", num);
		bson_append_int(objects[i], "Index", i);
		bson_append_string(objects[i], "Text", "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.");

		bson_finish(objects[i]);
	}

	ret = mongo_insert_batch(connection, "JULEA.Benchmark", (bson const**)objects, num, write_concern, 0);

	if (ret != MONGO_OK)
	{
		bson error[1];

		mongo_cmd_get_last_error(connection, "JULEA", error);
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
benchmark_read (mongo* connection)
{
	bson query[1];
	bson fields[1];
	mongo_cursor* cursor;

	guint32 const num = (g_random_int() % 1000) + 1;

	guint count = 0;

	bson_init(fields);
	bson_append_int(fields, "_id", 1);
	bson_append_int(fields, "Number", 1);
	bson_append_int(fields, "Index", 1);
	bson_append_int(fields, "Text", 1);
	bson_finish(fields);

	bson_init(query);
	bson_append_int(query, "Number", num);
	bson_finish(query);

	cursor = mongo_find(connection, "JULEA.Benchmark", query, fields, num, 0, 0);

	bson_destroy(fields);
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
benchmark_update (mongo* connection)
{
	bson cond[1];
	bson error[1];
	bson op[1];
	bson_iterator iterator[1];
	mongo_write_concern write_concern[1];

	guint32 const num = (g_random_int() % 1000) + 1;

	guint count;
	gint ret;

	mongo_write_concern_init(write_concern);

	if (opt_sync)
	{
		write_concern->j = 1;
		write_concern->w = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(cond);
	bson_append_int(cond, "Number", num);
	bson_finish(cond);

	bson_init(op);
	bson_append_start_object(op, "$set");
	bson_append_string(op, "Text", "foobar");
	bson_append_finish_object(op);
	bson_finish(op);

	ret = mongo_update(connection, "JULEA.Benchmark", cond, op, MONGO_UPDATE_MULTI, write_concern);
	g_assert(ret == MONGO_OK);

	mongo_cmd_get_last_error(connection, "JULEA", error);
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
benchmark_delete (mongo* connection)
{
	bson cond[1];
	bson error[1];
	bson_iterator iterator[1];
	mongo_write_concern write_concern[1];

	guint32 const num = (g_random_int() % 1000) + 1;

	guint count;
	gint ret;

	mongo_write_concern_init(write_concern);

	if (opt_sync)
	{
		write_concern->j = 1;
		write_concern->w = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(cond);
	bson_append_int(cond, "Number", num);
	bson_finish(cond);

	ret = mongo_remove(connection, "JULEA.Benchmark", cond, write_concern);
	g_assert(ret == MONGO_OK);

	mongo_cmd_get_last_error(connection, "JULEA", error);
	bson_find(iterator, error, "n");
	count = bson_iterator_int(iterator);
	bson_destroy(error);

	bson_destroy(cond);

	mongo_write_concern_destroy(write_concern);

	g_atomic_int_add(&stat_delete, count);
}

static
gpointer
benchmark_thread (gpointer data)
{
	mongo connection[1];

	mongo_init(connection);

	if (mongo_connect(connection, "localhost", 27017) != MONGO_OK)
	{
		goto end;
	}

	while (TRUE)
	{
		guint32 op;

		op = g_random_int() % 4;

		switch (op)
		{
			case 0:
				benchmark_write(connection);
				break;
			case 1:
				benchmark_read(connection);
				break;
			case 2:
				benchmark_update(connection);
				break;
			case 3:
				benchmark_delete(connection);
				break;
			default:
				g_warn_if_reached();
				break;
		}
	}

end:
	if (mongo_check_connection(connection) == MONGO_OK)
	{
		mongo_disconnect(connection);
	}

	mongo_destroy(connection);

	return NULL;
}

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "sync", 's', 0, G_OPTION_ARG_NONE, &opt_sync, "Whether to sync", NULL },
		{ "threads", 't', 0, G_OPTION_ARG_INT, &opt_threads, "Number of threads to use", "1" },
		{ NULL }
	};

	GMainLoop* main_loop;
	GThread** threads;

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

	threads = g_new(GThread*, opt_threads);

	for (gint i = 0; i < opt_threads; i++)
	{
		threads[i] = g_thread_new("test", benchmark_thread, NULL);
	}

	main_loop = g_main_loop_new(NULL, FALSE);
	g_timeout_add_seconds(1, print_statistics, NULL);

	g_main_loop_run(main_loop);

	for (gint i = 0; i < opt_threads; i++)
	{
		g_thread_join(threads[i]);
		g_thread_unref(threads[i]);
	}

	g_main_loop_unref(main_loop);

	g_free(threads);

	return 0;
}
