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
#include <glib-unix.h>
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <string.h>

#include <jcache-internal.h>
#include <jconfiguration.h>
#include <jmessage.h>
#include <jstatistics-internal.h>
#include <jthread-internal.h>
#include <jtrace.h>

#include "backend/backend.h"

static JStatistics* jd_statistics;

G_LOCK_DEFINE_STATIC(jd_statistics);

static
gboolean
jd_signal (gpointer data)
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
jd_on_run (GThreadedSocketService* service, GSocketConnection* connection, GObject* source_object, gpointer user_data)
{
	JCache* cache;
	JMessage* message;
	JThread* thread;
	JTrace* trace;
	GInputStream* input;
	GOutputStream* output;

	cache = j_cache_new(J_MIB(50));
	thread = j_thread_new(g_thread_self(), G_STRFUNC);
	trace = j_thread_get_trace(thread);

	message = j_message_new(J_MESSAGE_OPERATION_NONE, 0);
	input = g_io_stream_get_input_stream(G_IO_STREAM(connection));
	output = g_io_stream_get_output_stream(G_IO_STREAM(connection));

	while (j_message_read(message, input))
	{
		JBackendFile bf;
		gchar const* store;
		gchar const* collection;
		gchar const* item;
		guint32 operation_count;
		guint i;

		operation_count = j_message_operation_count(message);

		switch (j_message_operation_type(message))
		{
			case J_MESSAGE_OPERATION_NONE:
				break;
			case J_MESSAGE_OPERATION_CREATE:
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						item = j_message_get_string(message);

						if (jd_backend_create(&bf, store, collection, item, trace))
						{
							j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_FILES_CREATED, 1);
							jd_backend_close(&bf, trace);
						}
					}
				}
				break;
			case J_MESSAGE_OPERATION_DELETE:
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						JMessage* reply;

						item = j_message_get_string(message);

						jd_backend_open(&bf, store, collection, item, trace);

						if (jd_backend_delete(&bf, trace))
						{
							j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_FILES_DELETED, 1);
							jd_backend_close(&bf, trace);
						}

						reply = j_message_new_reply(message);
						j_message_write(reply, output);
						j_message_free(reply);
					}
				}
				break;
			case J_MESSAGE_OPERATION_READ:
				{
					JMessage* reply;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);


					jd_backend_open(&bf, store, collection, item, trace);

					for (i = 0; i < operation_count; i++)
					{
						gchar* buf;
						guint64 length;
						guint64 offset;
						guint64 bytes_read;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						buf = j_cache_get(cache, length);

						jd_backend_read(&bf, buf, length, offset, &bytes_read, trace);
						j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_BYTES_READ, bytes_read);

						// FIXME one big reply
						reply = j_message_new_reply(message);
						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_read);
						j_message_write(reply, output);
						j_message_free(reply);

						g_output_stream_write_all(output, buf, bytes_read, NULL, NULL, NULL);
						j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_BYTES_SENT, bytes_read);
					}

					jd_backend_close(&bf, trace);

					j_cache_clear(cache);
				}
				break;
			case J_MESSAGE_OPERATION_SYNC:
				{
					JMessage* reply;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					jd_backend_open(&bf, store, collection, item, trace);
					jd_backend_sync(&bf, trace);
					j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_SYNC, 1);
					reply = j_message_new_reply(message);
					j_message_write(reply, output);
					jd_backend_close(&bf, trace);

					j_message_free(reply);
				}
				break;
			case J_MESSAGE_OPERATION_WRITE:
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					jd_backend_open(&bf, store, collection, item, trace);

					for (i = 0; i < operation_count; i++)
					{
						gchar* buf;
						guint64 length;
						guint64 offset;
						guint64 bytes_written;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						buf = j_cache_get(cache, length);

						g_input_stream_read_all(input, buf, length, NULL, NULL, NULL);
						j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_BYTES_RECEIVED, length);

						jd_backend_write(&bf, buf, length, offset, &bytes_written, trace);
						j_statistics_set(j_thread_get_statistics(thread), J_STATISTICS_BYTES_WRITTEN, bytes_written);

						j_cache_clear(cache);
					}

					jd_backend_close(&bf, trace);
				}
				break;
			case J_MESSAGE_OPERATION_STATISTICS:
				{
					JMessage* reply;
					JStatistics* statistics;
					gchar get_all;
					guint64 value;

					get_all = j_message_get_1(message);

					if (get_all == 0)
					{
						statistics = j_thread_get_statistics(thread);
					}
					else
					{
						/* FIXME locking */
						statistics = jd_statistics;
					}

					reply = j_message_new_reply(message);
					j_message_add_operation(reply, 7 * sizeof(guint64));

					value = j_statistics_get(statistics, J_STATISTICS_FILES_CREATED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(statistics, J_STATISTICS_FILES_DELETED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(statistics, J_STATISTICS_SYNC);
					j_message_append_8(reply, &value);
					value = j_statistics_get(statistics, J_STATISTICS_BYTES_READ);
					j_message_append_8(reply, &value);
					value = j_statistics_get(statistics, J_STATISTICS_BYTES_WRITTEN);
					j_message_append_8(reply, &value);
					value = j_statistics_get(statistics, J_STATISTICS_BYTES_RECEIVED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(statistics, J_STATISTICS_BYTES_SENT);
					j_message_append_8(reply, &value);

					j_message_write(reply, output);
					j_message_free(reply);
				}
				break;
			case J_MESSAGE_OPERATION_REPLY:
			default:
				g_warn_if_reached();
				break;
		}
	}

	j_message_free(message);

	{
		JStatistics* statistics;
		guint64 value;

		statistics = j_thread_get_statistics(thread);

		G_LOCK(jd_statistics);

		value = j_statistics_get(statistics, J_STATISTICS_FILES_CREATED);
		j_statistics_set(jd_statistics, J_STATISTICS_FILES_CREATED, value);
		value = j_statistics_get(statistics, J_STATISTICS_FILES_DELETED);
		j_statistics_set(jd_statistics, J_STATISTICS_FILES_DELETED, value);
		value = j_statistics_get(statistics, J_STATISTICS_SYNC);
		j_statistics_set(jd_statistics, J_STATISTICS_SYNC, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_READ);
		j_statistics_set(jd_statistics, J_STATISTICS_BYTES_READ, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_WRITTEN);
		j_statistics_set(jd_statistics, J_STATISTICS_BYTES_WRITTEN, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_RECEIVED);
		j_statistics_set(jd_statistics, J_STATISTICS_BYTES_RECEIVED, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_SENT);
		j_statistics_set(jd_statistics, J_STATISTICS_BYTES_SENT, value);

		G_UNLOCK(jd_statistics);
	}

	j_cache_free(cache);
	j_thread_free(thread);

	return TRUE;
}

int
main (int argc, char** argv)
{
	gint opt_port = 4711;

	JConfiguration* configuration;
	JTrace* trace;
	GError* error = NULL;
	GMainLoop* main_loop;
	GModule* backend;
	GOptionContext* context;
	GSocketListener* listener;
	gchar* path;

	GOptionEntry entries[] = {
		{ "port", 'p', 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "4711" },
		{ NULL }
	};

	g_type_init();

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

	listener = G_SOCKET_LISTENER(g_threaded_socket_service_new(-1));

	if (!g_socket_listener_add_inet_port(listener, opt_port, NULL, &error))
	{
		g_object_unref(listener);

		if (error)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	j_trace_init("julead");
	trace = j_trace_thread_enter(NULL, G_STRFUNC);

	configuration = j_configuration_new();

	if (configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	path = g_module_build_path(JULEAD_BACKEND_PATH, j_configuration_get_storage_backend(configuration));
	backend = g_module_open(path, G_MODULE_BIND_LOCAL);
	g_free(path);

	g_module_symbol(backend, "backend_init", (gpointer*)&jd_backend_init);
	g_module_symbol(backend, "backend_fini", (gpointer*)&jd_backend_fini);
	g_module_symbol(backend, "backend_create", (gpointer*)&jd_backend_create);
	g_module_symbol(backend, "backend_delete", (gpointer*)&jd_backend_delete);
	g_module_symbol(backend, "backend_open", (gpointer*)&jd_backend_open);
	g_module_symbol(backend, "backend_close", (gpointer*)&jd_backend_close);
	g_module_symbol(backend, "backend_sync", (gpointer*)&jd_backend_sync);
	g_module_symbol(backend, "backend_read", (gpointer*)&jd_backend_read);
	g_module_symbol(backend, "backend_write", (gpointer*)&jd_backend_write);

	g_assert(jd_backend_init != NULL);
	g_assert(jd_backend_fini != NULL);
	g_assert(jd_backend_create != NULL);
	g_assert(jd_backend_delete != NULL);
	g_assert(jd_backend_open != NULL);
	g_assert(jd_backend_close != NULL);
	g_assert(jd_backend_sync != NULL);
	g_assert(jd_backend_read != NULL);
	g_assert(jd_backend_write != NULL);

	jd_backend_init(j_configuration_get_storage_path(configuration), trace);

	j_configuration_unref(configuration);

	jd_statistics = j_statistics_new(NULL);

	g_socket_service_start(G_SOCKET_SERVICE(listener));
	g_signal_connect(G_THREADED_SOCKET_SERVICE(listener), "run", G_CALLBACK(jd_on_run), NULL);

	main_loop = g_main_loop_new(NULL, FALSE);

	g_unix_signal_add(SIGHUP, jd_signal, main_loop);
	g_unix_signal_add(SIGINT, jd_signal, main_loop);
	g_unix_signal_add(SIGTERM, jd_signal, main_loop);

	g_main_loop_run(main_loop);
	g_main_loop_unref(main_loop);

	g_socket_service_stop(G_SOCKET_SERVICE(listener));
	g_object_unref(listener);

	j_statistics_free(jd_statistics);

	jd_backend_fini(trace);

	g_module_close(backend);

	j_trace_thread_leave(trace);
	j_trace_fini();

	return 0;
}
