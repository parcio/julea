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

#define _POSIX_SOURCE

#include <glib.h>
#include <glib-unix.h>
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <string.h>

#include <jconfiguration.h>
#include <jmessage.h>
#include <jmessage-reply.h>
#include <jtrace.h>

#include "backend/backend.h"

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
	JMessage* message;
	JTrace* trace;
	GInputStream* input;
	GOutputStream* output;

	trace = j_trace_thread_enter(g_thread_self(), G_STRFUNC);

	message = j_message_new(1024 * 1024, J_MESSAGE_OPERATION_NONE, 0);
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
				g_printerr("none_op\n");
				break;
			case J_MESSAGE_OPERATION_CREATE:
				g_printerr("create_op\n");
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						item = j_message_get_string(message);

						g_printerr("CREATE %s %s %s\n", store, collection, item);

						jd_backend_create(store, collection, item, trace);
					}
				}
				break;
			case J_MESSAGE_OPERATION_DELETE:
				g_printerr("delete_op\n");
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						JMessageReply* reply;

						item = j_message_get_string(message);

						g_printerr("DELETE %s %s %s\n", store, collection, item);

						jd_backend_open(&bf, store, collection, item, trace);

						jd_backend_delete(&bf, trace);

						reply = j_message_reply_new(message, 0);
						j_message_reply_write(reply, output);
						j_message_reply_free(reply);

						jd_backend_close(&bf, trace);
					}
				}
				break;
			case J_MESSAGE_OPERATION_READ:
				g_printerr("read_op\n");
				{
					JMessageReply* reply;
					gchar* buf;

					/* FIXME allow different items? */
					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					buf = g_new(gchar, 512 * 1024);

					jd_backend_open(&bf, store, collection, item, trace);

					for (i = 0; i < operation_count; i++)
					{
						guint64 length;
						guint64 offset;
						guint64 bytes_read;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						g_printerr("READ %s %s %s %ld %ld\n", store, collection, item, length, offset);

						jd_backend_read(&bf, buf, length, offset, &bytes_read, trace);
						j_trace_counter(trace, "julead_read", bytes_read);

						// FIXME one big reply
						reply = j_message_reply_new(message, bytes_read);
						j_message_reply_write(reply, output);
						g_output_stream_write_all(output, buf, bytes_read, NULL, NULL, NULL);
						j_trace_counter(trace, "julead_sent", bytes_read);
						j_message_reply_free(reply);
					}

					jd_backend_close(&bf, trace);

					g_free(buf);
				}
				break;
			case J_MESSAGE_OPERATION_SYNC:
				g_printerr("sync_op\n");
				{
					JMessageReply* reply;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					g_printerr("SYNC %s %s %s\n", store, collection, item);

					jd_backend_open(&bf, store, collection, item, trace);
					jd_backend_sync(&bf, trace);
					reply = j_message_reply_new(message, 0);
					j_message_reply_write(reply, output);
					jd_backend_close(&bf, trace);

					j_message_reply_free(reply);
				}
				break;
			case J_MESSAGE_OPERATION_WRITE:
				g_printerr("write_op\n");
				{
					gchar* buf;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					buf = g_new(gchar, 512 * 1024);

					jd_backend_open(&bf, store, collection, item, trace);

					for (i = 0; i < operation_count; i++)
					{
						guint64 length;
						guint64 offset;
						guint64 bytes_written;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						g_printerr("WRITE %s %s %s %ld %ld\n", store, collection, item, length, offset);

						g_input_stream_read_all(input, buf, length, NULL, NULL, NULL);
						j_trace_counter(trace, "julead_received", length);

						jd_backend_write(&bf, buf, length, offset, &bytes_written, trace);
						j_trace_counter(trace, "julead_written", bytes_written);
					}

					jd_backend_close(&bf, trace);

					g_free(buf);
				}
				break;
			default:
				g_warn_if_reached();
				break;
		}
	}

	j_message_free(message);

	j_trace_thread_leave(trace);

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

	if (!g_thread_get_initialized())
	{
		g_thread_init(NULL);
	}

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

	listener = G_SOCKET_LISTENER(g_threaded_socket_service_new(-1));
	g_socket_listener_add_inet_port(listener, opt_port, NULL, NULL);
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

	jd_backend_fini(trace);

	g_module_close(backend);

	j_trace_thread_leave(trace);
	j_trace_fini();

	return 0;
}
