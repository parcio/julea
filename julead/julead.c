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
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <signal.h>
#include <string.h>

#include <jconfiguration.h>
#include <jmessage.h>
#include <jmessage-reply.h>
#include <jtrace.h>

#include "backend/backend.h"

static gint opt_port = 4711;

static GModule* backend = NULL;
static GMainLoop* main_loop;

static
void
jd_signal (int signo)
{
	if (g_main_loop_is_running(main_loop))
	{
		g_main_loop_quit(main_loop);
	}
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

		switch (j_message_operation_type(message))
		{
			case J_MESSAGE_OPERATION_NONE:
				g_printerr("none_op\n");
				break;
			case J_MESSAGE_OPERATION_READ:
				g_printerr("read_op\n");
				{
					JMessageReply* reply;
					gchar* buf;
					guint64 length;
					guint64 offset;
					guint64 bytes_read;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);
					length = j_message_get_8(message);
					offset = j_message_get_8(message);

					buf = g_new(gchar, 512 * 1024);

					g_printerr("READ %s %s %s %ld %ld\n", store, collection, item, length, offset);

					jd_backend_open(&bf, store, collection, item, trace);
					bytes_read = jd_backend_read(&bf, buf, length, offset, trace);
					j_trace_counter(trace, "julead_read", bytes_read);
					reply = j_message_reply_new(message, bytes_read);
					j_message_reply_write(reply, output);
					g_output_stream_write_all(output, buf, bytes_read, NULL, NULL, NULL);
					j_trace_counter(trace, "julead_sent", bytes_read);
					jd_backend_close(&bf, trace);

					j_message_reply_free(reply);
					g_free(buf);
				}
				break;
			case J_MESSAGE_OPERATION_WRITE:
				g_printerr("write_op\n");
				{
					gchar* buf;
					guint64 length;
					guint64 offset;
					guint64 bytes_written;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);
					length = j_message_get_8(message);
					offset = j_message_get_8(message);

					buf = g_new(gchar, 512 * 1024);

					g_printerr("WRITE %s %s %s %ld %ld\n", store, collection, item, length, offset);

					jd_backend_open(&bf, store, collection, item, trace);
					g_input_stream_read_all(input, buf, length, NULL, NULL, NULL);
					j_trace_counter(trace, "julead_received", length);
					bytes_written = jd_backend_write(&bf, buf, length, offset, trace);
					j_trace_counter(trace, "julead_written", bytes_written);
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
	JConfiguration* configuration;
	JTrace* trace;
	GSocketListener* listener;
	GError* error = NULL;
	GOptionContext* context;
	gchar* path;
	struct sigaction sig;

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

	sig.sa_handler = jd_signal;
	sigemptyset(&sig.sa_mask);
	sig.sa_flags = 0;

	sigaction(SIGINT, &sig, NULL);
	sigaction(SIGHUP, &sig, NULL);
	sigaction(SIGTERM, &sig, NULL);
	sigaction(SIGQUIT, &sig, NULL);

	sig.sa_handler = SIG_IGN;

	sigaction(SIGPIPE, &sig, NULL);

	j_trace_init("julead");
	trace = j_trace_thread_enter(NULL, G_STRFUNC);

	configuration = j_configuration_new();

	if (configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	path = g_module_build_path(JULEAD_BACKEND_PATH, configuration->storage.backend);
	backend = g_module_open(path, G_MODULE_BIND_LOCAL);
	g_free(path);

	g_module_symbol(backend, "backend_init", (gpointer*)&jd_backend_init);
	g_module_symbol(backend, "backend_fini", (gpointer*)&jd_backend_fini);
	g_module_symbol(backend, "backend_open", (gpointer*)&jd_backend_open);
	g_module_symbol(backend, "backend_close", (gpointer*)&jd_backend_close);
	g_module_symbol(backend, "backend_read", (gpointer*)&jd_backend_read);
	g_module_symbol(backend, "backend_write", (gpointer*)&jd_backend_write);

	g_assert(jd_backend_init != NULL);
	g_assert(jd_backend_fini != NULL);
	g_assert(jd_backend_open != NULL);
	g_assert(jd_backend_close != NULL);
	g_assert(jd_backend_read != NULL);
	g_assert(jd_backend_write != NULL);

	jd_backend_init(configuration->storage.path, trace);

	j_configuration_free(configuration);

	listener = G_SOCKET_LISTENER(g_threaded_socket_service_new(-1));
	g_socket_listener_add_inet_port(listener, opt_port, NULL, NULL);
	g_socket_service_start(G_SOCKET_SERVICE(listener));
	g_signal_connect(G_THREADED_SOCKET_SERVICE(listener), "run", G_CALLBACK(jd_on_run), NULL);

	main_loop = g_main_loop_new(NULL, FALSE);
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
