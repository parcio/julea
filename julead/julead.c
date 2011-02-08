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

#include <string.h>

#include "jmessage.h"

static GFile* j_storage = NULL;

static gint opt_port = 4711;
static gchar const* opt_storage = "/tmp/julea";

static gboolean
julead_on_run (GThreadedSocketService* service, GSocketConnection* connection, GObject* source_object, gpointer user_data)
{
	JMessage* message;
	GInputStream* input;
	GOutputStream* output;

	g_printerr("new %p\n", (gpointer)connection);

	message = j_message_new(1024 * 1024, J_MESSAGE_OP_NONE);
	input = g_io_stream_get_input_stream(G_IO_STREAM(connection));
	output = g_io_stream_get_output_stream(G_IO_STREAM(connection));

	while (j_message_read(message, input))
	{
		switch (j_message_op(message))
		{
			case J_MESSAGE_OP_NONE:
				g_printerr("none_op\n");
				break;
			case J_MESSAGE_OP_READ:
				g_printerr("read_op\n");
				break;
			case J_MESSAGE_OP_WRITE:
				g_printerr("write_op\n");
				{
					gchar* buf;
					gchar const* store;
					gchar const* collection;
					gchar const* item;
					guint64 length;
					guint64 offset;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);
					length = j_message_get_8(message);
					offset = j_message_get_8(message);

					buf = g_new(gchar, 512 * 1024);

					g_printerr("xxx %s %s %s %ld %ld\n", store, collection, item, length, offset);

					g_input_stream_read_all(input, buf, length, NULL, NULL, NULL);

					g_free(buf);
				}
				break;
			default:
				g_warn_if_reached();
				break;
		}
	}

	j_message_free(message);

	g_printerr("close %p\n", (gpointer)connection);

	return TRUE;
}

int
main (int argc, char** argv)
{
	GMainLoop* main_loop;
	GSocketListener* listener;
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "port", 'p', 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "4711" },
		{ "storage", 's', 0, G_OPTION_ARG_STRING, &opt_storage, "Storage space to use", "/tmp/julea" },
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

	j_storage = g_file_new_for_commandline_arg(opt_storage);
	g_file_make_directory_with_parents(j_storage, NULL, NULL);

	listener = G_SOCKET_LISTENER(g_threaded_socket_service_new(-1));
	g_socket_listener_add_inet_port(listener, opt_port, NULL, NULL);
	g_socket_service_start(G_SOCKET_SERVICE(listener));
	g_signal_connect(G_THREADED_SOCKET_SERVICE(listener), "run", G_CALLBACK(julead_on_run), NULL);

	main_loop = g_main_loop_new(NULL, FALSE);
	g_main_loop_run(main_loop);
	g_main_loop_unref(main_loop);

	g_socket_service_stop(G_SOCKET_SERVICE(listener));
	g_object_unref(listener);

	g_object_unref(j_storage);

	return 0;
}
