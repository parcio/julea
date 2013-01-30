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
#include <glib/gstdio.h>
#include <glib-unix.h>
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <string.h>

#include <jcache-internal.h>
#include <jconfiguration.h>
#include <jconnection-internal.h>
#include <jmessage.h>
#include <jstatistics-internal.h>
#include <jthread-internal.h>
#include <jtrace-internal.h>

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
	GInputStream* input;
	GOutputStream* output;

	(void)service;
	(void)source_object;
	(void)user_data;

	j_connection_use_nodelay(connection);

	thread = j_thread_new(g_thread_self(), G_STRFUNC);
	cache = j_cache_new(J_MIB(50));

	if (jd_backend_thread_init != NULL)
	{
		jd_backend_thread_init();
	}

	message = j_message_new(J_MESSAGE_NONE, 0);
	input = g_io_stream_get_input_stream(G_IO_STREAM(connection));
	output = g_io_stream_get_output_stream(G_IO_STREAM(connection));

	while (j_message_read(message, input))
	{
		JBackendFile bf;
		gchar const* store;
		gchar const* collection;
		gchar const* item;
		guint32 operation_count;
		JMessageType type_modifier;
		guint i;

		operation_count = j_message_get_count(message);
		type_modifier = j_message_get_type_modifier(message);

		switch (j_message_get_type(message))
		{
			case J_MESSAGE_NONE:
				break;
			case J_MESSAGE_CREATE:
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						item = j_message_get_string(message);

						if (jd_backend_create(&bf, store, collection, item))
						{
							j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_FILES_CREATED, 1);
							jd_backend_close(&bf);
						}
					}
				}
				break;
			case J_MESSAGE_DELETE:
				{
					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						JMessage* reply;

						item = j_message_get_string(message);

						jd_backend_open(&bf, store, collection, item);

						if (jd_backend_delete(&bf))
						{
							j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_FILES_DELETED, 1);
							jd_backend_close(&bf);
						}

						if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
						{
							reply = j_message_new_reply(message);
							j_message_write(reply, output);
							j_message_unref(reply);
						}
					}
				}
				break;
			case J_MESSAGE_READ:
				{
					JMessage* reply;

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					reply = j_message_new_reply(message);

					jd_backend_open(&bf, store, collection, item);

					for (i = 0; i < operation_count; i++)
					{
						gchar* buf;
						guint64 length;
						guint64 offset;
						guint64 bytes_read;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						buf = j_cache_get(cache, length);

						if (buf == NULL)
						{
							// FIXME send smaller reply
						}

						jd_backend_read(&bf, buf, length, offset, &bytes_read);
						j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_BYTES_READ, bytes_read);

						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_read);
						j_message_add_send(reply, buf, bytes_read);
						j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_BYTES_SENT, bytes_read);
					}

					jd_backend_close(&bf);

					j_message_write(reply, output);
					j_message_unref(reply);

					j_cache_clear(cache);
				}
				break;
			case J_MESSAGE_WRITE:
				{
					JMessage* reply = NULL;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					store = j_message_get_string(message);
					collection = j_message_get_string(message);
					item = j_message_get_string(message);

					jd_backend_open(&bf, store, collection, item);

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
						j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_BYTES_RECEIVED, length);

						jd_backend_write(&bf, buf, length, offset, &bytes_written);
						j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_BYTES_WRITTEN, bytes_written);

						if (reply != NULL)
						{
							j_message_add_operation(reply, sizeof(guint64));
							j_message_append_8(reply, &bytes_written);
						}

						j_cache_clear(cache);
					}

					if (type_modifier & J_MESSAGE_SAFETY_STORAGE)
					{
						jd_backend_sync(&bf);
						j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_SYNC, 1);
					}

					jd_backend_close(&bf);

					if (reply != NULL)
					{
						j_message_write(reply, output);
						j_message_unref(reply);
					}
				}
				break;
			case J_MESSAGE_STATUS:
				{
					JMessage* reply;

					reply = j_message_new_reply(message);

					store = j_message_get_string(message);
					collection = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						guint count = 0;
						guint32 flags;
						gint64 modification_time = 0;
						guint64 size = 0;

						item = j_message_get_string(message);
						flags = j_message_get_4(message);

						jd_backend_open(&bf, store, collection, item);

						if (jd_backend_status(&bf, flags, &modification_time, &size))
						{
							// FIXME
							j_statistics_add(j_thread_get_statistics(thread), J_STATISTICS_FILES_CREATED, 1);
						}

						if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
						{
							count++;
						}

						if (flags & J_ITEM_STATUS_SIZE)
						{
							count++;
						}

						j_message_add_operation(reply, count * sizeof(guint64));

						if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
						{
							j_message_append_8(reply, &modification_time);
						}

						if (flags & J_ITEM_STATUS_SIZE)
						{
							j_message_append_8(reply, &size);
						}

						jd_backend_close(&bf);
					}

					j_message_write(reply, output);
					j_message_unref(reply);
				}
				break;
			case J_MESSAGE_STATISTICS:
				{
					JMessage* reply;
					JStatistics* statistics;
					gchar get_all;
					guint64 value;

					get_all = j_message_get_1(message);
					statistics = (get_all == 0) ? j_thread_get_statistics(thread) : jd_statistics;

					if (get_all != 0)
					{
						G_LOCK(jd_statistics);
						/* FIXME add statistics of all threads */
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

					if (get_all != 0)
					{
						G_UNLOCK(jd_statistics);
					}

					j_message_write(reply, output);
					j_message_unref(reply);
				}
				break;
			case J_MESSAGE_REPLY:
			case J_MESSAGE_SAFETY_NETWORK:
			case J_MESSAGE_SAFETY_STORAGE:
			case J_MESSAGE_MODIFIER_MASK:
			default:
				g_warn_if_reached();
				break;
		}
	}

	j_message_unref(message);

	{
		JStatistics* statistics;
		guint64 value;

		statistics = j_thread_get_statistics(thread);

		G_LOCK(jd_statistics);

		value = j_statistics_get(statistics, J_STATISTICS_FILES_CREATED);
		j_statistics_add(jd_statistics, J_STATISTICS_FILES_CREATED, value);
		value = j_statistics_get(statistics, J_STATISTICS_FILES_DELETED);
		j_statistics_add(jd_statistics, J_STATISTICS_FILES_DELETED, value);
		value = j_statistics_get(statistics, J_STATISTICS_SYNC);
		j_statistics_add(jd_statistics, J_STATISTICS_SYNC, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_READ);
		j_statistics_add(jd_statistics, J_STATISTICS_BYTES_READ, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_WRITTEN);
		j_statistics_add(jd_statistics, J_STATISTICS_BYTES_WRITTEN, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_RECEIVED);
		j_statistics_add(jd_statistics, J_STATISTICS_BYTES_RECEIVED, value);
		value = j_statistics_get(statistics, J_STATISTICS_BYTES_SENT);
		j_statistics_add(jd_statistics, J_STATISTICS_BYTES_SENT, value);

		G_UNLOCK(jd_statistics);
	}

	j_cache_free(cache);
	j_thread_free(thread);

	if (jd_backend_thread_fini != NULL)
	{
		jd_backend_thread_fini();
	}

	return TRUE;
}

static
gboolean
jd_daemon (void)
{
	gint fd;
	pid_t pid;

	pid = fork();

	if (pid > 0)
	{
		_exit(0);
	}
	else if (pid == -1)
	{
		return FALSE;
	}

	if (setsid() == -1)
	{
		return FALSE;
	}

	if (g_chdir("/") == -1)
	{
		return FALSE;
	}

	fd = open("/dev/null", O_RDWR);

	if (fd == -1)
	{
		return FALSE;
	}

	if (dup2(fd, STDIN_FILENO) == -1 || dup2(fd, STDOUT_FILENO) == -1 || dup2(fd, STDERR_FILENO) == -1)
	{
		return FALSE;
	}

	if (fd > 2)
	{
		close(fd);
	}

	return TRUE;
}

int
main (int argc, char** argv)
{
	gboolean opt_daemon = FALSE;
	gint opt_port = 4711;

	JConfiguration* configuration;
	JThread* thread;
	GError* error = NULL;
	GMainLoop* main_loop;
	GModule* backend;
	GOptionContext* context;
	GSocketListener* listener;
	gchar* path;

	GOptionEntry entries[] = {
		{ "daemon", 0, 0, G_OPTION_ARG_NONE, &opt_daemon, "Run as daemon", NULL },
		{ "port", 0, 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "4711" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

#if !GLIB_CHECK_VERSION(2,35,1)
	g_type_init();
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

	if (opt_daemon && !jd_daemon())
	{
		return 1;
	}

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

	j_trace_init("julea-daemon");

	thread = j_thread_new(NULL, G_STRFUNC);

	configuration = j_configuration_new();

	if (configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	path = g_module_build_path(DAEMON_BACKEND_PATH, j_configuration_get_storage_backend(configuration));
	backend = g_module_open(path, G_MODULE_BIND_LOCAL);
	g_free(path);

	g_module_symbol(backend, "backend_init", (gpointer*)&jd_backend_init);
	g_module_symbol(backend, "backend_fini", (gpointer*)&jd_backend_fini);
	g_module_symbol(backend, "backend_thread_init", (gpointer*)&jd_backend_thread_init);
	g_module_symbol(backend, "backend_thread_fini", (gpointer*)&jd_backend_thread_fini);
	g_module_symbol(backend, "backend_create", (gpointer*)&jd_backend_create);
	g_module_symbol(backend, "backend_delete", (gpointer*)&jd_backend_delete);
	g_module_symbol(backend, "backend_open", (gpointer*)&jd_backend_open);
	g_module_symbol(backend, "backend_close", (gpointer*)&jd_backend_close);
	g_module_symbol(backend, "backend_status", (gpointer*)&jd_backend_status);
	g_module_symbol(backend, "backend_sync", (gpointer*)&jd_backend_sync);
	g_module_symbol(backend, "backend_read", (gpointer*)&jd_backend_read);
	g_module_symbol(backend, "backend_write", (gpointer*)&jd_backend_write);

	g_assert(jd_backend_init != NULL);
	g_assert(jd_backend_fini != NULL);
	g_assert(jd_backend_create != NULL);
	g_assert(jd_backend_delete != NULL);
	g_assert(jd_backend_open != NULL);
	g_assert(jd_backend_close != NULL);
	g_assert(jd_backend_status != NULL);
	g_assert(jd_backend_sync != NULL);
	g_assert(jd_backend_read != NULL);
	g_assert(jd_backend_write != NULL);

	jd_backend_init(j_configuration_get_storage_path(configuration));

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

	jd_backend_fini();

	g_module_close(backend);

	j_thread_free(thread);

	j_trace_fini();

	return 0;
}
