/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <julea-config.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <glib-unix.h>
#include <glib-object.h>
#include <gio/gio.h>
#include <gmodule.h>

#include <string.h>

#include <julea-internal.h>

#include <jbackend.h>
#include <jbackend-internal.h>
#include <jconfiguration-internal.h>
#include <jhelper-internal.h>
#include <jmemory-chunk-internal.h>
#include <jmessage-internal.h>
#include <jstatistics-internal.h>
#include <jtrace-internal.h>

static JStatistics* jd_statistics;

G_LOCK_DEFINE_STATIC(jd_statistics);

static JBackend* jd_data_backend;
static JBackend* jd_meta_backend;

static guint jd_thread_num = 0;

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
	JMemoryChunk* memory_chunk;
	JMessage* message;
	JStatistics* statistics;
	GInputStream* input;
	gpointer backend_data = NULL;

	(void)service;
	(void)source_object;
	(void)user_data;

	j_trace_enter(G_STRFUNC);

	j_helper_set_nodelay(connection, TRUE);

	statistics = j_statistics_new(TRUE);
	memory_chunk = j_memory_chunk_new(J_STRIPE_SIZE);

	/* FIXME jd_data_backend might be NULL */
	if (jd_data_backend->u.data.thread_init != NULL)
	{
		backend_data = jd_data_backend->u.data.thread_init();
	}

	message = j_message_new(J_MESSAGE_NONE, 0);
	input = g_io_stream_get_input_stream(G_IO_STREAM(connection));

	while (j_message_receive(message, connection))
	{
		JBackendItem backend_item[1];
		gchar const* path;
		guint32 operation_count;
		JMessageType type_modifier;
		guint i;

		operation_count = j_message_get_count(message);
		type_modifier = j_message_get_type_modifier(message);

		switch (j_message_get_type(message))
		{
			case J_MESSAGE_NONE:
				break;
			case J_MESSAGE_DATA_CREATE:
				{
					JMessage* reply = NULL;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					for (i = 0; i < operation_count; i++)
					{
						path = j_message_get_string(message);

						if (jd_data_backend->u.data.create(backend_item, path, backend_data))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_CREATED, 1);

							if (type_modifier & J_MESSAGE_SAFETY_STORAGE)
							{
								jd_data_backend->u.data.sync(backend_item, backend_data);
								j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
							}

							jd_data_backend->u.data.close(backend_item, backend_data);
						}

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					if (reply != NULL)
					{
						j_message_send(reply, connection);
						j_message_unref(reply);
					}
				}
				break;
			case J_MESSAGE_DATA_DELETE:
				{
					JMessage* reply = NULL;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					for (i = 0; i < operation_count; i++)
					{
						path = j_message_get_string(message);

						if (jd_data_backend->u.data.open(backend_item, path, backend_data)
						    && jd_data_backend->u.data.delete(backend_item, backend_data))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_DELETED, 1);
							jd_data_backend->u.data.close(backend_item, backend_data);
						}

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					if (reply != NULL)
					{
						j_message_send(reply, connection);
						j_message_unref(reply);
					}
				}
				break;
			case J_MESSAGE_DATA_READ:
				{
					JMessage* reply;

					path = j_message_get_string(message);

					reply = j_message_new_reply(message);

					// FIXME return value
					jd_data_backend->u.data.open(backend_item, path, backend_data);

					for (i = 0; i < operation_count; i++)
					{
						gchar* buf;
						guint64 length;
						guint64 offset;
						guint64 bytes_read = 0;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						buf = j_memory_chunk_get(memory_chunk, length);

						if (buf == NULL)
						{
							// FIXME ugly
							j_message_send(reply, connection);
							j_message_unref(reply);

							reply = j_message_new_reply(message);

							j_memory_chunk_reset(memory_chunk);
							buf = j_memory_chunk_get(memory_chunk, length);
						}

						jd_data_backend->u.data.read(backend_item, buf, length, offset, &bytes_read, backend_data);
						j_statistics_add(statistics, J_STATISTICS_BYTES_READ, bytes_read);

						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_read);

						if (bytes_read > 0)
						{
							j_message_add_send(reply, buf, bytes_read);
						}

						j_statistics_add(statistics, J_STATISTICS_BYTES_SENT, bytes_read);
					}

					jd_data_backend->u.data.close(backend_item, backend_data);

					j_message_send(reply, connection);
					j_message_unref(reply);

					j_memory_chunk_reset(memory_chunk);
				}
				break;
			case J_MESSAGE_DATA_WRITE:
				{
					JMessage* reply = NULL;
					gchar* buf;
					guint64 merge_length = 0;
					guint64 merge_offset = 0;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					path = j_message_get_string(message);

					/* Guaranteed to work, because memory_chunk is not shared. */
					buf = j_memory_chunk_get(memory_chunk, J_STRIPE_SIZE);
					g_assert(buf != NULL);

					// FIXME return value
					jd_data_backend->u.data.open(backend_item, path, backend_data);

					for (i = 0; i < operation_count; i++)
					{
						guint64 length;
						guint64 offset;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						/* Check whether we can merge two consecutive operations. */
						if (merge_length > 0 && merge_offset + merge_length == offset && merge_length + length <= J_STRIPE_SIZE)
						{
							merge_length += length;
						}
						else if (merge_length > 0)
						{
							guint64 bytes_written = 0;

							g_input_stream_read_all(input, buf, merge_length, NULL, NULL, NULL);
							j_statistics_add(statistics, J_STATISTICS_BYTES_RECEIVED, merge_length);

							jd_data_backend->u.data.write(backend_item, buf, merge_length, merge_offset, &bytes_written, backend_data);
							j_statistics_add(statistics, J_STATISTICS_BYTES_WRITTEN, bytes_written);

							merge_length = 0;
							merge_offset = 0;
						}

						if (merge_length == 0)
						{
							merge_length = length;
							merge_offset = offset;
						}

						if (reply != NULL)
						{
							// FIXME the reply is faked (length should be bytes_written)
							j_message_add_operation(reply, sizeof(guint64));
							j_message_append_8(reply, &length);
						}
					}

					if (merge_length > 0)
					{
						guint64 bytes_written = 0;

						g_input_stream_read_all(input, buf, merge_length, NULL, NULL, NULL);
						j_statistics_add(statistics, J_STATISTICS_BYTES_RECEIVED, merge_length);

						jd_data_backend->u.data.write(backend_item, buf, merge_length, merge_offset, &bytes_written, backend_data);
						j_statistics_add(statistics, J_STATISTICS_BYTES_WRITTEN, bytes_written);
					}

					if (type_modifier & J_MESSAGE_SAFETY_STORAGE)
					{
						jd_data_backend->u.data.sync(backend_item, backend_data);
						j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
					}

					jd_data_backend->u.data.close(backend_item, backend_data);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
						j_message_unref(reply);
					}

					j_memory_chunk_reset(memory_chunk);
				}
				break;
			case J_MESSAGE_DATA_STATUS:
				{
					JMessage* reply;

					reply = j_message_new_reply(message);

					for (i = 0; i < operation_count; i++)
					{
						guint count = 0;
						guint32 flags;
						gint64 modification_time = 0;
						guint64 size = 0;

						path = j_message_get_string(message);
						flags = j_message_get_4(message);

						// FIXME return value
						jd_data_backend->u.data.open(backend_item, path, backend_data);

						if (jd_data_backend->u.data.status(backend_item, flags, &modification_time, &size, backend_data))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_STATED, 1);
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

						jd_data_backend->u.data.close(backend_item, backend_data);
					}

					j_message_send(reply, connection);
					j_message_unref(reply);
				}
				break;
			case J_MESSAGE_STATISTICS:
				{
					JMessage* reply;
					JStatistics* r_statistics;
					gchar get_all;
					guint64 value;

					get_all = j_message_get_1(message);
					r_statistics = (get_all == 0) ? statistics : jd_statistics;

					if (get_all != 0)
					{
						G_LOCK(jd_statistics);
						/* FIXME add statistics of all threads */
					}

					reply = j_message_new_reply(message);
					j_message_add_operation(reply, 8 * sizeof(guint64));

					value = j_statistics_get(r_statistics, J_STATISTICS_FILES_CREATED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_FILES_DELETED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_FILES_STATED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_SYNC);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_READ);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_WRITTEN);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_RECEIVED);
					j_message_append_8(reply, &value);
					value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_SENT);
					j_message_append_8(reply, &value);

					if (get_all != 0)
					{
						G_UNLOCK(jd_statistics);
					}

					j_message_send(reply, connection);
					j_message_unref(reply);
				}
				break;
			case J_MESSAGE_PING:
				{
					JMessage* reply;
					guint num;

					num = g_atomic_int_add(&jd_thread_num, 1);

					(void)num;
					//g_print("HELLO %d\n", num);

					reply = j_message_new_reply(message);

					if (jd_data_backend != NULL)
					{
						j_message_add_operation(reply, 5);
						j_message_append_n(reply, "data", 5);
					}

					if (jd_meta_backend != NULL)
					{
						j_message_add_operation(reply, 5);
						j_message_append_n(reply, "meta", 5);
					}

					j_message_send(reply, connection);
					j_message_unref(reply);
				}
				break;
			case J_MESSAGE_META_CREATE:
			case J_MESSAGE_META_DELETE:
			case J_MESSAGE_META_GET:
			case J_MESSAGE_META_GET_ALL:
			case J_MESSAGE_META_GET_BY_VALUE:
			case J_MESSAGE_META_ITERATE:
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
		guint64 value;

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

	if (jd_data_backend->u.data.thread_fini != NULL)
	{
		jd_data_backend->u.data.thread_fini(backend_data);
	}

	j_memory_chunk_free(memory_chunk);
	j_statistics_free(statistics);

	j_trace_leave(G_STRFUNC);

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
		g_printerr("Daemon started as process %d.\n", pid);
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
	GError* error = NULL;
	GMainLoop* main_loop;
	GModule* data_module = NULL;
	GModule* meta_module = NULL;
	GOptionContext* context;
	GSocketService* socket_service;

	GOptionEntry entries[] = {
		{ "daemon", 0, 0, G_OPTION_ARG_NONE, &opt_daemon, "Run as daemon", NULL },
		{ "port", 0, 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "4711" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

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

	socket_service = g_threaded_socket_service_new(-1);

	g_socket_listener_set_backlog(G_SOCKET_LISTENER(socket_service), 128);

	if (!g_socket_listener_add_inet_port(G_SOCKET_LISTENER(socket_service), opt_port, NULL, &error))
	{
		g_object_unref(socket_service);

		if (error != NULL)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	j_trace_init("julea-server");

	j_trace_enter(G_STRFUNC);

	configuration = j_configuration_new();

	if (configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	data_module = j_backend_load_server(j_configuration_get_data_backend(configuration), J_BACKEND_TYPE_DATA, &jd_data_backend);
	meta_module = j_backend_load_server(j_configuration_get_metadata_backend(configuration), J_BACKEND_TYPE_META, &jd_meta_backend);

	if (jd_data_backend != NULL && !jd_data_backend->u.data.init(j_configuration_get_data_path(configuration)))
	{
		g_printerr("Could not initialize data backend.\n");
		return 1;
	}

	if (jd_meta_backend != NULL && !jd_meta_backend->u.meta.init(j_configuration_get_metadata_path(configuration)))
	{
		g_printerr("Could not initialize metadata backend.\n");
		return 1;
	}

	j_configuration_unref(configuration);

	jd_statistics = j_statistics_new(FALSE);

	g_socket_service_start(socket_service);
	g_signal_connect(socket_service, "run", G_CALLBACK(jd_on_run), NULL);

	main_loop = g_main_loop_new(NULL, FALSE);

	g_unix_signal_add(SIGHUP, jd_signal, main_loop);
	g_unix_signal_add(SIGINT, jd_signal, main_loop);
	g_unix_signal_add(SIGTERM, jd_signal, main_loop);

	g_main_loop_run(main_loop);
	g_main_loop_unref(main_loop);

	g_socket_service_stop(socket_service);
	g_object_unref(socket_service);

	j_statistics_free(jd_statistics);

	if (jd_meta_backend != NULL)
	{
		jd_meta_backend->u.meta.fini();
	}

	if (jd_data_backend != NULL)
	{
		jd_data_backend->u.data.fini();
	}

	if (meta_module != NULL)
	{
		g_module_close(meta_module);
	}

	if (data_module)
	{
		g_module_close(data_module);
	}

	j_trace_leave(G_STRFUNC);

	j_trace_fini();

	return 0;
}
