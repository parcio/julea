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

#include <julea.h>
#include <julea-internal.h>

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
JSemanticsSafety
jd_safety_message_to_semantics (JMessageFlags flags)
{
	JSemanticsSafety safety;

	safety = J_SEMANTICS_SAFETY_NONE;

	if (flags == 0)
	{
		goto end;
	}

	switch (flags)
	{
		case J_MESSAGE_SAFETY_STORAGE:
			safety = J_SEMANTICS_SAFETY_STORAGE;
			break;
		case J_MESSAGE_SAFETY_NETWORK:
			safety = J_SEMANTICS_SAFETY_NETWORK;
			break;
		case J_MESSAGE_REPLY:
		default:
			g_warn_if_reached();
			break;
	}

end:
	return safety;
}

static
gboolean
jd_on_run (GThreadedSocketService* service, GSocketConnection* connection, GObject* source_object, gpointer user_data)
{
	JMemoryChunk* memory_chunk;
	g_autoptr(JMessage) message = NULL;
	JStatistics* statistics;
	GInputStream* input;

	(void)service;
	(void)source_object;
	(void)user_data;

	j_trace_enter(G_STRFUNC, NULL);

	j_helper_set_nodelay(connection, TRUE);

	statistics = j_statistics_new(TRUE);
	memory_chunk = j_memory_chunk_new(J_STRIPE_SIZE);

	message = j_message_new(J_MESSAGE_NONE, 0);
	input = g_io_stream_get_input_stream(G_IO_STREAM(connection));

	while (j_message_receive(message, connection))
	{
		gchar const* key;
		gchar const* namespace;
		gchar const* path;
		guint32 operation_count;
		JMessageFlags type_modifier;
		JSemanticsSafety safety;
		guint i;

		operation_count = j_message_get_count(message);
		type_modifier = j_message_get_flags(message);
		safety = jd_safety_message_to_semantics(type_modifier);

		switch (j_message_get_type(message))
		{
			case J_MESSAGE_NONE:
				break;
			case J_MESSAGE_DATA_CREATE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer object;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						path = j_message_get_string(message);

						if (j_backend_data_create(jd_data_backend, namespace, path, &object))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_CREATED, 1);

							if (type_modifier & J_MESSAGE_SAFETY_STORAGE)
							{
								j_backend_data_sync(jd_data_backend, object);
								j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
							}

							j_backend_data_close(jd_data_backend, object);
						}

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}
				}
				break;
			case J_MESSAGE_DATA_DELETE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer object;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						path = j_message_get_string(message);

						if (j_backend_data_open(jd_data_backend, namespace, path, &object)
						    && j_backend_data_delete(jd_data_backend, object))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_DELETED, 1);
						}

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}
				}
				break;
			case J_MESSAGE_DATA_READ:
				{
					JMessage* reply;
					gpointer object;

					namespace = j_message_get_string(message);
					path = j_message_get_string(message);

					reply = j_message_new_reply(message);

					// FIXME return value
					j_backend_data_open(jd_data_backend, namespace, path, &object);

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

						j_backend_data_read(jd_data_backend, object, buf, length, offset, &bytes_read);
						j_statistics_add(statistics, J_STATISTICS_BYTES_READ, bytes_read);

						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_read);

						if (bytes_read > 0)
						{
							j_message_add_send(reply, buf, bytes_read);
						}

						j_statistics_add(statistics, J_STATISTICS_BYTES_SENT, bytes_read);
					}

					j_backend_data_close(jd_data_backend, object);

					j_message_send(reply, connection);
					j_message_unref(reply);

					j_memory_chunk_reset(memory_chunk);
				}
				break;
			case J_MESSAGE_DATA_WRITE:
				{
					g_autoptr(JMessage) reply = NULL;
					gchar* buf;
					gpointer object;
					guint64 merge_length = 0;
					guint64 merge_offset = 0;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);
					path = j_message_get_string(message);

					/* Guaranteed to work, because memory_chunk is not shared. */
					buf = j_memory_chunk_get(memory_chunk, J_STRIPE_SIZE);
					g_assert(buf != NULL);

					// FIXME return value
					j_backend_data_open(jd_data_backend, namespace, path, &object);

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

							j_backend_data_write(jd_data_backend, object, buf, merge_length, merge_offset, &bytes_written);
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

						j_backend_data_write(jd_data_backend, object, buf, merge_length, merge_offset, &bytes_written);
						j_statistics_add(statistics, J_STATISTICS_BYTES_WRITTEN, bytes_written);
					}

					if (type_modifier & J_MESSAGE_SAFETY_STORAGE)
					{
						j_backend_data_sync(jd_data_backend, object);
						j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
					}

					j_backend_data_close(jd_data_backend, object);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}

					j_memory_chunk_reset(memory_chunk);
				}
				break;
			case J_MESSAGE_DATA_STATUS:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer object;

					reply = j_message_new_reply(message);

					namespace = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						gint64 modification_time = 0;
						guint64 size = 0;

						path = j_message_get_string(message);

						// FIXME return value
						j_backend_data_open(jd_data_backend, namespace, path, &object);

						if (j_backend_data_status(jd_data_backend, object, &modification_time, &size))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_STATED, 1);
						}

						j_message_add_operation(reply, sizeof(gint64) + sizeof(guint64));
						j_message_append_8(reply, &modification_time);
						j_message_append_8(reply, &size);

						j_backend_data_close(jd_data_backend, object);
					}

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_STATISTICS:
				{
					g_autoptr(JMessage) reply = NULL;
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
				}
				break;
			case J_MESSAGE_PING:
				{
					g_autoptr(JMessage) reply = NULL;
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
				}
				break;
			case J_MESSAGE_META_PUT:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer batch;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);
					j_backend_meta_batch_start(jd_meta_backend, namespace, safety, &batch);

					for (i = 0; i < operation_count; i++)
					{
						bson_t value[1];
						gconstpointer data;
						guint32 len;

						key = j_message_get_string(message);
						len = j_message_get_4(message);
						data = j_message_get_n(message, len);
						bson_init_static(value, data, len);

						j_backend_meta_put(jd_meta_backend, batch, key, value);

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					j_backend_meta_batch_execute(jd_meta_backend, batch);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}
				}
				break;
			case J_MESSAGE_META_DELETE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer batch;

					if (type_modifier & J_MESSAGE_SAFETY_NETWORK)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);
					j_backend_meta_batch_start(jd_meta_backend, namespace, safety, &batch);

					for (i = 0; i < operation_count; i++)
					{
						key = j_message_get_string(message);

						j_backend_meta_delete(jd_meta_backend, batch, key);

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					j_backend_meta_batch_execute(jd_meta_backend, batch);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}
				}
				break;
			case J_MESSAGE_META_GET:
				{
					g_autoptr(JMessage) reply = NULL;

					reply = j_message_new_reply(message);
					namespace = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						bson_t value[1];

						key = j_message_get_string(message);

						if (j_backend_meta_get(jd_meta_backend, namespace, key, value))
						{
							j_message_add_operation(reply, 4 + value->len);
							j_message_append_4(reply, &(value->len));
							j_message_append_n(reply, bson_get_data(value), value->len);

							bson_destroy(value);
						}
						else
						{
							guint32 zero = 0;

							j_message_add_operation(reply, 4);
							j_message_append_4(reply, &zero);
						}
					}

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_META_GET_ALL:
				{
					g_autoptr(JMessage) reply = NULL;
					bson_t value[1];
					gpointer iterator;
					guint32 zero = 0;

					reply = j_message_new_reply(message);
					namespace = j_message_get_string(message);

					j_backend_meta_get_all(jd_meta_backend, namespace, &iterator);

					while (j_backend_meta_iterate(jd_meta_backend, iterator, value))
					{
						j_message_add_operation(reply, 4 + value->len);
						j_message_append_4(reply, &(value->len));
						j_message_append_n(reply, bson_get_data(value), value->len);
						bson_destroy(value);
					}

					j_message_add_operation(reply, 4);
					j_message_append_4(reply, &zero);

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_META_GET_BY_PREFIX:
				{
					g_autoptr(JMessage) reply = NULL;
					bson_t value[1];
					gchar const* prefix;
					gpointer iterator;
					guint32 zero = 0;

					reply = j_message_new_reply(message);
					namespace = j_message_get_string(message);
					prefix = j_message_get_string(message);

					j_backend_meta_get_by_prefix(jd_meta_backend, namespace, prefix, &iterator);

					while (j_backend_meta_iterate(jd_meta_backend, iterator, value))
					{
						j_message_add_operation(reply, 4 + value->len);
						j_message_append_4(reply, &(value->len));
						j_message_append_n(reply, bson_get_data(value), value->len);
						bson_destroy(value);
					}

					j_message_add_operation(reply, 4);
					j_message_append_4(reply, &zero);

					j_message_send(reply, connection);
				}
				break;
			default:
				g_warn_if_reached();
				break;
		}
	}

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

	g_autoptr(JConfiguration) configuration = NULL;
	GError* error = NULL;
	g_autoptr(GMainLoop) main_loop = NULL;
	GModule* data_module = NULL;
	GModule* meta_module = NULL;
	g_autoptr(GOptionContext) context = NULL;
	g_autoptr(GSocketService) socket_service = NULL;
	gchar const* data_backend;
	gchar const* data_component;
	gchar const* data_path;
	gchar const* meta_backend;
	gchar const* meta_component;
	gchar const* meta_path;
#ifdef JULEA_DEBUG
	g_autofree gchar* data_path_port = NULL;
	g_autofree gchar* meta_path_port = NULL;
#endif

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

	if (opt_daemon && !jd_daemon())
	{
		return 1;
	}

	socket_service = g_threaded_socket_service_new(-1);

	g_socket_listener_set_backlog(G_SOCKET_LISTENER(socket_service), 128);

	if (!g_socket_listener_add_inet_port(G_SOCKET_LISTENER(socket_service), opt_port, NULL, &error))
	{
		if (error != NULL)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	j_trace_init("julea-server");

	j_trace_enter(G_STRFUNC, NULL);

	configuration = j_configuration_new();

	if (configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	data_backend = j_configuration_get_data_backend(configuration);
	data_component = j_configuration_get_data_component(configuration);
	data_path = j_configuration_get_data_path(configuration);

	meta_backend = j_configuration_get_metadata_backend(configuration);
	meta_component = j_configuration_get_metadata_component(configuration);
	meta_path = j_configuration_get_metadata_path(configuration);

#ifdef JULEA_DEBUG
	data_path_port = g_strdup_printf("%s/%d", data_path, opt_port);
	meta_path_port = g_strdup_printf("%s/%d", meta_path, opt_port);

	g_mkdir_with_parents(data_path_port, 0700);
	g_mkdir_with_parents(meta_path_port, 0700);

	data_path = data_path_port;
	meta_path = meta_path_port;
#endif

	if (j_backend_load_server(data_backend, data_component, J_BACKEND_TYPE_DATA, &data_module, &jd_data_backend))
	{
		if (jd_data_backend == NULL || !j_backend_data_init(jd_data_backend, data_path))
		{
			J_CRITICAL("Could not initialize data backend %s.\n", data_backend);
			return 1;
		}
	}

	if (j_backend_load_server(meta_backend, meta_component, J_BACKEND_TYPE_META, &meta_module, &jd_meta_backend))
	{
		if (jd_meta_backend == NULL || !j_backend_meta_init(jd_meta_backend, meta_path))
		{
			J_CRITICAL("Could not initialize metadata backend %s.\n", meta_backend);
			return 1;
		}
	}

	jd_statistics = j_statistics_new(FALSE);

	g_socket_service_start(socket_service);
	g_signal_connect(socket_service, "run", G_CALLBACK(jd_on_run), NULL);

	main_loop = g_main_loop_new(NULL, FALSE);

	g_unix_signal_add(SIGHUP, jd_signal, main_loop);
	g_unix_signal_add(SIGINT, jd_signal, main_loop);
	g_unix_signal_add(SIGTERM, jd_signal, main_loop);

	g_main_loop_run(main_loop);

	g_socket_service_stop(socket_service);

	j_statistics_free(jd_statistics);

	if (jd_meta_backend != NULL)
	{
		j_backend_meta_fini(jd_meta_backend);
	}

	if (jd_data_backend != NULL)
	{
		j_backend_data_fini(jd_data_backend);
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
