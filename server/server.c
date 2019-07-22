/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
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

#include "server-db-exec.h"

static JStatistics* jd_statistics;

G_LOCK_DEFINE_STATIC(jd_statistics);

static JConfiguration* jd_configuration;

static JBackend* jd_object_backend;
static JBackend* jd_kv_backend;
static JBackend* jd_db_backend;

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
	g_autoptr(JMessage) message = NULL;
	JStatistics* statistics;
	GInputStream* input;
	guint64 memory_chunk_size;
	JMessageType message_type;

	(void)service;
	(void)source_object;
	(void)user_data;

	j_trace_enter(G_STRFUNC, NULL);

	j_helper_set_nodelay(connection, TRUE);

	statistics = j_statistics_new(TRUE);
	memory_chunk_size = j_configuration_get_max_operation_size(jd_configuration);
	memory_chunk = j_memory_chunk_new(memory_chunk_size);

	message = j_message_new(J_MESSAGE_NONE, 0);
	input = g_io_stream_get_input_stream(G_IO_STREAM(connection));

	while (j_message_receive(message, connection))
	{
		gchar const* key;
		gchar const* namespace;
		gchar const* path;
		guint32 operation_count;
		JSemantics* semantics;
		JSemanticsSafety safety;
		guint i;

		operation_count = j_message_get_count(message);
		semantics = j_message_get_semantics(message);
		safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
		j_semantics_unref(semantics);

		message_type = j_message_get_type(message);
		switch (message_type)
		{
			case J_MESSAGE_NONE:
				break;
			case J_MESSAGE_OBJECT_CREATE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer object;

					if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						path = j_message_get_string(message);

						if (j_backend_object_create(jd_object_backend, namespace, path, &object))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_CREATED, 1);

							if (safety == J_SEMANTICS_SAFETY_STORAGE)
							{
								j_backend_object_sync(jd_object_backend, object);
								j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
							}

							j_backend_object_close(jd_object_backend, object);
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
			case J_MESSAGE_OBJECT_DELETE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer object;

					if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);

					for (i = 0; i < operation_count; i++)
					{
						path = j_message_get_string(message);

						if (j_backend_object_open(jd_object_backend, namespace, path, &object)
						    && j_backend_object_delete(jd_object_backend, object))
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
			case J_MESSAGE_OBJECT_READ:
				{
					JMessage* reply;
					gpointer object;

					namespace = j_message_get_string(message);
					path = j_message_get_string(message);

					reply = j_message_new_reply(message);

					// FIXME return value
					j_backend_object_open(jd_object_backend, namespace, path, &object);

					for (i = 0; i < operation_count; i++)
					{
						gchar* buf;
						guint64 length;
						guint64 offset;
						guint64 bytes_read = 0;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						if (length > memory_chunk_size)
						{
							// FIXME return proper error
							j_message_add_operation(reply, sizeof(guint64));
							j_message_append_8(reply, &bytes_read);
							continue;
						}

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

						j_backend_object_read(jd_object_backend, object, buf, length, offset, &bytes_read);
						j_statistics_add(statistics, J_STATISTICS_BYTES_READ, bytes_read);

						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_read);

						if (bytes_read > 0)
						{
							j_message_add_send(reply, buf, bytes_read);
						}

						j_statistics_add(statistics, J_STATISTICS_BYTES_SENT, bytes_read);
					}

					j_backend_object_close(jd_object_backend, object);

					j_message_send(reply, connection);
					j_message_unref(reply);

					j_memory_chunk_reset(memory_chunk);
				}
				break;
			case J_MESSAGE_OBJECT_WRITE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer object;

					if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);
					path = j_message_get_string(message);

					// FIXME return value
					j_backend_object_open(jd_object_backend, namespace, path, &object);

					for (i = 0; i < operation_count; i++)
					{
						gchar* buf;
						guint64 length;
						guint64 offset;
						guint64 bytes_written = 0;

						length = j_message_get_8(message);
						offset = j_message_get_8(message);

						if (length > memory_chunk_size)
						{
							// FIXME return proper error
							j_message_add_operation(reply, sizeof(guint64));
							j_message_append_8(reply, &bytes_written);
							continue;
						}

						// Guaranteed to work because memory_chunk is reset below
						buf = j_memory_chunk_get(memory_chunk, length);
						g_assert(buf != NULL);

						g_input_stream_read_all(input, buf, length, NULL, NULL, NULL);
						j_statistics_add(statistics, J_STATISTICS_BYTES_RECEIVED, length);

						j_backend_object_write(jd_object_backend, object, buf, length, offset, &bytes_written);
						j_statistics_add(statistics, J_STATISTICS_BYTES_WRITTEN, bytes_written);

						if (reply != NULL)
						{
							j_message_add_operation(reply, sizeof(guint64));
							j_message_append_8(reply, &bytes_written);
						}

						j_memory_chunk_reset(memory_chunk);
					}

					if (safety == J_SEMANTICS_SAFETY_STORAGE)
					{
						j_backend_object_sync(jd_object_backend, object);
						j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
					}

					j_backend_object_close(jd_object_backend, object);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}

					j_memory_chunk_reset(memory_chunk);
				}
				break;
			case J_MESSAGE_OBJECT_STATUS:
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
						j_backend_object_open(jd_object_backend, namespace, path, &object);

						if (j_backend_object_status(jd_object_backend, object, &modification_time, &size))
						{
							j_statistics_add(statistics, J_STATISTICS_FILES_STATED, 1);
						}

						j_message_add_operation(reply, sizeof(gint64) + sizeof(guint64));
						j_message_append_8(reply, &modification_time);
						j_message_append_8(reply, &size);

						j_backend_object_close(jd_object_backend, object);
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

					if (jd_object_backend != NULL)
					{
						j_message_add_operation(reply, 7);
						j_message_append_string(reply, "object");
					}

					if (jd_kv_backend != NULL)
					{
						j_message_add_operation(reply, 3);
						j_message_append_string(reply, "kv");
					}

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_KV_PUT:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer batch;

					if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);
					j_backend_kv_batch_start(jd_kv_backend, namespace, safety, &batch);

					for (i = 0; i < operation_count; i++)
					{
						gconstpointer data;
						guint32 len;

						key = j_message_get_string(message);
						len = j_message_get_4(message);
						data = j_message_get_n(message, len);

						j_backend_kv_put(jd_kv_backend, batch, key, data, len);

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					j_backend_kv_batch_execute(jd_kv_backend, batch);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}
				}
				break;
			case J_MESSAGE_KV_DELETE:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer batch;

					if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
					{
						reply = j_message_new_reply(message);
					}

					namespace = j_message_get_string(message);
					j_backend_kv_batch_start(jd_kv_backend, namespace, safety, &batch);

					for (i = 0; i < operation_count; i++)
					{
						key = j_message_get_string(message);

						j_backend_kv_delete(jd_kv_backend, batch, key);

						if (reply != NULL)
						{
							j_message_add_operation(reply, 0);
						}
					}

					j_backend_kv_batch_execute(jd_kv_backend, batch);

					if (reply != NULL)
					{
						j_message_send(reply, connection);
					}
				}
				break;
			case J_MESSAGE_KV_GET:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer batch;

					reply = j_message_new_reply(message);
					namespace = j_message_get_string(message);
					j_backend_kv_batch_start(jd_kv_backend, namespace, safety, &batch);

					for (i = 0; i < operation_count; i++)
					{
						gpointer value;
						guint32 len;

						key = j_message_get_string(message);

						if (j_backend_kv_get(jd_kv_backend, batch, key, &value, &len))
						{
							j_message_add_operation(reply, 4 + len);
							j_message_append_4(reply, &len);
							j_message_append_n(reply, value, len);

							g_free(value);
						}
						else
						{
							guint32 zero = 0;

							j_message_add_operation(reply, 4);
							j_message_append_4(reply, &zero);
						}
					}

					j_backend_kv_batch_execute(jd_kv_backend, batch);

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_KV_GET_ALL:
				{
					g_autoptr(JMessage) reply = NULL;
					gpointer iterator;
					gconstpointer value;
					guint32 len;
					guint32 zero = 0;

					reply = j_message_new_reply(message);
					namespace = j_message_get_string(message);

					j_backend_kv_get_all(jd_kv_backend, namespace, &iterator);

					while (j_backend_kv_iterate(jd_kv_backend, iterator, &key, &value, &len))
					{
						gsize key_len;

						key_len = strlen(key) + 1;

						j_message_add_operation(reply, 4 + len + key_len);
						j_message_append_4(reply, &len);
						j_message_append_n(reply, value, len);
						j_message_append_string(reply, key);
					}

					j_message_add_operation(reply, 4);
					j_message_append_4(reply, &zero);

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_KV_GET_BY_PREFIX:
				{
					g_autoptr(JMessage) reply = NULL;
					gchar const* prefix;
					gpointer iterator;
					gconstpointer value;
					guint32 len;
					guint32 zero = 0;

					reply = j_message_new_reply(message);
					namespace = j_message_get_string(message);
					prefix = j_message_get_string(message);

					j_backend_kv_get_by_prefix(jd_kv_backend, namespace, prefix, &iterator);

					while (j_backend_kv_iterate(jd_kv_backend, iterator, &key, &value, &len))
					{
						gsize key_len;

						key_len = strlen(key) + 1;

						j_message_add_operation(reply, 4 + len + key_len);
						j_message_append_4(reply, &len);
						j_message_append_n(reply, value, len);
						j_message_append_string(reply, key);
					}

					j_message_add_operation(reply, 4);
					j_message_append_4(reply, &zero);

					j_message_send(reply, connection);
				}
				break;
			case J_MESSAGE_DB_SCHEMA_CREATE:
			case J_MESSAGE_DB_SCHEMA_GET:
			case J_MESSAGE_DB_SCHEMA_DELETE:
			case J_MESSAGE_DB_INSERT:
			case J_MESSAGE_DB_UPDATE:
			case J_MESSAGE_DB_DELETE:
			case J_MESSAGE_DB_GET_ALL:
				db_server_message_exec(message_type, message, operation_count, jd_db_backend, safety, connection);
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

	GError* error = NULL;
	g_autoptr(GMainLoop) main_loop = NULL;
	GModule* object_module = NULL;
	GModule* kv_module = NULL;
	GModule* db_module = NULL;
	g_autoptr(GOptionContext) context = NULL;
	g_autoptr(GSocketService) socket_service = NULL;
	gchar const* object_backend;
	gchar const* object_component;
	gchar const* object_path;
	gchar const* kv_backend;
	gchar const* kv_component;
	gchar const* kv_path;
	gchar const* db_backend;
	gchar const* db_component;
	gchar const* db_path;
#ifdef JULEA_DEBUG
	g_autofree gchar* object_path_port = NULL;
	g_autofree gchar* kv_path_port = NULL;
	g_autofree gchar* db_path_port = NULL;
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

	jd_configuration = j_configuration_new();

	if (jd_configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	object_backend = j_configuration_get_object_backend(jd_configuration);
	object_component = j_configuration_get_object_component(jd_configuration);
	object_path = j_configuration_get_object_path(jd_configuration);

	kv_backend = j_configuration_get_kv_backend(jd_configuration);
	kv_component = j_configuration_get_kv_component(jd_configuration);
	kv_path = j_configuration_get_kv_path(jd_configuration);

	db_backend = j_configuration_get_db_backend(jd_configuration);
	db_component = j_configuration_get_db_component(jd_configuration);
	db_path = j_configuration_get_db_path(jd_configuration);

#ifdef JULEA_DEBUG
	object_path_port = g_strdup_printf("%s/%d", object_path, opt_port);
	kv_path_port = g_strdup_printf("%s/%d", kv_path, opt_port);
	db_path_port = g_strdup_printf("%s/%d", db_path, opt_port);

	object_path = object_path_port;
	kv_path = kv_path_port;
	db_path = db_path_port;
#endif

	if (j_backend_load_server(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &object_module, &jd_object_backend))
	{
		if (jd_object_backend == NULL || !j_backend_object_init(jd_object_backend, object_path))
		{
			J_CRITICAL("Could not initialize object backend %s.\n", object_backend);
			return 1;
		}
	}

	if (j_backend_load_server(kv_backend, kv_component, J_BACKEND_TYPE_KV, &kv_module, &jd_kv_backend))
	{
		if (jd_kv_backend == NULL || !j_backend_kv_init(jd_kv_backend, kv_path))
		{
			J_CRITICAL("Could not initialize kv backend %s.\n", kv_backend);
			return 1;
		}
	}

	if (j_backend_load_server(db_backend, db_component, J_BACKEND_TYPE_DB, &db_module, &jd_db_backend))
	{
		if (jd_db_backend == NULL || !j_backend_db_init(jd_db_backend, db_path))
		{
			J_CRITICAL("Could not initialize db backend %s.\n", db_backend);
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

	if (jd_db_backend != NULL)
	{
		j_backend_db_fini(jd_db_backend);
	}

	if (jd_kv_backend != NULL)
	{
		j_backend_kv_fini(jd_kv_backend);
	}

	if (jd_object_backend != NULL)
	{
		j_backend_object_fini(jd_object_backend);
	}

	if (db_module != NULL)
	{
		g_module_close(db_module);
	}

	if (kv_module != NULL)
	{
		g_module_close(kv_module);
	}

	if (object_module)
	{
		g_module_close(object_module);
	}

	j_configuration_unref(jd_configuration);

	j_trace_leave(G_STRFUNC);

	j_trace_fini();

	return 0;
}
