/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2024 Michael Kuhn
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

#include <locale.h>
#include <string.h>
#include <unistd.h>

#include <julea.h>

#include "server.h"

JStatistics* jd_statistics = NULL;
GMutex jd_statistics_mutex[1] = { 0 };

JBackend* jd_object_backend = NULL;
JBackend* jd_kv_backend = NULL;
JBackend* jd_db_backend = NULL;

JConfiguration* jd_configuration = NULL;

static gboolean
jd_signal(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	GMainLoop* main_loop = data;

	if (g_main_loop_is_running(main_loop))
	{
		g_main_loop_quit(main_loop);
	}

	return FALSE;
}

static gboolean
jd_on_run(GThreadedSocketService* service, GSocketConnection* connection, GObject* source_object, gpointer user_data)
{
	J_TRACE_FUNCTION(NULL);

	JMemoryChunk* memory_chunk;
	g_autoptr(JMessage) message = NULL;
	JStatistics* statistics;
	guint64 memory_chunk_size;

	(void)service;
	(void)source_object;
	(void)user_data;

	j_helper_set_nodelay(connection, TRUE);

	statistics = j_statistics_new(TRUE);
	memory_chunk_size = j_configuration_get_max_operation_size(jd_configuration);
	memory_chunk = j_memory_chunk_new(memory_chunk_size);

	message = j_message_new(J_MESSAGE_NONE, 0);

	while (j_message_receive(message, connection))
	{
		jd_handle_message(message, connection, memory_chunk, memory_chunk_size, statistics);
	}

	{
		guint64 value;

		g_mutex_lock(jd_statistics_mutex);

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

		g_mutex_unlock(jd_statistics_mutex);
	}

	j_memory_chunk_free(memory_chunk);
	j_statistics_free(statistics);

	return TRUE;
}

static gboolean
jd_daemon(void)
{
	J_TRACE_FUNCTION(NULL);

	gint fd;
	pid_t pid;

	pid = fork();

	if (pid > 0)
	{
		g_message("Daemon started as process %d.", pid);
		g_message("Log messages will be redirected to journald if possible. To view them, use journalctl GLIB_DOMAIN=%s.", G_LOG_DOMAIN);
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

	// FIXME Use sd_journal_stream_fd if available
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

	// Try to redirect GLib log messages to journald.
	// They can be shown with: journalctl GLIB_DOMAIN=JULEA
	g_log_set_writer_func(g_log_writer_journald, NULL, NULL);

	return TRUE;
}

static gboolean
jd_is_server_for_backend(gchar const* host, gint port, JBackendType backend_type)
{
	guint count;

	count = j_configuration_get_server_count(jd_configuration, backend_type);

	for (guint i = 0; i < count; i++)
	{
		g_autoptr(GNetworkAddress) address = NULL;
		gchar const* addr_server;
		gchar const* server;
		guint16 addr_port;

		server = j_configuration_get_server(jd_configuration, backend_type, i);
		address = G_NETWORK_ADDRESS(g_network_address_parse(server, j_configuration_get_port(jd_configuration), NULL));

		addr_server = g_network_address_get_hostname(address);
		addr_port = g_network_address_get_port(address);

		if (g_strcmp0(host, addr_server) == 0 && port == addr_port)
		{
			return TRUE;
		}
	}

	return FALSE;
}

static gboolean
jd_load_and_init_backend(gchar const* host, gint port, JBackendType type, JBackend** backend, GModule** module)
{
	gchar const* backend_name;
	g_autofree gchar* backend_path = NULL;
	g_autofree gchar* port_str = NULL;
	gchar const* type_str = NULL;

	gboolean (*backend_init)(JBackend*, gchar const*) = NULL;

	switch (type)
	{
		case J_BACKEND_TYPE_OBJECT:
			type_str = "object";
			backend_init = j_backend_object_init;
			break;
		case J_BACKEND_TYPE_KV:
			type_str = "kv";
			backend_init = j_backend_kv_init;
			break;
		case J_BACKEND_TYPE_DB:
			type_str = "db";
			backend_init = j_backend_db_init;
			break;
		default:
			g_warn_if_reached();
	}

	port_str = g_strdup_printf("%d", port);

	backend_name = j_configuration_get_backend(jd_configuration, type);
	backend_path = j_helper_str_replace(j_configuration_get_backend_path(jd_configuration, type), "{PORT}", port_str);

	if (!jd_is_server_for_backend(host, port, type))
	{
		return TRUE;
	}

	if (!j_backend_load(backend_name, J_BACKEND_COMPONENT_SERVER, type, module, backend))
	{
		return FALSE;
	}

	if (*backend == NULL)
	{
		return TRUE;
	}

	if (!backend_init(*backend, backend_path))
	{
		g_critical("Could not initialize %s backend %s.", type_str, backend_name);

		return FALSE;
	}

	g_debug("Initialized %s backend %s.", type_str, backend_name);

	return TRUE;
}

int
main(int argc, char** argv)
{
	J_TRACE_FUNCTION(NULL);

	gboolean opt_daemon = FALSE;
	g_autofree gchar* opt_host = NULL;
	gint opt_port = 0;

	JTrace* trace;
	GError* error = NULL;
	g_autoptr(GMainLoop) main_loop = NULL;
	GModule* object_module = NULL;
	GModule* kv_module = NULL;
	GModule* db_module = NULL;
	g_autoptr(GOptionContext) context = NULL;
	g_autoptr(GSocketService) socket_service = NULL;
	guint listen_retries = 0;

	GOptionEntry entries[] = {
		{ "daemon", 0, 0, G_OPTION_ARG_NONE, &opt_daemon, "Run as daemon", NULL },
		{ "host", 0, 0, G_OPTION_ARG_STRING, &opt_host, "Override host name", "hostname" },
		{ "port", 0, 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "0" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	// Explicitly enable UTF-8 since functions such as g_format_size might return UTF-8 characters.
	setlocale(LC_ALL, "C.UTF-8");

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		if (error)
		{
			g_warning("%s", error->message);
			g_error_free(error);
		}

		return 1;
	}

	if (opt_daemon && !jd_daemon())
	{
		return 1;
	}

	jd_configuration = j_configuration_new();

	if (jd_configuration == NULL)
	{
		g_warning("Could not read configuration.");
		return 1;
	}

	if (opt_host == NULL)
	{
		gchar hostname[HOST_NAME_MAX + 1];

		gethostname(hostname, HOST_NAME_MAX + 1);
		opt_host = g_strdup(hostname);
	}

	if (opt_port == 0)
	{
		opt_port = j_configuration_get_port(jd_configuration);
	}

	socket_service = g_threaded_socket_service_new(-1);
	g_socket_listener_set_backlog(G_SOCKET_LISTENER(socket_service), 128);

	while (TRUE)
	{
		if (!g_socket_listener_add_inet_port(G_SOCKET_LISTENER(socket_service), opt_port, NULL, &error))
		{
			if (error != NULL)
			{
				g_warning("%s", error->message);
				g_clear_error(&error);
			}

			listen_retries++;

			if (listen_retries < 10)
			{
				sleep(1);
				continue;
			}
			else
			{
				g_critical("Cannot listen on port %d after %d retries, giving up.", opt_port, listen_retries);
				return 1;
			}
		}

		break;
	}

	j_trace_init("julea-server");

	trace = j_trace_enter(G_STRFUNC, NULL);

	jd_object_backend = NULL;
	jd_kv_backend = NULL;
	jd_db_backend = NULL;

	if (!jd_load_and_init_backend(opt_host, opt_port, J_BACKEND_TYPE_OBJECT, &jd_object_backend, &object_module))
	{
		return 1;
	}

	if (!jd_load_and_init_backend(opt_host, opt_port, J_BACKEND_TYPE_KV, &jd_kv_backend, &kv_module))
	{
		return 1;
	}

	if (!jd_load_and_init_backend(opt_host, opt_port, J_BACKEND_TYPE_DB, &jd_db_backend, &db_module))
	{
		return 1;
	}

	jd_statistics = j_statistics_new(FALSE);
	g_mutex_init(jd_statistics_mutex);

	g_socket_service_start(socket_service);
	g_signal_connect(socket_service, "run", G_CALLBACK(jd_on_run), NULL);

	main_loop = g_main_loop_new(NULL, FALSE);

	g_unix_signal_add(SIGHUP, jd_signal, main_loop);
	g_unix_signal_add(SIGINT, jd_signal, main_loop);
	g_unix_signal_add(SIGTERM, jd_signal, main_loop);

	g_main_loop_run(main_loop);

	g_socket_service_stop(socket_service);

	g_mutex_clear(jd_statistics_mutex);
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
		j_backend_unload(jd_db_backend, db_module);
	}

	if (kv_module != NULL)
	{
		j_backend_unload(jd_kv_backend, kv_module);
	}

	if (object_module)
	{
		j_backend_unload(jd_object_backend, object_module);
	}

	j_configuration_unref(jd_configuration);

	j_trace_leave(trace);

	j_trace_fini();

	return 0;
}
