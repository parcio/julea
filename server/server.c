/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include "server.h"

#include <rdma/fabric.h>
#include <rdma/fi_domain.h> //includes cqs and
#include <rdma/fi_cm.h> //connection management
#include <rdma/fi_errno.h> //translation error numbers

#include <netinet/in.h> //for target address

//libfabric high level objects
static fid_fabric* j_fid_fabric;
static fid_eq* j_fid_passive_ep_eventqueue;
//libfabric config structures
static fi_info* j_fi_info;

static JConfiguration* jd_configuration;

static
gboolean
jd_signal (gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	GMainLoop* main_loop = data;

	if (g_main_loop_is_running(main_loop))
	{
		g_main_loop_quit(main_loop);
	}

	return FALSE;
}

static
gboolean
jd_on_run (GThreadedSocketService* service, GSocketConnection* connection, GObject* source_object, gpointer user_data) //TODO change connection to endpoint
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

static
gboolean
jd_daemon (void)
{
	J_TRACE_FUNCTION(NULL);

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

//TODO
static
jboolean jd_compare_domain_infos(fid_info* info1, fid_info* info2)
{
	jboolean ret FALSE;
	if( != ) goto end;

	ret = TRUE;
	end:
	return ret;
}


int
main (int argc, char** argv)
{
	J_TRACE_FUNCTION(NULL);

	gboolean opt_daemon = FALSE;
	gint opt_port = 4711;

	JTrace* trace;
	GError* error = NULL;
	g_autoptr(GMainLoop) main_loop = NULL;
	GModule* object_module = NULL;
	GModule* kv_module = NULL;
	GModule* db_module = NULL;
	g_autoptr(GOptionContext) context = NULL;
	g_autoptr(GSocketService) socket_service = NULL; //TODO not needed anymore for Program, but relevant for entries parse?
	gchar const* object_backend;
	gchar const* object_component;
	g_autofree gchar* object_path = NULL;
	gchar const* kv_backend;
	gchar const* kv_component;
	g_autofree gchar* kv_path = NULL;
	gchar const* db_backend;
	gchar const* db_component;
	g_autofree gchar* db_path = NULL;
	g_autofree gchar* port_str = NULL;

	int fi_error = 0;
	int version = FI_VERSION(FI_MAJOR(1, 6); //versioning Infos from libfabric, should be hardcoded so server and client run same versions, not the available ones
	const char* node = NULL; //NULL if addressing Format defined, otherwise can somehow be used to parse hostnames
	const char* service = "4711"; //target port (in future maybe not hardcoded)
	uint64_t flags = 0;// Alternatives: FI_NUMERICHOST (defines node to be a doted IP) // FI_SOURCE (source defined by node+service)
	fid_pep* passive_endpoint;

	fi_info* fi_hints = NULL; //config object

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

	//TODO init libfabric ressources. Up to init passive endpoint (prob new function)
	fi_hints = fi_allocinfo(); //initiated object is zeroed

	if(fi_hints == NULL)
	{
		g_critical("Allocating empty hints did not work");
	}
	//TODO read hints from config (and define corresponding fields there) + or all given caps
	//define Fabric attributes
	fi_hints.caps = FI_MSG | FI_SEND | FI_RECV;
	fi_hints->fabric_attr->prov_name = "sockets"; //sets later returned providers to socket providers, TODO for better performance not socket, but the best (first) available
	fi_hints->adr_format = FI_SOCKADDR_IN; //Server-Adress Format IPV4. TODO: Change server Definition in Config or base system of name resolution
	//TODO future support to set modes
	//fi_hints.mode = 0;
	fi_hints->domain_attr.threading = FI_THREAD_FID; //FI_THREAD_COMPLETION or FI_THREAD_FID or FI_THREAD_SAFE

	//get fi_info
	fi_error = fi_getinfo(version, node, service, flags,fi_hints, &j_fi_info)
	fi_freeinfo(fi_hints); //hints only used for config
	if(fi_error != 0)
	{
		g_critical("Something went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(fi_error));
	}
	if(j_fi_info == NULL)
	{
		g_critical("Allocating j_fi_info did not work");
	}
	fi_error = 0;

	//Init fabric
	fi_error = fi_fabric(j_fi_info->fabric_attr, &j_fid_fabric, NULL);
	if(fi_error != FI_SUCCESS)
	{
		g_critical("Something went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(fi_error));
	}
	if(j_fid_fabric == NULL)
	{
		g_critical("Allocating j_fid_fabric did not work");
	}
	fi_error = 0;

	//build event queue for passive endpoint
	//PERROR: Wrong formatting of event queue attributes
	struct fi_eq_attr eventqueue_attr = {50, FI_WRITE, FI_WAIT_UNSPEC, 0, NULL};
	fi_error = fi_eq_open(j_fid_fabric, &eventqueue_attr, &j_fid_passive_ep_eventqueue, NULL);
	if(fi_error != 0)
	{
		g_critical("Something went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(fi_error));
	}
	fi_error = 0;


	//init passive Endpoint
	fi_error = fi_passive_ep(j_fid_fabric, j_fi_info, &passive_endpoint, NULL);
	if(fi_error != 0)
	{
		g_critical("Something went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(fi_error));
	}
	fi_error = 0;

	fi_error = fi_pep_bind(passive_endpoint, j_fid_passive_ep_eventqueue, 0);
	if(fi_error != 0)
	{
		g_critical("Something went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(fi_error));
	}
	fi_error = 0;

	/*
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
	*/

	j_trace_init("julea-server");

	trace = j_trace_enter(G_STRFUNC, NULL);

	jd_configuration = j_configuration_new();

	if (jd_configuration == NULL)
	{
		g_printerr("Could not read configuration.\n");
		return 1;
	}

	port_str = g_strdup_printf("%d", opt_port);

	object_backend = j_configuration_get_backend(jd_configuration, J_BACKEND_TYPE_OBJECT);
	object_component = j_configuration_get_backend_component(jd_configuration, J_BACKEND_TYPE_OBJECT);
	object_path = j_helper_str_replace(j_configuration_get_backend_path(jd_configuration, J_BACKEND_TYPE_OBJECT), "{PORT}", port_str);

	kv_backend = j_configuration_get_backend(jd_configuration, J_BACKEND_TYPE_KV);
	kv_component = j_configuration_get_backend_component(jd_configuration, J_BACKEND_TYPE_KV);
	kv_path = j_helper_str_replace(j_configuration_get_backend_path(jd_configuration, J_BACKEND_TYPE_KV), "{PORT}", port_str);

	db_backend = j_configuration_get_backend(jd_configuration, J_BACKEND_TYPE_DB);
	db_component = j_configuration_get_backend_component(jd_configuration, J_BACKEND_TYPE_DB);
	db_path = j_helper_str_replace(j_configuration_get_backend_path(jd_configuration, J_BACKEND_TYPE_DB), "{PORT}", port_str);

	if (j_backend_load_server(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &object_module, &jd_object_backend))
	{
		if (jd_object_backend == NULL || !j_backend_object_init(jd_object_backend, object_path))
		{
			g_critical("Could not initialize object backend %s.\n", object_backend);
			return 1;
		}
	}

	if (j_backend_load_server(kv_backend, kv_component, J_BACKEND_TYPE_KV, &kv_module, &jd_kv_backend))
	{
		if (jd_kv_backend == NULL || !j_backend_kv_init(jd_kv_backend, kv_path))
		{
			g_critical("Could not initialize kv backend %s.\n", kv_backend);
			return 1;
		}
	}

	if (j_backend_load_server(db_backend, db_component, J_BACKEND_TYPE_DB, &db_module, &jd_db_backend))
	{
		if (jd_db_backend == NULL || !j_backend_db_init(jd_db_backend, db_path))
		{
			g_critical("Could not initialize db backend %s.\n", db_backend);
			return 1;
		}
	}

	jd_statistics = j_statistics_new(FALSE);
	g_mutex_init(jd_statistics_mutex);

	fi_error = fi_listen(passive_endpoint);
	if(fi_error != 0)
	{
		g_critical("Something went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(fi_error));
	}
	fi_error = 0;
	/*
	g_socket_service_start(socket_service); //TODO start listening, if called, start new thread with endpoint
	g_signal_connect(socket_service, "run", G_CALLBACK(jd_on_run), NULL);
	*/

	//TODO: if connreq, new thread,
		//thread runs new active endpoint until shutdown event, then free elements


	//TODO replace main loop
	main_loop = g_main_loop_new(NULL, FALSE);

	g_unix_signal_add(SIGHUP, jd_signal, main_loop);
	g_unix_signal_add(SIGINT, jd_signal, main_loop);
	g_unix_signal_add(SIGTERM, jd_signal, main_loop);

	g_main_loop_run(main_loop);

	g_socket_service_stop(socket_service); //TODO end listening

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

	//TODO unbind Fabric ressources

	j_configuration_unref(jd_configuration);

	j_trace_leave(trace);

	j_trace_fini();

	return 0;
}
