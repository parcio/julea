/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#include <netinet/in.h> //for target address
#include <sys/socket.h>
#include <arpa/inet.h>

#include <ifaddrs.h>

//signal handling
#include <signal.h>
#include <unistd.h>

#include <glib/gprintf.h>

//libfabric high level object
static struct fid_fabric* j_fabric;

//thread & server flags
static volatile gint thread_count = 0;
static volatile gboolean j_server_running = TRUE;
static volatile gboolean j_thread_running = TRUE;
//completion queue handling for waking on interrupt
static GPtrArray* thread_cq_array;
static GMutex thread_cq_array_mutex;

static JConfiguration* jd_configuration;

JStatistics* jd_statistics = NULL;
GMutex jd_statistics_mutex[1] = { 0 };

JBackend* jd_object_backend = NULL;
JBackend* jd_kv_backend = NULL;
JBackend* jd_db_backend = NULL;

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
		address = G_NETWORK_ADDRESS(g_network_address_parse(server, 4711, NULL));

		addr_server = g_network_address_get_hostname(address);
		addr_port = g_network_address_get_port(address);

		if (g_strcmp0(host, addr_server) == 0 && port == addr_port)
		{
			return TRUE;
		}
	}

	return FALSE;
}

/**
* handling caught signal for ending server
*/
void
sig_handler(int signal)
{
	if (signal == SIGHUP || signal == SIGINT || signal == SIGTERM)
	{
		j_thread_running = FALSE; //set thread running to false, thus ending thread loops
		if ((g_atomic_int_compare_and_exchange(&thread_count, 0, 0)))
		{
			j_server_running = FALSE; //set server running to false, thus ending server main loop if no threads detected and signal caught
		}
		else
		{
			g_mutex_lock(&thread_cq_array_mutex);
			//TODO replace workaround
			//g_ptr_array_foreach(thread_cq_array, thread_unblock, NULL);
			for (guint i = 0; i < thread_cq_array->len; i++)
			{
				thread_unblock((struct fid_cq*)g_ptr_array_index(thread_cq_array, i));
			}
			g_mutex_unlock(&thread_cq_array_mutex);
		}
		switch (signal)
		{
			case SIGHUP:
				g_printf("\nSERVER: SIGHUP caught\n");
				break;
			case SIGINT:
				g_printf("\nSERVER: SIGINT caught\n");
				break;
			case SIGTERM:
				g_printf("\nSERVER: SIGTERM caught\n");
				break;
			default:
				g_critical("\nSERVER: signal handler broken\n");
		}
	}
}

/*
/gets fi_info structure for internal information about available fabric ressources
/inits fabric, passive endpoint and event queue for the endpoint.
/Binds event queue to endpoint
*/
static void
j_libfabric_ress_init(struct fi_info** info, struct fid_pep** passive_ep, struct fid_eq** passive_ep_event_queue)
{
	int error = 0;
	struct fi_eq_attr* event_queue_attr;
	struct fi_info* hints;

	error = 0;
	event_queue_attr = j_configuration_get_fi_eq_attr(jd_configuration);
	hints = fi_dupinfo(j_configuration_fi_get_hints(jd_configuration));

	if (hints == NULL)
	{
		g_critical("\nAllocating empty hints did not work\n");
	}

	//get fi_info
	error = fi_getinfo(j_configuration_get_fi_version(jd_configuration),
			   j_configuration_get_fi_node(jd_configuration),
			   j_configuration_get_fi_service(jd_configuration),
			   j_configuration_get_fi_flags(jd_configuration, 0),
			   hints,
			   info);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong during server initializing info struct.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}
	if (*info == NULL)
	{
		g_critical("\nAllocating info struct did not work");
	}
	fi_freeinfo(hints);

	//Init fabric
	error = fi_fabric((*info)->fabric_attr, &j_fabric, NULL);
	if (error != FI_SUCCESS)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}
	if (j_fabric == NULL)
	{
		g_critical("\nAllocating j_fabric did not work");
	}

	//build event queue for passive endpoint
	error = fi_eq_open(j_fabric, event_queue_attr, passive_ep_event_queue, NULL);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	//build passive Endpoint
	error = fi_passive_ep(j_fabric, *info, passive_ep, NULL);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_pep_bind(*passive_ep, &(*passive_ep_event_queue)->fid, 0);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\nDetails:\n %s", fi_strerror(error));
		error = 0;
	}
	//TODO manipulate backlog size
}

int
main(int argc, char** argv)
{
	J_TRACE_FUNCTION(NULL);

	gboolean opt_daemon = FALSE;
	g_autofree gchar* opt_host = NULL;
	gint opt_port = 4711;

	JTrace* trace;
	GError* error = NULL;
	GModule* object_module = NULL;
	GModule* kv_module = NULL;
	GModule* db_module = NULL;
	g_autoptr(GOptionContext) context = NULL;
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

	int fi_error;
	uint32_t event;
	uint32_t event_entry_size;

	struct fi_info* info;
	struct fid_eq* passive_ep_event_queue;
	struct fid_pep* passive_endpoint;

	struct fi_eq_cm_entry* event_entry;
	struct fi_eq_err_entry event_queue_err_entry;

	struct ifaddrs* own_addr;
	struct ifaddrs* first_own_addr;

	ThreadData* thread_data;

	DomainManager* domain_manager;

	struct sigaction sig_action;

	GOptionEntry entries[] = {
		{ "daemon", 0, 0, G_OPTION_ARG_NONE, &opt_daemon, "Run as daemon", NULL },
		{ "host", 0, 0, G_OPTION_ARG_STRING, &opt_host, "Override host name", "hostname" },
		{ "port", 0, 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "4711" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	// Explicitly enable UTF-8 since functions such as g_format_size might return UTF-8 characters.
	setlocale(LC_ALL, "C.UTF-8");

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	printf("\n\nSERVER STARTED\n\n"); //debug
	fflush(stdout);
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

	fi_error = 0;
	event_entry_size = sizeof(event_entry) + 128;

	memset(&sig_action, 0, sizeof(struct sigaction));
	sig_action.sa_handler = sig_handler;

	sigaction(SIGINT, &sig_action, NULL);
	sigaction(SIGHUP, &sig_action, NULL);
	sigaction(SIGTERM, &sig_action, NULL);

	//gets a hostname if none given
	if (opt_host == NULL)
	{
		gchar hostname[HOST_NAME_MAX + 1];

		gethostname(hostname, HOST_NAME_MAX + 1);
		opt_host = g_strdup(hostname);
	}

	j_trace_init("julea-server");

	trace = j_trace_enter(G_STRFUNC, NULL);

	jd_configuration = j_configuration_new();
	if (jd_configuration == NULL)
	{
		g_warning("Could not read configuration.");
		return 1;
	}

	j_libfabric_ress_init(&info, &passive_endpoint, &passive_ep_event_queue);

	jd_object_backend = NULL;
	jd_kv_backend = NULL;
	jd_db_backend = NULL;

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

	if (jd_is_server_for_backend(opt_host, opt_port, J_BACKEND_TYPE_OBJECT)
	    && j_backend_load_server(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &object_module, &jd_object_backend))
	{
		if (jd_object_backend == NULL || !j_backend_object_init(jd_object_backend, object_path))
		{
			g_warning("\nCould not initialize object backend %s. \n", object_backend);
			return 1;
		}

		g_debug("Initialized object backend %s.", object_backend);
	}

	if (jd_is_server_for_backend(opt_host, opt_port, J_BACKEND_TYPE_KV)
	    && j_backend_load_server(kv_backend, kv_component, J_BACKEND_TYPE_KV, &kv_module, &jd_kv_backend))
	{
		if (jd_kv_backend == NULL || !j_backend_kv_init(jd_kv_backend, kv_path))
		{
			g_warning("\nCould not initialize kv backend %s.\n", kv_backend);
			return 1;
		}

		g_debug("Initialized kv backend %s.", kv_backend);
	}

	if (jd_is_server_for_backend(opt_host, opt_port, J_BACKEND_TYPE_DB)
	    && j_backend_load_server(db_backend, db_component, J_BACKEND_TYPE_DB, &db_module, &jd_db_backend))
	{
		if (jd_db_backend == NULL || !j_backend_db_init(jd_db_backend, db_path))
		{
			g_warning("\nCould not initialize db backend %s.\n", db_backend);
			return 1;
		}

		g_debug("Initialized db backend %s.", db_backend);
	}

	jd_statistics = j_statistics_new(FALSE);
	g_mutex_init(jd_statistics_mutex);

	thread_cq_array = g_ptr_array_new();
	g_mutex_init(&thread_cq_array_mutex);

	domain_manager = domain_manager_init();

	fi_error = fi_listen(passive_endpoint);
	if (fi_error != 0)
	{
		g_critical("\nError setting passive Endpoint to listening.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	//debug
	fi_error = getifaddrs(&own_addr);
	first_own_addr = own_addr;
	if (fi_error < 0)
	{
		g_critical("\nSERVER: getifaddrs failed\n");
	}

	printf("\nSERVER network adresses:\n");
	fflush(stdout);
	while (own_addr != NULL)
	{
		printf("	Interface Name: %s\n	IP Address: %s\n\n", own_addr->ifa_name, inet_ntoa(((struct sockaddr_in*)own_addr->ifa_addr)->sin_addr));
		fflush(stdout);
		own_addr = own_addr->ifa_next;
	}
	freeifaddrs(first_own_addr);

	//thread runs new active endpoint until shutdown event, then free elements //g_atomic_int_dec_and_test ()
	printf("\nSERVER: Main loop started\n"); //debug
	fflush(stdout);
	do
	{
		event = 0;
		event_entry = malloc(event_entry_size);
		fi_error = fi_eq_sread(passive_ep_event_queue, &event, event_entry, event_entry_size, 1000, 0); //timeout 5th argument in ms
		if (fi_error < 0)
		{
			if (fi_error == -FI_EAVAIL)
			{
				fi_error = fi_eq_readerr(passive_ep_event_queue, &event_queue_err_entry, 0);
				if (fi_error < 0)
				{
					g_critical("\nSERVER:nError while reading errormessage from Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_strerror(fi_error));
					fi_error = 0;
				}
				else
				{
					g_critical("\nSERVER: Error Message on Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_eq_strerror(passive_ep_event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				}
			}
			else
			{
				//g_critical("\nError while reading from Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_strerror(fi_error)); //Can happen through device busy
				fi_error = 0;
			}
		}
		if (event == FI_CONNREQ)
		{
			printf("\nSERVER: FI_CONNREQ found\n"); //debug
			fflush(stdout);
			thread_data = malloc(sizeof(ThreadData) + 128);
			//TODO put relevatn data into thread data
			thread_data->event_entry = event_entry;
			thread_data->j_configuration = jd_configuration;
			thread_data->thread_cq_array = thread_cq_array;
			thread_data->thread_cq_array_mutex = &thread_cq_array_mutex;
			thread_data->fabric = j_fabric;
			thread_data->server_running = &j_server_running;
			thread_data->thread_running = &j_thread_running;
			thread_data->thread_count = &thread_count;
			thread_data->j_statistics = jd_statistics;
			thread_data->j_statistics_mutex = &jd_statistics_mutex[0];
			thread_data->domain_manager = domain_manager;
			g_thread_new(NULL, *j_thread_function, (gpointer)thread_data);
		}
		else
		{
			printf("\nSERVER: No connection request"); //DEBUG
			fflush(stdout);
			free(event_entry);
		}
	} while (j_server_running == TRUE);

	fi_freeinfo(info);

	fi_error = fi_close(&passive_ep_event_queue->fid);
	if (fi_error != 0)
	{
		g_critical("\nSomething went horribly wrong closing passive Endpoint Event Queue.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	fi_error = fi_close(&passive_endpoint->fid);
	if (fi_error != 0)
	{
		g_critical("\nSomething went horribly wrong closing passive Endpoint.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	fi_error = fi_close(&j_fabric->fid);
	if (fi_error != 0)
	{
		g_critical("\nSomething went horribly wrong closing Fabric.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	domain_manager_fini(domain_manager);
	g_ptr_array_unref(thread_cq_array);
	g_mutex_clear(&thread_cq_array_mutex);

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

	j_configuration_unref(jd_configuration);

	j_trace_leave(trace);

	j_trace_fini();

	g_printf("\nSERVER: shutdown finished.\n");

	fflush(stdout);

	return 0;
}
