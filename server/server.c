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
static JFabric* jfabric;

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

struct PepList
{
	struct fi_info* info;
	struct fid_pep* pep;
	struct PepList* next;
	struct PepList* first;
	int cntr;
};
typedef struct PepList PepList;

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
static gboolean
j_libfabric_ress_init(PepList** pep_list, struct fid_eq** passive_ep_event_queue)
{
	int error;
	gboolean ret;
	struct fi_eq_attr* event_queue_attr;

	struct ifaddrs* current_own_addr;
	struct ifaddrs* own_addr_list;
	char my_hostname[HOST_NAME_MAX + 1];

	error = 0;
	ret = FALSE;
	event_queue_attr = j_configuration_get_fi_eq_attr(jd_configuration);

	error = getifaddrs(&current_own_addr);
	own_addr_list = current_own_addr;
	if (error < 0)
	{
		g_critical("\nSERVER: getifaddrs failed\n");
		goto end;
	}

	error = gethostname(my_hostname, HOST_NAME_MAX + 1);
	if (error < 0)
	{
		g_critical("SERVER: gethostname failed");
		goto end_hostname;
	}

	if (!j_fabric_init(&jfabric, J_SERVER, jd_configuration, "SERVER"))
	{
		goto end_hints;
	}

	//build event queue for passive endpoint
	error = fi_eq_open(j_get_fabric(jfabric, J_MSG), event_queue_attr, passive_ep_event_queue, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error initializing msg event_queue\n Details:\n %s", fi_strerror(abs(error)));
		goto end_hints;
	}

	//go through all interfaces, build new fi_info for each, pass info to respective new passive_ep, put it in queue, return queue
	//debug
	//printf("\nSERVER Network Adresses:\n	Hostname:%s\n", my_hostname);

	while (current_own_addr != NULL)
	{
		struct fid_pep* passive_ep;
		struct fi_info* tmp_info;
		PepList* pep_list_next;
		gboolean error_occoured;

		error_occoured = FALSE;
		//get fi_info
		error = fi_getinfo(j_configuration_get_fi_version(jd_configuration),
				   inet_ntoa(((struct sockaddr_in*)current_own_addr->ifa_addr)->sin_addr),
				   j_configuration_get_fi_service(jd_configuration),
				   j_configuration_get_fi_flags(jd_configuration, 0),
				   j_configuration_fi_get_hints(jd_configuration, J_MSG),
				   &tmp_info);
		if (error != 0 && error != -FI_ENODATA)
		{
			g_critical("\nSERVER: Error during initializing msg fi_info struct.\n Details:\n %s", fi_strerror(abs(error)));
			error_occoured = TRUE;
		}
		else if (error == 0)
		{
			//build passive Endpoints
			error = fi_passive_ep(j_get_fabric(jfabric, J_MSG), tmp_info, &passive_ep, NULL);
			if (error != 0)
			{
				g_critical("\nSERVER: Error building passive Endpoint for %s-Interface with IP: %s.\nDetails:\n %s", current_own_addr->ifa_name, inet_ntoa(((struct sockaddr_in*)current_own_addr->ifa_addr)->sin_addr), fi_strerror(abs(error)));
				error_occoured = TRUE;
			}

			error = fi_pep_bind(passive_ep, &(*passive_ep_event_queue)->fid, 0);
			if (error != 0)
			{
				g_critical("\nSERVER: Error binding passive Endpoint to %s-Interface with IP: %s.\nDetails:\n %s", current_own_addr->ifa_name, inet_ntoa(((struct sockaddr_in*)current_own_addr->ifa_addr)->sin_addr), fi_strerror(abs(error)));
				error_occoured = TRUE;
			}

			error = fi_listen(passive_ep);
			if (error != 0)
			{
				g_critical("\nSERVER: Error setting passive Endpoint to listening to %s-Interface with IP: %s\nDetails:\n %s", current_own_addr->ifa_name, inet_ntoa(((struct sockaddr_in*)current_own_addr->ifa_addr)->sin_addr), fi_strerror(abs(error)));
				error_occoured = TRUE;
			}

			if ((*pep_list) == NULL)
			{
				(*pep_list) = malloc(sizeof(struct PepList));
				(*pep_list)->first = (*pep_list);
				(*pep_list)->info = tmp_info;
				(*pep_list)->pep = passive_ep;
				(*pep_list)->next = NULL;
				(*pep_list)->cntr = 0;
			}
			else
			{
				pep_list_next = malloc(sizeof(struct PepList));
				pep_list_next->first = (*pep_list)->first;
				pep_list_next->info = tmp_info;
				pep_list_next->pep = passive_ep;
				pep_list_next->next = NULL;
				pep_list_next->cntr = (*pep_list)->cntr++;
				(*pep_list)->next = pep_list_next;
				(*pep_list) = pep_list_next;
			}

			if (error_occoured)
			{
				goto end_bld_peps;
			}

			//debug
			//printf("\n	Ep established for\n		Interface Name: %s\n		IP Address: %s\n", current_own_addr->ifa_name, inet_ntoa(((struct sockaddr_in*)current_own_addr->ifa_addr)->sin_addr));
			//fflush(stdout);
		}

		current_own_addr = current_own_addr->ifa_next;
	}

	(*pep_list) = (*pep_list)->first;
	ret = TRUE;
	goto end_hints;

end_bld_peps:
	(*pep_list) = (*pep_list)->first;
	while ((*pep_list) != NULL)
	{
		fi_close(&(*pep_list)->pep->fid);
		fi_freeinfo((*pep_list)->info);
		(*pep_list) = (*pep_list)->next;
	}
end_hints:
	freeifaddrs(own_addr_list);
end_hostname:
end:
	return ret;
	// TODO manipulate backlog size
}

int
main(int argc, char** argv)
{
	J_TRACE_FUNCTION(NULL);

	gboolean allow_main_loop = FALSE;
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

	struct fid_eq* passive_ep_event_queue;
	PepList* pep_list;

	JDomainManager* domain_manager;

	struct sigaction* sig_action;

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

	//printf("\n\nSERVER STARTED\n\n"); //debug
	//fflush(stdout);

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

	sig_action = g_malloc(sizeof(struct sigaction));
	sig_action->sa_handler = sig_handler;

	sigaction(SIGINT, sig_action, NULL);
	sigaction(SIGHUP, sig_action, NULL);
	sigaction(SIGTERM, sig_action, NULL);

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
		g_warning("SERVER: Could not read configuration.");
		return 1;
	}

	pep_list = NULL;
	if (!j_libfabric_ress_init(&pep_list, &passive_ep_event_queue))
	{
		g_critical("\nSERVER: init of libfabric ressources did not work\n");
		return 1;
	}
	else
	{
		allow_main_loop = TRUE;
	}

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
			g_warning("\nSERVER: Could not initialize object backend %s. \n", object_backend);
			return 1;
		}

		g_debug("SERVER: Initialized object backend %s.", object_backend);
	}

	if (jd_is_server_for_backend(opt_host, opt_port, J_BACKEND_TYPE_KV)
	    && j_backend_load_server(kv_backend, kv_component, J_BACKEND_TYPE_KV, &kv_module, &jd_kv_backend))
	{
		if (jd_kv_backend == NULL || !j_backend_kv_init(jd_kv_backend, kv_path))
		{
			g_warning("\nSERVER: Could not initialize kv backend %s.\n", kv_backend);
			return 1;
		}

		g_debug("SERVER: Initialized kv backend %s.", kv_backend);
	}

	if (jd_is_server_for_backend(opt_host, opt_port, J_BACKEND_TYPE_DB)
	    && j_backend_load_server(db_backend, db_component, J_BACKEND_TYPE_DB, &db_module, &jd_db_backend))
	{
		if (jd_db_backend == NULL || !j_backend_db_init(jd_db_backend, db_path))
		{
			g_warning("\nSERVER: Could not initialize db backend %s.\n", db_backend);
			return 1;
		}

		g_debug("SERVER: Initialized db backend %s.", db_backend);
	}

	jd_statistics = j_statistics_new(FALSE);
	g_mutex_init(jd_statistics_mutex);

	thread_cq_array = g_ptr_array_new();
	g_mutex_init(&thread_cq_array_mutex);

	domain_manager = j_domain_manager_init();

	//thread runs new active endpoint until shutdown event, then free elements //g_atomic_int_dec_and_test ()
	if (allow_main_loop)
	{
		GSList* connreq_list;
		uint32_t event_entry_size;
		ThreadData* thread_data;

		// printf("\nSERVER: Main loop started\n"); //debug
		//fflush(stdout);

		// libfabric event entries need more space than the size of the struct due to possible protocoll padding, 128 was an example size, real is smaller.
		// in addition space for the identifiers is added (JConData + the 37 needed for the uuid string)
		event_entry_size = sizeof(struct fi_eq_cm_entry*) + 128 + sizeof(struct JConData);
		connreq_list = NULL;

		do
		{
			ssize_t connreq_size;
			gboolean connreq_found;
			uint32_t event;
			struct fi_eq_cm_entry* event_entry;
			struct fi_eq_err_entry event_queue_err_entry;
			JConData* con_data;

			thread_data = NULL;
			event = 0;
			event_entry = malloc(event_entry_size);

			fi_error = fi_eq_sread(passive_ep_event_queue, &event, event_entry, event_entry_size, 1000, 0); //timeout 5th argument in ms
			connreq_size = fi_error;
			if (fi_error < 0)
			{
				if (fi_error == -FI_EAVAIL)
				{
					fi_error = fi_eq_readerr(passive_ep_event_queue, &event_queue_err_entry, 0);
					if (fi_error < 0)
					{
						g_critical("\nSERVER:nError while reading errormessage from Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_strerror(abs(fi_error)));
						fi_error = 0;
					}
					else
					{
						g_critical("\nSERVER: Error Message on Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_eq_strerror(passive_ep_event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
					}
				}
				else if (fi_error != -FI_EAGAIN && fi_error != -EINTR)
				{
					g_critical("\nSERVER: Error while reading from Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_strerror(abs(fi_error)));
					fi_error = 0;
				}
			}
			if (event == FI_CONNREQ && j_server_running && j_thread_running)
			{
				// printf("\nSERVER: FI_CONNREQ found\n"); //debug
				// printf("	total size: %ld\n	event_entry_size: %d\n	offset start: %ld\n	event-entry*: %p\n	con_data*: %p\n", connreq_size, event_entry_size, connreq_size - sizeof(struct JConData), (void*) event_entry, (void*) con_data);
				// fflush(stdout);

				// calculates offset after which user data (JConData) begins, cast to char* because char* are incremented by 1 byte and the numbers are in number of bytes
				con_data = (JConData*)(((char*)event_entry) + (connreq_size - sizeof(struct JConData)));
				connreq_found = FALSE;

				for (guint i = 0; g_slist_length(connreq_list); i++)
				{
					thread_data = g_slist_nth_data(connreq_list, i);
					if (g_strcmp0(thread_data->connection.uuid, con_data->uuid) == 0)
					{
						connreq_found = TRUE;
						break;
					}
				}

				// if a corresponding connreq is found, add the 2nd request to the thread data and start a thread
				if (connreq_found)
				{
					if (con_data->type == J_MSG)
					{
						if (thread_data->connection.rdma_event == NULL)
						{
							g_critical("\nFaulty ThreadData on connreq_list. Found no RDMA request.\n");
						}
						else
						{
							thread_data->connection.msg_event = event_entry;
						}
					}
					else if (con_data->type == J_RDMA)
					{
						if (thread_data->connection.msg_event == NULL)
						{
							g_critical("\nFaulty ThreadData on connreq_list. Found no MSG request.\n");
						}
						else
						{
							thread_data->connection.rdma_event = event_entry;
						}
					}
					else
					{
						g_critical("\nSERVER: Did not read a valid connection type from connreq while holding a valid connreq pair\n");
					}

					g_thread_new(NULL, *j_thread_function, (gpointer)thread_data);
					connreq_list = g_slist_remove(connreq_list, (gpointer)thread_data);
				}
				else // if no corresponding thread data is found, build a new thread_data with the first connreq and add to list
				{
					if (!g_uuid_string_is_valid(con_data->uuid))
					{
						g_critical("\nSERVER: Did not read a valid uuid from connreq\n");
					}
					thread_data = malloc(sizeof(ThreadData));
					switch (con_data->type)
					{
						case J_MSG:
							thread_data->connection.msg_event = event_entry;
							thread_data->connection.rdma_event = NULL;
							break;
						case J_RDMA:
							thread_data->connection.msg_event = NULL;
							thread_data->connection.rdma_event = event_entry;
							break;
						case J_UNDEFINED:
						default:
							g_assert_not_reached();
					}
					thread_data->connection.j_configuration = jd_configuration;
					thread_data->connection.fabric = jfabric;
					thread_data->connection.domain_manager = domain_manager;
					thread_data->connection.uuid = g_strdup(con_data->uuid);
					thread_data->julea_state.thread_cq_array = thread_cq_array;
					thread_data->julea_state.thread_cq_array_mutex = &thread_cq_array_mutex;
					thread_data->julea_state.server_running = &j_server_running;
					thread_data->julea_state.thread_running = &j_thread_running;
					thread_data->julea_state.thread_count = &thread_count;
					thread_data->julea_state.j_statistics = jd_statistics;
					thread_data->julea_state.j_statistics_mutex = &jd_statistics_mutex[0];

					connreq_list = g_slist_append(connreq_list, (gpointer)thread_data);
				}
			}
			else
			{
				//printf("\nSERVER: No connection request"); //DEBUG
				//fflush(stdout);
				free(event_entry);
			}
		} while (j_server_running == TRUE);

		// close possible event entries left on connreq_list + deny connreq
		{
			PepList* first_entry;
			first_entry = pep_list;

			for (guint i = 0; g_slist_length(connreq_list); i++)
			{
				thread_data = g_slist_nth_data(connreq_list, i);

				if (thread_data->connection.msg_event != NULL)
				{
					if (pep_list != NULL)
					{
						while (pep_list != NULL)
						{
							if (((struct sockaddr_in*)pep_list->info->src_addr)->sin_addr.s_addr == ((struct sockaddr_in*)thread_data->connection.msg_event->info->src_addr)->sin_addr.s_addr && ((struct sockaddr_in*)pep_list->info->src_addr)->sin_port == ((struct sockaddr_in*)thread_data->connection.msg_event->info->src_addr)->sin_port)
							{
								fi_error = fi_reject(pep_list->pep, thread_data->connection.msg_event->fid, NULL, 0);
								break;
							}
							pep_list = pep_list->next;
						}
					}
					pep_list = first_entry;
					fi_freeinfo(thread_data->connection.msg_event->info);
					free(thread_data->connection.msg_event);
				}
				if (thread_data->connection.rdma_event != NULL)
				{
					if (pep_list != NULL)
					{
						while (pep_list != NULL)
						{
							if (((struct sockaddr_in*)pep_list->info->src_addr)->sin_addr.s_addr == ((struct sockaddr_in*)thread_data->connection.rdma_event->info->src_addr)->sin_addr.s_addr && ((struct sockaddr_in*)pep_list->info->src_addr)->sin_port == ((struct sockaddr_in*)thread_data->connection.rdma_event->info->src_addr)->sin_port)
							{
								fi_error = fi_reject(pep_list->pep, thread_data->connection.rdma_event->fid, NULL, 0);
								break;
							}
							pep_list = pep_list->next;
						}
					}
					pep_list = first_entry;
					fi_freeinfo(thread_data->connection.rdma_event->info);
					free(thread_data->connection.rdma_event);
				}
				free(thread_data);
			}
			g_slist_free(connreq_list);
		}
	}

	// close passive event queue
	// printf("\nSERVER: Close pep_eq\n"); //debug
	// fflush(stdout);
	fi_error = fi_close(&passive_ep_event_queue->fid);
	if (fi_error != 0)
	{
		g_critical("\nSERVER: Error shutting down passive event queue.\n Details:\n %s", fi_strerror(abs(fi_error)));
		fi_error = 0;
	}

	//==CLosing ressources==
	//	Close passive endpoints & associated fi_infos

	{
		int len;

		len = 0;
		if (pep_list != NULL)
		{
			len++;
			while (pep_list->next != NULL)
			{
				len++;
				pep_list = pep_list->next;
			}
		}

		pep_list = pep_list->first;

		//printf("\nSERVER: Finishing PepList of size %d\n", len); //debug
		//fflush(stdout);

		while (pep_list != NULL)
		{
			PepList* tmp_pep_list;

			//printf("\nSERVER: Finishing PepList entry %d\n", pep_list->cntr); //debug
			//fflush(stdout);
			//printf("\n	close PeP\n"); //debug
			//fflush(stdout);

			fi_error = fi_close(&pep_list->pep->fid);
			if (fi_error != 0)
			{
				g_critical("\nSERVER: Error closing passive Endpoint %d out of %d.\n Details:\n %s", pep_list->cntr + 1, len, fi_strerror(abs(fi_error)));
				fi_error = 0;
			}

			//printf("\n	close fi_info\n"); //debug
			//fflush(stdout);
			fi_freeinfo(pep_list->info);

			tmp_pep_list = pep_list->next;
			free(pep_list);
			pep_list = tmp_pep_list;
		}
	}

	// close fabric
	j_fabric_fini(jfabric, "SERVER");

	j_domain_manager_fini(domain_manager);
	g_ptr_array_unref(thread_cq_array);
	g_mutex_clear(&thread_cq_array_mutex);

	//close libfabric done

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

	g_free(sig_action);

	j_configuration_unref(jd_configuration);

	j_trace_leave(trace);

	j_trace_fini();

	g_printf("\nSERVER: shutdown finished.\n");

	fflush(stdout);

	return 0;
}
