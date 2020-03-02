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
#include <unistd.h>

#include <julea.h>

#include "server.h"

#include <netinet/in.h> //for target address
#include <sys/socket.h>
#include <arpa/inet.h>

//signal handling
#include<signal.h>
#include<unistd.h>

#include <glib/gprintf.h>


//libfabric high level object
static struct fid_fabric* j_fabric;

//thread & server flags
static volatile gint thread_count = 0;
static volatile gboolean j_thread_running = TRUE;
static volatile gboolean j_server_running = TRUE;
//completion queue handling for waking on interrupt
static GPtrArray* thread_cq_array;
static GMutex thread_cq_array_mutex;

static JConfiguration* jd_configuration;


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


static
gboolean
jd_is_server_for_backend (gchar const* host, gint port, JBackendType backend_type)
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
* inits ressources for endpoint-threads
*/
gboolean j_thread_libfabric_ress_init(struct fi_info* info,
											 								struct fid_domain** domain,
										 	 								struct fid_eq** domain_eq,
										 	 								JEndpoint** jendpoint)
{
	int error;
	uint32_t event;
	gboolean ret;

	struct fi_eq_cm_entry* event_entry;
	uint32_t event_entry_size;
	struct fi_eq_err_entry event_queue_err_entry;

	struct fi_eq_attr event_queue_attr = * j_configuration_get_fi_eq_attr(jd_configuration);
	struct fi_cq_attr completion_queue_attr = * j_configuration_get_fi_cq_attr(jd_configuration);

	error = 0;
	event = 0;
	ret = FALSE;

	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
	(* jendpoint) = malloc(sizeof(struct JEndpoint));
	(* jendpoint)->max_msg_size = info->ep_attr->max_msg_size;

	error = fi_domain(j_fabric, info, domain, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating domain for active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_eq_open(j_fabric, &event_queue_attr, domain_eq, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating Domain Event queue.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_domain_bind(* domain, &(* domain_eq)->fid, 0);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while binding Event queue to Domain.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	//bulding endpoint
	error = fi_endpoint(* domain, info, &(* jendpoint)->endpoint, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	error = fi_eq_open(j_fabric, &event_queue_attr, &(* jendpoint)->event_queue, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating endpoint Event queue.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	error = fi_ep_bind((* jendpoint)->endpoint, &(* jendpoint)->event_queue->fid, 0);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while binding Event queue to active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_cq_open(* domain, &completion_queue_attr, &(* jendpoint)->completion_queue_receive, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating Completion receive queue for active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_cq_open(* domain, &completion_queue_attr, &(* jendpoint)->completion_queue_transmit, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating Completion transmit queue for active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_ep_bind((* jendpoint)->endpoint, &(* jendpoint)->completion_queue_transmit->fid, FI_TRANSMIT);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while binding Completion transmit queue to active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_ep_bind((* jendpoint)->endpoint, &(* jendpoint)->completion_queue_receive->fid, FI_RECV);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while binding Completion receive queue to active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	g_mutex_lock(&thread_cq_array_mutex);
	g_ptr_array_add(thread_cq_array, (* jendpoint)->completion_queue_receive);
	g_ptr_array_add(thread_cq_array, (* jendpoint)->completion_queue_transmit);
	g_mutex_unlock(&thread_cq_array_mutex);

	//PERROR no receive Buffer before accepting connetcion.

	error = fi_enable((* jendpoint)->endpoint);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while enabling Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	error = fi_accept((* jendpoint)->endpoint, NULL, 0);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while accepting connection request.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	event_entry = malloc(event_entry_size);
	error = fi_eq_sread((* jendpoint)->event_queue, &event, event_entry, event_entry_size, -1, 0);
	if(error != 0)
	{
		if(error == -FI_EAVAIL)
		{
			error = fi_eq_readerr((* jendpoint)->event_queue, &event_queue_err_entry, 0);
			if(error != 0)
			{
				g_critical("\nError occurred on Server while reading Error Message from Event queue (active Endpoint) reading for connection accept.\nDetails:\n%s", fi_strerror(error));
				error = 0;
				goto end;
			}
			else
			{
				g_critical("\nError Message on Server, on Event queue (active Endpoint) reading for connection accept available .\nDetails:\n%s", fi_eq_strerror((* jendpoint)->event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
		}
		else if(error == -FI_EAGAIN)
		{
			g_critical("\nNo Event data on Server Event Queue while reading for CONNECTED Event.\n");
			goto end;
		}
		else if(error < 0)
		{
			g_critical("\nError outside Error Event on Server Event Queue while reading for CONNECTED Event.\nDetails:\n%s", fi_strerror(error));
			goto end;
		}
	}
	free(event_entry);

	if(event == FI_CONNECTED)
	{
		ret = TRUE;
	}
	end:
	return ret;
}

/**
* shuts down thread ressources
*/
void
j_thread_libfabric_ress_shutdown (struct fi_info* info,
													 			  struct fid_domain* domain,
												   		   	struct fid_eq* domain_eq,
												 	 		  	JEndpoint* jendpoint)
{
	int error;
	gboolean berror;

	error = 0;
	berror = FALSE;

	//if shutdown initiated by server initiate a shutdown event on peer
	if(j_thread_running == FALSE)
	{
		error = fi_shutdown(jendpoint->endpoint, 0);
		if(error != 0)
		{
			g_critical("\nError on Server while shutting down connection\n.Details: \n%s", fi_strerror(error));
		}
		error = 0;
	}

  //end all fabric resources
	error = 0;

	fi_freeinfo(info);

	//remove cq of ending thread from thread_cq_array
	g_mutex_lock(&thread_cq_array_mutex);
	berror = g_ptr_array_remove_fast(thread_cq_array, jendpoint->completion_queue_receive);
	if(berror == FALSE)
	{
		g_critical("\nRemoving cq_recv information from thread_cq_array did not work.\n");
	}
	berror = g_ptr_array_remove_fast(thread_cq_array, jendpoint->completion_queue_transmit);
	if(berror == FALSE)
	{
		g_critical("\nRemoving cq_transmit information from thread_cq_array did not work.\n");
	}
	g_mutex_unlock(&thread_cq_array_mutex);

	error = fi_close(&jendpoint->endpoint->fid);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread endpoint.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&jendpoint->completion_queue_receive->fid);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread receive queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&jendpoint->completion_queue_transmit->fid);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread transmition queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&jendpoint->event_queue->fid);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread endpoint event queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&domain_eq->fid);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread domain event queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&domain->fid);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread domain.\n Details:\n %s", fi_strerror(error));
	}

	free(jendpoint);
}

//TODO dont build a domain for each thread
/**
* Main function for each spawned endpoint-thread
*/
static
gpointer
j_thread_function(gpointer connection_event_entry)
{
	//Needed Ressources
	int error = 0;
	uint32_t event = 0;

	struct fi_info* info;
	struct fid_domain* domain;
	struct fid_eq* domain_event_queue;

	struct fi_eq_cm_entry* event_entry;
	uint32_t event_entry_size;
	struct fi_eq_err_entry event_queue_err_entry;

	JEndpoint* jendpoint;

	J_TRACE_FUNCTION(NULL);

	JMemoryChunk* memory_chunk;
	g_autoptr(JMessage) message;
	JStatistics* statistics;
	guint64 memory_chunk_size;


	message = NULL;

	statistics = j_statistics_new(TRUE);
	memory_chunk_size = j_configuration_get_max_operation_size(jd_configuration);
	memory_chunk = j_memory_chunk_new(memory_chunk_size);

	message = j_message_new(J_MESSAGE_NONE, 0);

	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
	event_entry = (struct fi_eq_cm_entry*) connection_event_entry;

	info = fi_dupinfo(event_entry->info);
	fi_freeinfo(event_entry->info);
	free(connection_event_entry);

	if(j_thread_libfabric_ress_init(info, &domain, &domain_event_queue, &jendpoint) == TRUE)
	{
		do
		{

			if(j_message_receive(message, jendpoint))
			{
				jd_handle_message(message, jendpoint, memory_chunk, memory_chunk_size, statistics);
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


			//Read event queue repeat loop if no shutdown sent
			event = 0;
			event_entry = malloc(event_entry_size);
			error = fi_eq_read(jendpoint->event_queue, &event, event_entry, event_entry_size, 0);
			if(error != 0)
			{
				if(error == -FI_EAVAIL)
				{
					error = fi_eq_readerr(jendpoint->event_queue, &event_queue_err_entry, 0);
					if(error != 0)
					{
						g_critical("\nError occurred on Server while reading Error Message from Event queue (active Endpoint) reading for shutdown.\nDetails:\n%s", fi_strerror(error));
						goto end;
					}
					else
					{
						g_critical("\nError Message on Server, on Event queue (active Endpoint) reading for shutdown .\nDetails:\n%s", fi_eq_strerror(jendpoint->event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
						goto end;
					}
				}
				else if(error == -FI_EAGAIN)
				{
					//g_printf("\nNo Event data on event Queue on Server while reading for SHUTDOWN Event.\n");
				}
				else if(error < 0)
				{
					g_critical("\nError while reading completion queue after receiving Message (JMessage Header).\nDetails:\n%s", fi_strerror(error));
					goto end;
				}
			}
			free(event_entry);
		}while (!(event == FI_SHUTDOWN || j_thread_running == FALSE));
	}

	end:

	j_memory_chunk_free(memory_chunk);
	j_statistics_free(statistics);

	j_thread_libfabric_ress_shutdown(info, domain, domain_event_queue, jendpoint);

	g_atomic_int_dec_and_test(&thread_count);
	if(g_atomic_int_compare_and_exchange(&thread_count, 0, 0) && !j_thread_running)
	{
		j_server_running = FALSE; //set server running to false, thus ending server main loop if shutting down
		g_printf("\nLast Thread finished\n");
	}

	g_thread_exit(NULL);
	return NULL;
}


static
void
thread_unblock(gpointer completion_queue, gpointer user_data)
{
	int error = 0;
	error = fi_cq_signal((struct fid_cq*) completion_queue);
	if(error != 0)
	{
		g_critical("\nError waking up Threads\nDetails:\n %s", fi_strerror(error));
	}
}

//handling caught signal for ending server
//
//TODO input validation for SIGINT
void
sig_handler(int signal)
{
	if(signal == SIGHUP || signal == SIGINT || signal == SIGTERM)
	{
		j_thread_running = FALSE; //set thread running to false, thus ending thread loops
		if((g_atomic_int_compare_and_exchange(&thread_count, 0, 0)))
		{
			j_server_running = FALSE; //set server running to false, thus ending server main loop if no threads detected and signal caught
		}
		else
		{
			g_mutex_lock(&thread_cq_array_mutex);
			g_ptr_array_foreach(thread_cq_array, thread_unblock, NULL);
			g_mutex_unlock(&thread_cq_array_mutex);
		}
		switch(signal)
		{
			case SIGHUP:
				g_printf("\nSIGHUP caught\n");
				break;
			case SIGINT:
				g_printf("\nSIGINT caught\n");
				break;
			case SIGTERM:
				g_printf("\nSIGTERM caught\n");
				break;
			default:
				g_critical("\nsignal handler broken\n");
		}
	}
}

/*
/gets fi_info structure for internal information about available fabric ressources
/inits fabric, passive endpoint and event queue for the endpoint.
/Binds event queue to endpoint
*/
static
void
j_libfabric_ress_init(struct fi_info** info, struct fid_pep** passive_ep, struct fid_eq** passive_ep_event_queue)
{
	int error = 0;
	struct fi_eq_attr* event_queue_attr;
	struct fi_info* hints;

	error = 0;
	event_queue_attr = j_configuration_get_fi_eq_attr(jd_configuration);
	hints = fi_dupinfo(j_configuration_fi_get_hints(jd_configuration));

	if(hints == NULL)
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
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during server initializing info struct.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}
	if(* info == NULL)
	{
		g_critical("\nAllocating info struct did not work");
	}
	fi_freeinfo(hints);

	//Init fabric
	error = fi_fabric((* info)->fabric_attr, &j_fabric, NULL);
	if(error != FI_SUCCESS)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}
	if(j_fabric == NULL)
	{
		g_critical("\nAllocating j_fabric did not work");
	}

	//build event queue for passive endpoint
	error = fi_eq_open(j_fabric, event_queue_attr, passive_ep_event_queue, NULL);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	//build passive Endpoint
	error = fi_passive_ep(j_fabric, * info, passive_ep, NULL);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_pep_bind(* passive_ep, &(* passive_ep_event_queue)->fid, 0);
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during server-initializing libfabric ressources.\nDetails:\n %s", fi_strerror(error));
		error = 0;
	}
	//TODO manipulate backlog size
}


int
main (int argc, char** argv)
{
	J_TRACE_FUNCTION(NULL);

	gboolean opt_daemon = FALSE;
	g_autofree gchar* opt_host = NULL;
	gint opt_port = 4711;

	JTrace* trace;
	GError* error = NULL;
	g_autoptr(GMainLoop) main_loop = NULL;
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

	struct sigaction sig_action;

	GOptionEntry entries[] = {
		{ "daemon", 0, 0, G_OPTION_ARG_NONE, &opt_daemon, "Run as daemon", NULL },
		{ "host", 0, 0, G_OPTION_ARG_STRING, &opt_host, "Override host name", "hostname" },
		{ "port", 0, 0, G_OPTION_ARG_INT, &opt_port, "Port to use", "4711" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	//g_printf("\n\nSERVER STARTED\n\n");
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

	fi_error = fi_listen(passive_endpoint);
	if(fi_error != 0)
	{
		g_critical("\nError setting passive Endpoint to listening.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	//thread runs new active endpoint until shutdown event, then free elements //g_atomic_int_dec_and_test ()
	do
	{
		event = 0;
		event_entry = malloc(event_entry_size);
		fi_error = fi_eq_sread(passive_ep_event_queue, &event, event_entry, event_entry_size, 1000, 0); //timeout 5th argument in ms
		if(fi_error != 0)
		{
			if(fi_error == -FI_EAVAIL)
			{
				fi_error = fi_eq_readerr(passive_ep_event_queue, &event_queue_err_entry, 0);
				if(fi_error != 0)
				{
					g_critical("\nError while reading errormessage from Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_strerror(fi_error));
					fi_error = 0;
				}
				else
				{
					g_critical("\nError Message on Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_eq_strerror(passive_ep_event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				}
			}
			else
			{
				//g_critical("\nError while reading from Event queue (passive Endpoint) in main loop.\nDetails:\n%s", fi_strerror(fi_error)); //Can happen through device busy
				fi_error = 0;
			}
		}
		if(event == FI_CONNREQ)
		{
			g_atomic_int_inc (&thread_count);
			g_thread_new(NULL, *j_thread_function, (gpointer) event_entry);
		}
		else
		{
			free(event_entry);
		}
	} while(j_server_running == TRUE);

	fi_freeinfo(info);

	fi_error = fi_close(&passive_ep_event_queue->fid);
	if(fi_error != 0)
	{
		g_critical("\nSomething went horribly wrong closing passive Endpoint Event Queue.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	fi_error = fi_close(&passive_endpoint->fid);
	if(fi_error != 0)
	{
		g_critical("\nSomething went horribly wrong closing passive Endpoint.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

	fi_error = fi_close(&j_fabric->fid);
	if(fi_error != 0)
	{
		g_critical("\nSomething went horribly wrong closing Fabric.\n Details:\n %s", fi_strerror(fi_error));
		fi_error = 0;
	}

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

	g_printf("\nServer shutdown finished.\n");

	fflush(stdout);

	return 0;
}
