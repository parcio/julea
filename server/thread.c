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

#include <julea.h>

#include "thread.h"

struct ThreadData
{
	struct
	{
		struct fi_eq_cm_entry* msg_event;
		struct fi_eq_cm_entry* rdma_event;
		JFabric* jfabric;
		JConfiguration* configuration;
		JDomainManager* domain_manager;
		gchar* uuid;
	} connection;

	struct
	{
		GPtrArray* thread_cq_array; // for registration in waking threads
		GMutex* thread_cq_array_mutex; // for registration in waking threads
		volatile gboolean* server_running;
		volatile gboolean* thread_running;
		volatile gint* thread_count;
		JStatistics* j_statistics;
		GMutex* j_statistics_mutex;
	} julea_state;
};
typedef struct ThreadData ThreadData;

static JFabric* jfabric;

static volatile gboolean* j_thread_running;
static volatile gboolean* j_server_running;

static GPtrArray* thread_cq_array;
static GMutex* thread_cq_array_mutex;

static volatile gint* thread_count;

static JConfiguration* jd_configuration;

static JDomainManager* domain_manager;

void
thread_unblock(struct fid_cq* completion_queue)
{
	int error = 0;
	error = fi_cq_signal(completion_queue);
	if (error != 0)
	{
		g_critical("\nSERVER: Error waking up Threads\nDetails:\n %s", fi_strerror(abs(error)));
	}
}

/**
* inits ressources for endpoint-threads
*/
gboolean
j_thread_libfabric_ress_init(gpointer thread_data, JEndpoint** jendpoint)
{
	int error;
	uint32_t event;
	gboolean ret_msg;
	gboolean ret_rdma;

	struct fi_eq_cm_entry* event_entry;
	uint32_t event_entry_size;
	struct fi_eq_err_entry event_queue_err_entry;

	error = 0;
	event = 0;
	ret_msg = FALSE;
	ret_rdma = FALSE;
	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
	event_entry = malloc(event_entry_size);

	if (!j_endpoint_init(jendpoint,
			     jfabric,
			     domain_manager,
			     jd_configuration,
			     ((ThreadData*)thread_data)->connection.msg_event->info,
			     ((ThreadData*)thread_data)->connection.rdma_event->info,
			     "SERVER"))
	{
		goto end;
	}

	g_mutex_lock(thread_cq_array_mutex);
	g_ptr_array_add(thread_cq_array, j_endpoint_get_completion_queue(*jendpoint, J_MSG, FI_RECV));
	g_ptr_array_add(thread_cq_array, j_endpoint_get_completion_queue(*jendpoint, J_MSG, FI_TRANSMIT));

	g_ptr_array_add(thread_cq_array, j_endpoint_get_completion_queue(*jendpoint, J_RDMA, FI_RECV));
	g_ptr_array_add(thread_cq_array, j_endpoint_get_completion_queue(*jendpoint, J_RDMA, FI_TRANSMIT));
	g_mutex_unlock(thread_cq_array_mutex);

	// accept both endpoints
	error = fi_accept(j_endpoint_get_endpoint(*jendpoint, J_MSG), NULL, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while accepting connection request.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_accept(j_endpoint_get_endpoint(*jendpoint, J_RDMA), NULL, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while accepting connection request.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	event = 0;

	//read msg event queue for CONNECTED event

	switch (j_endpoint_read_event_queue(j_endpoint_get_event_queue(*jendpoint, J_MSG), &event, event_entry, event_entry_size, -1, &event_queue_err_entry, "SERVER", "msg CONNECTED"))
	{
		case J_READ_QUEUE_ERROR:
			goto end;
			break;
		case J_READ_QUEUE_SUCCESS:
			if (event == FI_CONNECTED)
			{
				// printf("\nSERVER: Connected event on eq\n"); //debug
				// fflush(stdout);
				j_endpoint_set_connected(*jendpoint, J_MSG);
				ret_msg = TRUE;
			}
			else
			{
				g_critical("\nSERVER: Problem with connection, no msg FI_CONNECTED.\n");
			}
			break;
		case J_READ_QUEUE_NO_EVENT:
			goto end; //case should not happen here
			break;
		case J_READ_QUEUE_CANCELED:
			goto end;
			break;
		default:
			g_assert_not_reached();
	}

	//read rdma event queue for CONNECTED event
	switch (j_endpoint_read_event_queue(j_endpoint_get_event_queue(*jendpoint, J_RDMA), &event, event_entry, event_entry_size, -1, &event_queue_err_entry, "SERVER", "rdma CONNECTED"))
	{
		case J_READ_QUEUE_ERROR:
			goto end;
			break;
		case J_READ_QUEUE_SUCCESS:
			if (event == FI_CONNECTED)
			{
				// printf("\nSERVER: Connected event on eq\n"); //debug
				// fflush(stdout);
				j_endpoint_set_connected(*jendpoint, J_RDMA);
				ret_rdma = TRUE;
			}
			else
			{
				g_critical("\nSERVER: Problem with connection, no rdma FI_CONNECTED.\n");
			}
			break;
		case J_READ_QUEUE_NO_EVENT:
			goto end; //case should not happen here
			break;
		case J_READ_QUEUE_CANCELED:
			goto end;
			break;
		default:
			g_assert_not_reached();
	}

end:
	j_thread_data_free((ThreadData*)thread_data);

	free(event_entry);
	return ret_msg && ret_rdma;
}

/**
* shuts down thread ressources
*/
void
j_thread_libfabric_ress_shutdown(JEndpoint* jendpoint)
{
	//end all fabric resources
	//close msg part
	//remove cq of ending thread from thread_cq_array
	g_mutex_lock(thread_cq_array_mutex);
	if (!g_ptr_array_remove_fast(thread_cq_array, j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_RECV)))
	{
		g_critical("\nSERVER: Removing msg cq_recv information from thread_cq_array did not work.\n");
	}
	if (!g_ptr_array_remove_fast(thread_cq_array, j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_TRANSMIT)))
	{
		g_critical("\nSERVER: Removing msg cq_transmit information from thread_cq_array did not work.\n");
	}
	if (!g_ptr_array_remove_fast(thread_cq_array, j_endpoint_get_completion_queue(jendpoint, J_RDMA, FI_RECV)))
	{
		g_critical("\nSERVER: Removing rdma cq_recv information from thread_cq_array did not work.\n");
	}
	if (!g_ptr_array_remove_fast(thread_cq_array, j_endpoint_get_completion_queue(jendpoint, J_RDMA, FI_TRANSMIT)))
	{
		g_critical("\nSERVER: Removing rdma cq_transmit information from thread_cq_array did not work.\n");
	}
	g_mutex_unlock(thread_cq_array_mutex);

	j_endpoint_fini(jendpoint, domain_manager, j_endpoint_shutdown_test(jendpoint, "SERVER"), FALSE, "SERVER");
}

/**
* Main function for each spawned endpoint-thread
*/
gpointer
j_thread_function(gpointer thread_data)
{
	//Needed Ressources
	int error = 0;
	uint32_t event = 0;

	struct fi_eq_cm_entry* event_entry;
	uint32_t event_entry_size;
	struct fi_eq_err_entry event_queue_err_entry;

	JEndpoint* jendpoint;

	J_TRACE_FUNCTION(NULL);

	JMemoryChunk* memory_chunk;
	g_autoptr(JMessage) message;
	JStatistics* statistics;
	guint64 memory_chunk_size;

	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;

	jfabric = ((ThreadData*)thread_data)->connection.jfabric;
	jd_configuration = ((ThreadData*)thread_data)->connection.configuration;
	domain_manager = ((ThreadData*)thread_data)->connection.domain_manager;
	j_thread_running = ((ThreadData*)thread_data)->julea_state.thread_running;
	j_server_running = ((ThreadData*)thread_data)->julea_state.server_running;
	thread_cq_array = ((ThreadData*)thread_data)->julea_state.thread_cq_array;
	thread_cq_array_mutex = ((ThreadData*)thread_data)->julea_state.thread_cq_array_mutex;
	thread_count = ((ThreadData*)thread_data)->julea_state.thread_count;
	j_statistics = ((ThreadData*)thread_data)->julea_state.j_statistics;
	j_statistics_mutex = ((ThreadData*)thread_data)->julea_state.j_statistics_mutex;

	message = NULL;

	statistics = j_statistics_new(TRUE);
	memory_chunk_size = j_configuration_get_max_operation_size(jd_configuration);
	memory_chunk = j_memory_chunk_new(memory_chunk_size);

	message = j_message_new(J_MESSAGE_NONE, 0);

	g_atomic_int_inc(thread_count);

	//g_printf("\nSERVER: Thread initiated\n"); //debug

	if (j_thread_libfabric_ress_init(thread_data, &jendpoint) == TRUE)
	{
		do
		{
			if (j_message_receive(message, jendpoint))
			{
				jd_handle_message(message, jendpoint, memory_chunk, memory_chunk_size, statistics);
			}

			{
				guint64 value;

				g_mutex_lock(j_statistics_mutex);

				value = j_statistics_get(statistics, J_STATISTICS_FILES_CREATED);
				j_statistics_add(j_statistics, J_STATISTICS_FILES_CREATED, value);
				value = j_statistics_get(statistics, J_STATISTICS_FILES_DELETED);
				j_statistics_add(j_statistics, J_STATISTICS_FILES_DELETED, value);
				value = j_statistics_get(statistics, J_STATISTICS_SYNC);
				j_statistics_add(j_statistics, J_STATISTICS_SYNC, value);
				value = j_statistics_get(statistics, J_STATISTICS_BYTES_READ);
				j_statistics_add(j_statistics, J_STATISTICS_BYTES_READ, value);
				value = j_statistics_get(statistics, J_STATISTICS_BYTES_WRITTEN);
				j_statistics_add(j_statistics, J_STATISTICS_BYTES_WRITTEN, value);
				value = j_statistics_get(statistics, J_STATISTICS_BYTES_RECEIVED);
				j_statistics_add(j_statistics, J_STATISTICS_BYTES_RECEIVED, value);
				value = j_statistics_get(statistics, J_STATISTICS_BYTES_SENT);
				j_statistics_add(j_statistics, J_STATISTICS_BYTES_SENT, value);

				g_mutex_unlock(j_statistics_mutex);
			}

			//Read event queue repeat loop if no shutdown sent
			event = 0;
			event_entry = malloc(event_entry_size);
			//TODO use j_endpoint_read_event_queue here
			error = fi_eq_read(j_endpoint_get_event_queue(jendpoint, J_MSG), &event, event_entry, event_entry_size, 0);
			if (error != 0)
			{
				if (error == -FI_EAVAIL)
				{
					error = fi_eq_readerr(j_endpoint_get_event_queue(jendpoint, J_MSG), &event_queue_err_entry, 0);
					if (error < 0)
					{
						g_critical("\nSERVER: Error occurred while reading Error Message from Event queue (active Endpoint) reading for shutdown.\nDetails:\n%s", fi_strerror(abs(error)));
						goto end;
					}
					else
					{
						g_critical("\nSERVER: Error Message, on Event queue (active Endpoint) reading for shutdown .\nDetails:\n%s", fi_eq_strerror(j_endpoint_get_event_queue(jendpoint, J_MSG), event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
						goto end;
					}
				}
				else if (error == -FI_EAGAIN)
				{
					//g_printf("\nNo Event data on event Queue on Server while reading for SHUTDOWN Event.\n");
				}
				else if (error < 0)
				{
					g_critical("\nSERVER: Error while reading completion queue after receiving Message (JMessage Header).\nDetails:\n%s", fi_strerror(abs(error)));
					goto end;
				}
			}
			free(event_entry);
		} while (!(event == FI_SHUTDOWN || (*j_thread_running) == FALSE));
	}

end:

	j_memory_chunk_free(memory_chunk);
	j_statistics_free(statistics);

	j_thread_libfabric_ress_shutdown(jendpoint);

	g_atomic_int_dec_and_test(thread_count);
	if (g_atomic_int_compare_and_exchange(thread_count, 0, 0) && !(*j_thread_running))
	{
		*j_server_running = FALSE; //set server running to false, thus ending server main loop if shutting down
		g_printf("\nSERVER: Last Thread finished\n");
	}

	g_thread_exit(NULL);
	return NULL;
}

ThreadData*
j_thread_data_new(JConfiguration* configuration_in,
		  JFabric* jfabric_in,
		  JDomainManager* domain_manager_in,
		  gchar* uuid_in,
		  GPtrArray* thread_cq_array_in,
		  GMutex* thread_cq_array_mutex_in,
		  volatile gboolean* server_running_in,
		  volatile gboolean* thread_running_in,
		  volatile gint* thread_count_in,
		  JStatistics* j_statistics_in,
		  GMutex* j_statistics_mutex_in)
{
	ThreadData* thread_data;

	thread_data = malloc(sizeof(ThreadData));

	thread_data->connection.rdma_event = NULL;
	thread_data->connection.msg_event = NULL;
	thread_data->connection.jfabric = jfabric_in;
	thread_data->connection.configuration = configuration_in;
	thread_data->connection.domain_manager = domain_manager_in;
	thread_data->connection.uuid = g_strdup(uuid_in);
	thread_data->julea_state.thread_cq_array = thread_cq_array_in;
	thread_data->julea_state.thread_cq_array_mutex = thread_cq_array_mutex_in;
	thread_data->julea_state.server_running = server_running_in;
	thread_data->julea_state.thread_running = thread_running_in;
	thread_data->julea_state.thread_count = thread_count_in;
	thread_data->julea_state.j_statistics = j_statistics_in;
	thread_data->julea_state.j_statistics_mutex = j_statistics_mutex_in;

	return thread_data;
}

void
j_thread_data_free(ThreadData* thread_data)
{
	g_free(thread_data->connection.uuid);
	if (thread_data->connection.msg_event != NULL)
	{
		fi_freeinfo(thread_data->connection.msg_event->info);
		free(thread_data->connection.msg_event);
	}
	if (thread_data->connection.rdma_event != NULL)
	{
		fi_freeinfo(thread_data->connection.rdma_event->info);
		free(thread_data->connection.rdma_event);
	}
	free(thread_data);
}

void
j_thread_data_set_rdma_event(ThreadData* thread_data, struct fi_eq_cm_entry* event_entry)
{
	thread_data->connection.rdma_event = event_entry;
}

void
j_thread_data_set_msg_event(ThreadData* thread_data, struct fi_eq_cm_entry* event_entry)
{
	thread_data->connection.msg_event = event_entry;
}

gboolean
j_thread_data_check_completion(ThreadData* thread_data)
{
	return thread_data->connection.rdma_event != NULL && thread_data->connection.msg_event != NULL;
}

struct fi_eq_cm_entry*
j_thread_data_get_msg_event(ThreadData* thread_data)
{
	return thread_data->connection.msg_event;
}

struct fi_eq_cm_entry*
j_thread_data_get_rdma_event(ThreadData* thread_data)
{
	return thread_data->connection.rdma_event;
}

gchar*
j_thread_data_get_uuid(ThreadData* thread_data)
{
	return thread_data->connection.uuid;
}
