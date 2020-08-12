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

static JFabric* jfabric;

static volatile gboolean* j_thread_running;
static volatile gboolean* j_server_running;

static GPtrArray* thread_cq_array;
static GMutex* thread_cq_array_mutex;

static volatile gint* thread_count;

static JConfiguration* jd_configuration;

static DomainManager* domain_manager;

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

	struct fi_eq_attr event_queue_attr = *j_configuration_get_fi_eq_attr(jd_configuration);
	struct fi_cq_attr completion_queue_attr = *j_configuration_get_fi_cq_attr(jd_configuration);

	error = 0;
	event = 0;
	ret_msg = FALSE;
	ret_rdma = FALSE;
	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
	event_entry = malloc(event_entry_size);

	(*jendpoint) = malloc(sizeof(struct JEndpoint));
	(*jendpoint)->msg.info = fi_dupinfo(((ThreadData*)thread_data)->connection.msg_event->info);
	(*jendpoint)->rdma.info = fi_dupinfo(((ThreadData*)thread_data)->connection.rdma_event->info);
	(*jendpoint)->msg.is_connected = FALSE;
	(*jendpoint)->rdma.is_connected = FALSE;

	// got everything out of the connection request we need, so ressources can be freed;
	g_free(((ThreadData*)thread_data)->connection.uuid);
	fi_freeinfo(((ThreadData*)thread_data)->connection.msg_event->info);
	fi_freeinfo(((ThreadData*)thread_data)->connection.rdma_event->info);
	free(((ThreadData*)thread_data)->connection.msg_event);
	free(((ThreadData*)thread_data)->connection.rdma_event);
	free((ThreadData*)thread_data);

	//bulding endpoint
	//msg
	if (!domain_request(jfabric->msg_fabric, (*jendpoint)->msg.info, jd_configuration, &(*jendpoint)->msg.rc_domain, domain_manager, "Server Thread msg"))
	{
		g_critical("\nSERVER: Error occurred while requesting msg domain on server.\n");
		goto end;
	}

	error = fi_endpoint((*jendpoint)->msg.rc_domain->domain, (*jendpoint)->msg.info, &(*jendpoint)->msg.ep, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating active msg Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_eq_open(jfabric->msg_fabric, &event_queue_attr, &(*jendpoint)->msg.eq, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating msg endpoint Event queue.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_ep_bind((*jendpoint)->msg.ep, &(*jendpoint)->msg.eq->fid, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Event queue to msg active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open((*jendpoint)->msg.rc_domain->domain, &completion_queue_attr, &(*jendpoint)->msg.cq_receive, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating Completion receive queue for msg active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open((*jendpoint)->msg.rc_domain->domain, &completion_queue_attr, &(*jendpoint)->msg.cq_transmit, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating Completion transmit queue for msg active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->msg.ep, &(*jendpoint)->msg.cq_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Completion transmit queue to msg active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->msg.ep, &(*jendpoint)->msg.cq_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Completion receive queue to msg active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	g_mutex_lock(thread_cq_array_mutex);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->msg.cq_transmit);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->msg.cq_receive);
	g_mutex_unlock(thread_cq_array_mutex);

	// PERROR no receive Buffer before accepting connection. (should not happen due to the inner workings of Julea)

	error = fi_enable((*jendpoint)->msg.ep);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while enabling msg Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	//rdma
	if (!domain_request(jfabric->rdma_fabric, (*jendpoint)->rdma.info, jd_configuration, &(*jendpoint)->rdma.rc_domain, domain_manager, "Server Thread rdma"))
	{
		g_critical("\nSERVER: Error occurred while requesting rdma domain on server.\n");
		goto end;
	}

	error = fi_endpoint((*jendpoint)->rdma.rc_domain->domain, (*jendpoint)->rdma.info, &(*jendpoint)->rdma.ep, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating rdma active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_eq_open(jfabric->rdma_fabric, &event_queue_attr, &(*jendpoint)->rdma.eq, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating rdma endpoint Event queue.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_ep_bind((*jendpoint)->rdma.ep, &(*jendpoint)->rdma.eq->fid, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Event queue to rdma active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open((*jendpoint)->rdma.rc_domain->domain, &completion_queue_attr, &(*jendpoint)->rdma.cq_receive, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating Completion receive queue for rdma active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open((*jendpoint)->rdma.rc_domain->domain, &completion_queue_attr, &(*jendpoint)->rdma.cq_transmit, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating Completion transmit queue for rdma active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->rdma.ep, &(*jendpoint)->rdma.cq_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Completion transmit queue to rdma active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->rdma.ep, &(*jendpoint)->rdma.cq_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Completion receive queue to rdma active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	g_mutex_lock(thread_cq_array_mutex);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->rdma.cq_transmit);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->rdma.cq_receive);
	g_mutex_unlock(thread_cq_array_mutex);

	// PERROR no receive Buffer before accepting connection. (should not happen due to the inner workings of Julea)

	error = fi_enable((*jendpoint)->rdma.ep);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while enabling rdma Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	// accept both endpoints
	error = fi_accept((*jendpoint)->msg.ep, NULL, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while accepting connection request.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_accept((*jendpoint)->rdma.ep, NULL, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while accepting connection request.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	event = 0;

	//read msg event queue for CONNECTED event
	error = fi_eq_sread((*jendpoint)->msg.eq, &event, event_entry, event_entry_size, -1, 0);
	if (error < 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr((*jendpoint)->msg.eq, &event_queue_err_entry, 0);
			if (error < 0)
			{
				g_critical("\nSERVER: Error occurred while reading Error Message from msg Event queue (active Endpoint) reading for connection accept.\nDetails:\n%s", fi_strerror(abs(error)));
				error = 0;
				goto end;
			}
			else
			{
				g_critical("\nSERVER: Error Message on msg Event queue (active Endpoint) reading for connection accept available .\nDetails:\n%s", fi_eq_strerror((*jendpoint)->msg.eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nSERVER: No Event data on msg Event Queue while reading for CONNECTED Event.\n");
			goto end;
		}
		else
		{
			g_critical("\nSERVER: Error outside Error msg Event on Event Queue while reading for CONNECTED Event.\nDetails:\n%s", fi_strerror(abs(error)));
			goto end;
		}
	}

	if (event == FI_CONNECTED)
	{
		// printf("\nSERVER: Connected event on eq\n"); //debug
		// fflush(stdout);
		(*jendpoint)->msg.is_connected = TRUE;
		ret_msg = TRUE;
	}
	else
	{
		g_critical("\nSERVER: Problem with connection, no msg FI_CONNECTED.\n");
	}

	//read rdma event queue for CONNECTED event
	error = fi_eq_sread((*jendpoint)->rdma.eq, &event, event_entry, event_entry_size, -1, 0);
	if (error < 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr((*jendpoint)->rdma.eq, &event_queue_err_entry, 0);
			if (error < 0)
			{
				g_critical("\nSERVER: Error occurred while reading rdma Error Message from Event queue (active Endpoint) reading for connection accept.\nDetails:\n%s", fi_strerror(abs(error)));
				error = 0;
				goto end;
			}
			else
			{
				g_critical("\nSERVER: Error Message on rdma Event queue (active Endpoint) reading for connection accept available .\nDetails:\n%s", fi_eq_strerror((*jendpoint)->msg.eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nSERVER: No Event data on rdma Event Queue while reading for CONNECTED Event.\n");
			goto end;
		}
		else
		{
			g_critical("\nSERVER: Error outside rdma Error Event on Event Queue while reading for CONNECTED Event.\nDetails:\n%s", fi_strerror(abs(error)));
			goto end;
		}
	}

	if (event == FI_CONNECTED)
	{
		// printf("\nSERVER: Connected event on eq\n"); //debug
		// fflush(stdout);
		(*jendpoint)->rdma.is_connected = TRUE;
		ret_rdma = TRUE;
	}
	else
	{
		g_critical("\nSERVER: Problem with connection, no rdma FI_CONNECTED.\n");
	}

end:
	free(event_entry);
	return ret_msg && ret_rdma;
}

/**
* shuts down thread ressources
*/
void
j_thread_libfabric_ress_shutdown(JEndpoint* jendpoint)
{
	int error;

	error = 0;

	//if shutdown initiated by server initiate a shutdown event on peer
	if ((*j_thread_running) == FALSE)
	{
		error = fi_shutdown(jendpoint->msg.ep, 0);
		if (error != 0)
		{
			g_critical("\nSERVER: Error while shutting down msg connection\n.Details: \n%s", fi_strerror(abs(error)));
		}
		error = fi_shutdown(jendpoint->rdma.ep, 0);
		if (error != 0)
		{
			g_critical("\nSERVER: Error while shutting down rdma connection\n.Details: \n%s", fi_strerror(abs(error)));
		}
	}

	//end all fabric resources
	//close msg part
	//remove cq of ending thread from thread_cq_array
	g_mutex_lock(thread_cq_array_mutex);
	if (!g_ptr_array_remove_fast(thread_cq_array, jendpoint->msg.cq_receive))
	{
		g_critical("\nSERVER: Removing msg cq_recv information from thread_cq_array did not work.\n");
	}
	if (!g_ptr_array_remove_fast(thread_cq_array, jendpoint->msg.cq_transmit))
	{
		g_critical("\nSERVER: Removing msg cq_transmit information from thread_cq_array did not work.\n");
	}
	g_mutex_unlock(thread_cq_array_mutex);

	error = fi_close(&jendpoint->msg.ep->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread msg endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.cq_receive->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread msg receive queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.cq_transmit->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread msg transmition queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.eq->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error thread msg endpoint event queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}
	domain_unref(jendpoint->msg.rc_domain, domain_manager, "server thread msg");
	fi_freeinfo(jendpoint->msg.info);

	// close rdma part
	g_mutex_lock(thread_cq_array_mutex);
	if (!g_ptr_array_remove_fast(thread_cq_array, jendpoint->rdma.cq_receive))
	{
		g_critical("\nSERVER: Removing rdma cq_recv information from thread_cq_array did not work.\n");
	}
	if (!g_ptr_array_remove_fast(thread_cq_array, jendpoint->rdma.cq_transmit))
	{
		g_critical("\nSERVER: Removing rdma cq_transmit information from thread_cq_array did not work.\n");
	}
	g_mutex_unlock(thread_cq_array_mutex);

	error = fi_close(&jendpoint->rdma.ep->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread rdma endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.cq_receive->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread rdma receive queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.cq_transmit->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread rdma transmition queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.eq->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error thread rdma endpoint event queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}
	domain_unref(jendpoint->rdma.rc_domain, domain_manager, "server thread rdma");
	fi_freeinfo(jendpoint->rdma.info);

	free(jendpoint);
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

	jfabric = ((ThreadData*)thread_data)->connection.fabric;
	jd_configuration = ((ThreadData*)thread_data)->connection.j_configuration;
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

	if (j_message_get_comm_type(message) == J_MSG)
	{
		printf("\nSERVER: J_MSG Type spotted\n"); //debug
	}

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
			error = fi_eq_read(jendpoint->msg.eq, &event, event_entry, event_entry_size, 0);
			if (error != 0)
			{
				if (error == -FI_EAVAIL)
				{
					error = fi_eq_readerr(jendpoint->msg.eq, &event_queue_err_entry, 0);
					if (error < 0)
					{
						g_critical("\nSERVER: Error occurred while reading Error Message from Event queue (active Endpoint) reading for shutdown.\nDetails:\n%s", fi_strerror(abs(error)));
						goto end;
					}
					else
					{
						g_critical("\nSERVER: Error Message, on Event queue (active Endpoint) reading for shutdown .\nDetails:\n%s", fi_eq_strerror(jendpoint->msg.eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
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
