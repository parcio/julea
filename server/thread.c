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

static struct fid_fabric* j_fabric;

static volatile gboolean* j_thread_running;
static volatile gboolean* j_server_running;

static GPtrArray* thread_cq_array;
static GMutex* thread_cq_array_mutex;

static volatile gint* thread_count;

static JConfiguration* jd_configuration;

static DomainManager* domain_manager;

/**
* inits ressources for endpoint-threads
*/
gboolean
j_thread_libfabric_ress_init(gpointer thread_data,
			     RefCountedDomain** rc_domain,
			     JEndpoint** jendpoint)
{
	int error;
	uint32_t event;
	gboolean ret;

	struct fi_eq_cm_entry* event_entry;
	uint32_t event_entry_size;
	struct fi_eq_err_entry event_queue_err_entry;

	struct fi_eq_attr event_queue_attr = *j_configuration_get_fi_eq_attr(jd_configuration);
	struct fi_cq_attr completion_queue_attr = *j_configuration_get_fi_cq_attr(jd_configuration);

	error = 0;
	event = 0;
	ret = FALSE;

	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
	(*jendpoint) = malloc(sizeof(struct JEndpoint));
	(*jendpoint)->info_msg = fi_dupinfo(((ThreadData*)thread_data)->event_entry->info);

	// got everything out of the connection request we need, so ressources can be freed;
	fi_freeinfo(((ThreadData*)thread_data)->event_entry->info);
	free(((ThreadData*)thread_data)->event_entry);
	free(thread_data);

	if (domain_request(j_fabric, (*jendpoint)->info_msg, jd_configuration, rc_domain, domain_manager) != TRUE)
	{
		goto end;
	}

	//bulding endpoint
	error = fi_endpoint((*rc_domain)->domain, (*jendpoint)->info_msg, &(*jendpoint)->ep_msg, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_eq_open(j_fabric, &event_queue_attr, &(*jendpoint)->eq_msg, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating endpoint Event queue.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_ep_bind((*jendpoint)->ep_msg, &(*jendpoint)->eq_msg->fid, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Event queue to active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open((*rc_domain)->domain, &completion_queue_attr, &(*jendpoint)->cq_receive_msg, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating Completion receive queue for active Endpoint.\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open((*rc_domain)->domain, &completion_queue_attr, &(*jendpoint)->cq_transmit_msg, NULL);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while creating Completion transmit queue for active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->ep_msg, &(*jendpoint)->cq_transmit_msg->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Completion transmit queue to active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->ep_msg, &(*jendpoint)->cq_receive_msg->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while binding Completion receive queue to active Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	g_mutex_lock(thread_cq_array_mutex);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->cq_transmit_msg);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->cq_receive_msg);
	g_mutex_unlock(thread_cq_array_mutex);

	// PERROR no receive Buffer before accepting connection. (should not happen due to the inner workings of Julea)

	error = fi_enable((*jendpoint)->ep_msg);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while enabling Endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_accept((*jendpoint)->ep_msg, NULL, 0);
	if (error != 0)
	{
		g_critical("\nSERVER: Error occurred while accepting connection request.\n Details:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	event_entry = malloc(event_entry_size);
	error = fi_eq_sread((*jendpoint)->eq_msg, &event, event_entry, event_entry_size, -1, 0);
	if (error < 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr((*jendpoint)->eq_msg, &event_queue_err_entry, 0);
			if (error < 0)
			{
				g_critical("\nSERVER: Error occurred while reading Error Message from Event queue (active Endpoint) reading for connection accept.\nDetails:\n%s", fi_strerror(abs(error)));
				error = 0;
				goto end;
			}
			else
			{
				g_critical("\nSERVER: Error Message on Event queue (active Endpoint) reading for connection accept available .\nDetails:\n%s", fi_eq_strerror((*jendpoint)->eq_msg, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nSERVER: No Event data on Event Queue while reading for CONNECTED Event.\n");
			goto end;
		}
		else
		{
			g_critical("\nSERVER: Error outside Error Event on Event Queue while reading for CONNECTED Event.\nDetails:\n%s", fi_strerror(abs(error)));
			goto end;
		}
	}
	free(event_entry);

	if (event == FI_CONNECTED)
	{
		// printf("\nSERVER: Connected event on eq\n"); //debug
		// fflush(stdout);
		ret = TRUE;
	}
	else
	{
		g_critical("\nSERVER: Problem with connection, no FI_CONNECTED event.\n");
	}
end:
	return ret;
}

/**
* shuts down thread ressources
*/
void
j_thread_libfabric_ress_shutdown(RefCountedDomain* rc_domain, JEndpoint* jendpoint)
{
	int error;
	gboolean berror;

	error = 0;
	berror = FALSE;

	//if shutdown initiated by server initiate a shutdown event on peer
	if ((*j_thread_running) == FALSE)
	{
		error = fi_shutdown(jendpoint->ep_msg, 0);
		if (error != 0)
		{
			g_critical("\nSERVER: Error while shutting down connection\n.Details: \n%s", fi_strerror(abs(error)));
		}
		error = 0;
	}

	//end all fabric resources
	error = 0;

	//remove cq of ending thread from thread_cq_array
	g_mutex_lock(thread_cq_array_mutex);
	berror = g_ptr_array_remove_fast(thread_cq_array, jendpoint->cq_receive_msg);
	if (berror == FALSE)
	{
		g_critical("\nSERVER: Removing cq_recv information from thread_cq_array did not work.\n");
	}
	berror = g_ptr_array_remove_fast(thread_cq_array, jendpoint->cq_transmit_msg);
	if (berror == FALSE)
	{
		g_critical("\nSERVER: Removing cq_transmit information from thread_cq_array did not work.\n");
	}
	g_mutex_unlock(thread_cq_array_mutex);

	error = fi_close(&jendpoint->ep_msg->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread endpoint.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->cq_receive_msg->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread receive queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->cq_transmit_msg->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error closing thread transmition queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->eq_msg->fid);
	if (error != 0)
	{
		g_critical("\nSERVER: Error thread endpoint event queue.\n Details:\n %s", fi_strerror(abs(error)));
		error = 0;
  }

	domain_unref(rc_domain, domain_manager, "server thread");

	fi_freeinfo(jendpoint->info_msg);

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

	RefCountedDomain* rc_domain;

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

	j_fabric = ((ThreadData*)thread_data)->fabric;
	j_thread_running = ((ThreadData*)thread_data)->thread_running;
	j_server_running = ((ThreadData*)thread_data)->server_running;
	thread_cq_array = ((ThreadData*)thread_data)->thread_cq_array;
	thread_cq_array_mutex = ((ThreadData*)thread_data)->thread_cq_array_mutex;
	thread_count = ((ThreadData*)thread_data)->thread_count;
	jd_configuration = ((ThreadData*)thread_data)->j_configuration;
	j_statistics = ((ThreadData*)thread_data)->j_statistics;
	j_statistics_mutex = ((ThreadData*)thread_data)->j_statistics_mutex;
	domain_manager = ((ThreadData*)thread_data)->domain_manager;

	message = NULL;

	statistics = j_statistics_new(TRUE);
	memory_chunk_size = j_configuration_get_max_operation_size(jd_configuration);
	memory_chunk = j_memory_chunk_new(memory_chunk_size);

	message = j_message_new(J_MESSAGE_NONE, 0);

	g_atomic_int_inc(thread_count);

	//g_printf("\nSERVER: Thread initiated\n"); //debug

	if (j_thread_libfabric_ress_init(thread_data, &rc_domain, &jendpoint) == TRUE)
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
			error = fi_eq_read(jendpoint->eq_msg, &event, event_entry, event_entry_size, 0);
			if (error != 0)
			{
				if (error == -FI_EAVAIL)
				{
					error = fi_eq_readerr(jendpoint->eq_msg, &event_queue_err_entry, 0);
					if (error < 0)
					{
						g_critical("\nSERVER: Error occurred while reading Error Message from Event queue (active Endpoint) reading for shutdown.\nDetails:\n%s", fi_strerror(abs(error)));
						goto end;
					}
					else
					{
						g_critical("\nSERVER: Error Message, on Event queue (active Endpoint) reading for shutdown .\nDetails:\n%s", fi_eq_strerror(jendpoint->eq_msg, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
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

	j_thread_libfabric_ress_shutdown(rc_domain, jendpoint);

	g_atomic_int_dec_and_test(thread_count);
	if (g_atomic_int_compare_and_exchange(thread_count, 0, 0) && !(*j_thread_running))
	{
		*j_server_running = FALSE; //set server running to false, thus ending server main loop if shutting down
		g_printf("\nSERVER: Last Thread finished\n");
	}

	g_thread_exit(NULL);
	return NULL;
}

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
