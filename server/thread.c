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
j_thread_libfabric_ress_init(struct fi_info* info,
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
	(*jendpoint)->max_msg_size = info->ep_attr->max_msg_size;

	if (domain_request(j_fabric, info, jd_configuration, rc_domain, domain_manager) != TRUE)
	{
		goto end;
	}

	//bulding endpoint
	error = fi_endpoint((*rc_domain)->domain, info, &(*jendpoint)->endpoint, NULL);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while creating active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	error = fi_eq_open(j_fabric, &event_queue_attr, &(*jendpoint)->event_queue, NULL);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while creating endpoint Event queue.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	error = fi_ep_bind((*jendpoint)->endpoint, &(*jendpoint)->event_queue->fid, 0);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while binding Event queue to active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_cq_open((*rc_domain)->domain, &completion_queue_attr, &(*jendpoint)->completion_queue_receive, NULL);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while creating Completion receive queue for active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_cq_open((*rc_domain)->domain, &completion_queue_attr, &(*jendpoint)->completion_queue_transmit, NULL);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while creating Completion transmit queue for active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->endpoint, &(*jendpoint)->completion_queue_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while binding Completion transmit queue to active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->endpoint, &(*jendpoint)->completion_queue_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while binding Completion receive queue to active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	g_mutex_lock(thread_cq_array_mutex);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->completion_queue_receive);
	g_ptr_array_add(thread_cq_array, (*jendpoint)->completion_queue_transmit);
	g_mutex_unlock(thread_cq_array_mutex);

	//PERROR no receive Buffer before accepting connetcion.

	error = fi_enable((*jendpoint)->endpoint);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while enabling Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

	error = fi_accept((*jendpoint)->endpoint, NULL, 0);
	if (error != 0)
	{
		g_critical("\nError occurred on Server while accepting connection request.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	else
	{
		g_printf("\nSERVER: accepted connection\n");
	}

	event_entry = malloc(event_entry_size);
	error = fi_eq_sread((*jendpoint)->event_queue, &event, event_entry, event_entry_size, -1, 0);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr((*jendpoint)->event_queue, &event_queue_err_entry, 0);
			if (error != 0)
			{
				g_critical("\nError occurred on Server while reading Error Message from Event queue (active Endpoint) reading for connection accept.\nDetails:\n%s", fi_strerror(error));
				error = 0;
				goto end;
			}
			else
			{
				g_critical("\nError Message on Server, on Event queue (active Endpoint) reading for connection accept available .\nDetails:\n%s", fi_eq_strerror((*jendpoint)->event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nNo Event data on Server Event Queue while reading for CONNECTED Event.\n");
			goto end;
		}
		else if (error < 0)
		{
			g_critical("\nError outside Error Event on Server Event Queue while reading for CONNECTED Event.\nDetails:\n%s", fi_strerror(error));
			goto end;
		}
	}
	free(event_entry);

	if (event == FI_CONNECTED)
	{
		ret = TRUE;
	}
	else
	{
		g_printf("\nServer has problems with connection, no FI_CONNECTED event.\n");
	}
end:
	return ret;
}

/**
* shuts down thread ressources
*/
void
j_thread_libfabric_ress_shutdown(struct fi_info* info,
				 RefCountedDomain* rc_domain,
				 JEndpoint* jendpoint)
{
	int error;
	gboolean berror;

	error = 0;
	berror = FALSE;

	//if shutdown initiated by server initiate a shutdown event on peer
	if ((*j_thread_running) == FALSE)
	{
		error = fi_shutdown(jendpoint->endpoint, 0);
		if (error != 0)
		{
			g_critical("\nError on Server while shutting down connection\n.Details: \n%s", fi_strerror(error));
		}
		error = 0;
	}

	//end all fabric resources
	error = 0;

	fi_freeinfo(info);

	//remove cq of ending thread from thread_cq_array
	g_mutex_lock(thread_cq_array_mutex);
	berror = g_ptr_array_remove_fast(thread_cq_array, jendpoint->completion_queue_receive);
	if (berror == FALSE)
	{
		g_critical("\nRemoving cq_recv information from thread_cq_array did not work.\n");
	}
	berror = g_ptr_array_remove_fast(thread_cq_array, jendpoint->completion_queue_transmit);
	if (berror == FALSE)
	{
		g_critical("\nRemoving cq_transmit information from thread_cq_array did not work.\n");
	}
	g_mutex_unlock(thread_cq_array_mutex);

	error = fi_close(&jendpoint->endpoint->fid);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread endpoint.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&jendpoint->completion_queue_receive->fid);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread receive queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&jendpoint->completion_queue_transmit->fid);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread transmition queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&jendpoint->event_queue->fid);
	if (error != 0)
	{
		g_critical("\nSomething went horribly wrong closing thread endpoint event queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	domain_unref(rc_domain, domain_manager, "server thread");

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

	struct fi_info* info;
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

	info = fi_dupinfo(((ThreadData*)thread_data)->event_entry->info);
	fi_freeinfo(((ThreadData*)thread_data)->event_entry->info);
	free(((ThreadData*)thread_data)->event_entry);
	free(thread_data);

	message = NULL;

	statistics = j_statistics_new(TRUE);
	memory_chunk_size = j_configuration_get_max_operation_size(jd_configuration);
	memory_chunk = j_memory_chunk_new(memory_chunk_size);

	message = j_message_new(J_MESSAGE_NONE, 0);

	g_atomic_int_inc(thread_count);

	g_printf("\nSERVER: Thread initiated\n");

	if (j_thread_libfabric_ress_init(info, &rc_domain, &jendpoint) == TRUE)
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
			error = fi_eq_read(jendpoint->event_queue, &event, event_entry, event_entry_size, 0);
			if (error != 0)
			{
				if (error == -FI_EAVAIL)
				{
					error = fi_eq_readerr(jendpoint->event_queue, &event_queue_err_entry, 0);
					if (error != 0)
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
				else if (error == -FI_EAGAIN)
				{
					//g_printf("\nNo Event data on event Queue on Server while reading for SHUTDOWN Event.\n");
				}
				else if (error < 0)
				{
					g_critical("\nError while reading completion queue after receiving Message (JMessage Header).\nDetails:\n%s", fi_strerror(error));
					goto end;
				}
			}
			free(event_entry);
		} while (!(event == FI_SHUTDOWN || (*j_thread_running) == FALSE));
	}

end:

	j_memory_chunk_free(memory_chunk);
	j_statistics_free(statistics);

	j_thread_libfabric_ress_shutdown(info, rc_domain, jendpoint);

	g_atomic_int_dec_and_test(thread_count);
	if (g_atomic_int_compare_and_exchange(thread_count, 0, 0) && !(*j_thread_running))
	{
		*j_server_running = FALSE; //set server running to false, thus ending server main loop if shutting down
		g_printf("\nLast Thread finished\n");
	}

	g_thread_exit(NULL);
	return NULL;
}

void
//TODO replace workaround
//thread_unblock(gpointer completion_queue, gpointer user_data)
thread_unblock(struct fid_cq* completion_queue)
{
	int error = 0;
	//error = fi_cq_signal((struct fid_cq*) completion_queue);
	error = fi_cq_signal(completion_queue);
	if (error != 0)
	{
		g_critical("\nError waking up Threads\nDetails:\n %s", fi_strerror(error));
	}
}
