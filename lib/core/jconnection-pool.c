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

/**
 * \file
 **/

#define _DEFAULT_SOURCE

#include <julea-config.h>

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include <jconnection-pool.h>
#include <jconnection-pool-internal.h>

#include <jbackend.h>
#include <jhelper.h>
#include <jhelper-internal.h>
#include <jmessage.h>
#include <jtrace.h>

//libfabric interfaces for used Objects
#include <rdma/fabric.h>
#include <rdma/fi_domain.h> //includes cqs and
#include <rdma/fi_cm.h> //connection management
#include <rdma/fi_errno.h> //translation error numbers

#include <glib/gprintf.h>

#include <j_fi_domain_manager.h>

/**
 * \defgroup JConnectionPool Connection Pool
 *
 * Data structures and functions for managing connection pools.
 *
 * @{
 **/

struct JConnectionPoolQueue
{
	GAsyncQueue* queue;
	guint count;
};

typedef struct JConnectionPoolQueue JConnectionPoolQueue;

/**
 * A connection.
 **/
struct JConnectionPool
{
	JConfiguration* configuration;
	JConnectionPoolQueue* object_queues;
	JConnectionPoolQueue* kv_queues;
	JConnectionPoolQueue* db_queues;
	guint object_len;
	guint kv_len;
	guint db_len;
	guint max_count;
};

typedef struct JConnectionPool JConnectionPool;

static JConnectionPool* j_connection_pool = NULL;

//libfabric high level objects
static struct fid_fabric* j_fabric;

//libfabric config structures
static struct fi_info* j_info;

static DomainManager* domain_manager;

void
j_connection_pool_init(JConfiguration* configuration)
{
	// Pool Init
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

	//Parameter for fabric init
	int error;

	g_return_if_fail(j_connection_pool == NULL);

	pool = g_slice_new(JConnectionPool);
	pool->configuration = j_configuration_ref(configuration);
	pool->object_len = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT);
	pool->object_queues = g_new(JConnectionPoolQueue, pool->object_len);
	pool->kv_len = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_KV);
	pool->kv_queues = g_new(JConnectionPoolQueue, pool->kv_len);
	pool->db_len = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_DB);
	pool->db_queues = g_new(JConnectionPoolQueue, pool->db_len);
	pool->max_count = j_configuration_get_max_connections(configuration);

	for (guint i = 0; i < pool->object_len; i++)
	{
		pool->object_queues[i].queue = g_async_queue_new();
		pool->object_queues[i].count = 0;
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		pool->kv_queues[i].queue = g_async_queue_new();
		pool->kv_queues[i].count = 0;
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		pool->db_queues[i].queue = g_async_queue_new();
		pool->db_queues[i].count = 0;
	}

	g_atomic_pointer_set(&j_connection_pool, pool);

	//Init Libfabric Objects
	error = 0;

	//inits j_info
	error = fi_getinfo(j_configuration_get_fi_version(configuration),
			   j_configuration_get_fi_node(configuration),
			   j_configuration_get_fi_service(configuration),
			   j_configuration_get_fi_flags(configuration, 0),
			   j_configuration_fi_get_hints(configuration),
			   &j_info);

	if (error != 0)
	{
		goto end;
	}
	if (j_info == NULL)
	{
		g_critical("\nAllocating Client j_info did not work");
	}

	//init fabric
	error = fi_fabric(j_info->fabric_attr, &j_fabric, NULL);
	if (error != FI_SUCCESS)
	{
		g_critical("\nAllocating fabric on client side did not work\nDetails:\n %s", fi_strerror(error));
		goto end;
	}

	domain_manager = domain_manager_init();

end:;
}

/**
* Checks whether endpoint has a shutdown message on event queue
*
*/
gboolean
j_endpoint_shutdown_test(JEndpoint* endpoint, const gchar* location)
{
	int error;
	gboolean ret;

	uint32_t event;
	struct fi_eq_cm_entry* event_entry;
	struct fi_eq_err_entry event_queue_err_entry;

	ret = FALSE;
	error = 0;

	event_entry = malloc(sizeof(struct fi_eq_cm_entry) + 128);
	error = fi_eq_read(endpoint->event_queue, &event, event_entry, sizeof(struct fi_eq_cm_entry) + 128, 0);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr(endpoint->event_queue, &event_queue_err_entry, 0);
			if (error < 0)
			{
				g_critical("\nError occurred on Client while reading Error Message from Event queue (%s) reading for shutdown.\nDetails:\n%s", fi_strerror(error), location);
			}
			else
			{
				g_critical("\nError Message on Client, on Event queue (%s) reading for shutdown .\nDetails:\n%s", location, fi_eq_strerror(endpoint->event_queue, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
			}
		}
		else if (error == -FI_EAGAIN)
		{
			ret = FALSE;
		}
		else if (error < 0)
		{
			g_critical("\nError while reading event queue (%s) after reading for shutdown.\nDetails:\n%s", fi_strerror(error), location);
		}
	}
	free(event_entry);
	if (event == FI_SHUTDOWN)
	{
		ret = TRUE;
	}
	return ret;
}

/**
* closes the JEndpoint given and all associated objects
*
*/
void
j_endpoint_fini(JEndpoint* endpoint, JMessage* message, gboolean send_shutdown_message, const gchar* location)
{
	int error = 0;

	//empty wakeup message for server Thread shutdown
	if (send_shutdown_message == TRUE)
	{
		gboolean berror;
		berror = j_message_send(message, endpoint);
		if (berror == FALSE)
		{
			g_critical("\nSending wakeup message while connection pool (%s queues) shutdown did not work.\n", location);
		}
		error = fi_shutdown(endpoint->endpoint, 0);
		if (error != 0)
		{
			g_critical("\nSomething went horribly wrong during %s connection shutdown.\n Details:\n %s", location, fi_strerror(error));
			error = 0;
		}
	}

	error = fi_close(&endpoint->endpoint->fid);
	if (error != 0)
	{
		g_critical("\nProblem closing %s-Endpoint.\n Details:\n %s", location, fi_strerror(error));
		error = 0;
	}

	error = fi_close(&endpoint->completion_queue_receive->fid);
	if (error != 0)
	{
		g_critical("\nProblem closing %s-Endpoint receive completion queue.\n Details:\n %s", location, fi_strerror(error));
		error = 0;
	}

	error = fi_close(&endpoint->completion_queue_transmit->fid);
	if (error != 0)
	{
		g_critical("\nProblem closing %s-Endpoint transmit completion queue.\n Details:\n %s", location, fi_strerror(error));
		error = 0;
	}

	error = fi_close(&endpoint->event_queue->fid);
	if (error != 0)
	{
		g_critical("\nProblem closing %s-Endpoint event queue.\n Details:\n %s", location, fi_strerror(error));
		error = 0;
	}

	domain_unref(endpoint->rc_domain, domain_manager, "client");

	free(endpoint);
}

/*
/closes the connection_pool and all associated objects
*/
void
j_connection_pool_fini(void)
{
	J_TRACE_FUNCTION(NULL);

	int error;
	gboolean server_shutdown_happened;
	JConnectionPool* pool;
	JEndpoint* endpoint;
	JMessage* message;

	g_return_if_fail(j_connection_pool != NULL);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	message = j_message_new(J_MESSAGE_NONE, 0);

	fi_freeinfo(j_info);

	server_shutdown_happened = FALSE;

	if ((endpoint = g_async_queue_try_pop(pool->object_queues[0].queue)) != NULL)
	{
		server_shutdown_happened = j_endpoint_shutdown_test(endpoint, "object");
		g_async_queue_push_front(pool->object_queues[0].queue, endpoint);
	}

	else if ((endpoint = g_async_queue_try_pop(pool->kv_queues[0].queue)) != NULL)
	{
		server_shutdown_happened = j_endpoint_shutdown_test(endpoint, "kv");
		g_async_queue_push_front(pool->kv_queues[0].queue, endpoint);
	}
	else if ((endpoint = g_async_queue_try_pop(pool->db_queues[0].queue)) != NULL)
	{
		server_shutdown_happened = j_endpoint_shutdown_test(endpoint, "db");
		g_async_queue_push_front(pool->db_queues[0].queue, endpoint);
	}

	for (guint i = 0; i < pool->object_len; i++)
	{
		while ((endpoint = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(endpoint, message, !server_shutdown_happened, "object");
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		while ((endpoint = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(endpoint, message, !server_shutdown_happened, "kv");
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		while ((endpoint = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(endpoint, message, !server_shutdown_happened, "db");
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	j_message_unref(message);

	error = 0;

	error = fi_close(&j_fabric->fid);
	if (error != 0)
	{
		g_critical("\nProblem closing fabric on client side.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	g_free(pool->object_queues);
	g_free(pool->kv_queues);
	g_free(pool->db_queues);

	domain_manager_fini(domain_manager);

	g_slice_free(JConnectionPool, pool);

	g_printf("\nCLIENT: shutdown finished\n");
}

/**
*
* Returns first Element from respective queue or  if none found
*
*/
static gpointer
j_connection_pool_pop_internal(GAsyncQueue* queue, guint* count, gchar const* server)
{
	J_TRACE_FUNCTION(NULL);

	JEndpoint* endpoint;

	g_return_val_if_fail(queue != NULL, NULL);
	g_return_val_if_fail(count != NULL, NULL);

start:
	endpoint = NULL;
	endpoint = g_async_queue_try_pop(queue);

	if (endpoint != NULL)
	{
		if (j_endpoint_shutdown_test(endpoint, "pop") == TRUE)
		{
			g_warning("\nShutdown Event present on this endpoint. Server most likely ended, thus no longer available.\n");
			j_endpoint_fini(endpoint, NULL, FALSE, "pop");
			goto start;
		}
		else
		{
			return endpoint;
		}
	}

	if ((guint)g_atomic_int_get(count) < j_connection_pool->max_count)
	{
		if ((guint)g_atomic_int_add(count, 1) < j_connection_pool->max_count)
		{
			gboolean comm_check;

			//fi_connection data
			g_autoptr(JMessage) message = NULL;
			g_autoptr(JMessage) reply = NULL;

			endpoint = malloc(sizeof(struct JEndpoint));

			endpoint->max_msg_size = j_info->ep_attr->max_msg_size;

			if (hostname_connector(server, j_configuration_get_fi_service(j_connection_pool->configuration), endpoint) != TRUE)
			{
				g_critical("\nhostname_connector could not connect Endpoint to given hostname\n");
			}

			comm_check = FALSE;
			message = j_message_new(J_MESSAGE_PING, 0);
			comm_check = j_message_send(message, (gpointer)endpoint);
			if (comm_check == FALSE)
			{
				g_critical("\nInitital sending check failed on Client endpoint\n\n");
			}
			else
			{
				reply = j_message_new_reply(message);

				comm_check = j_message_receive(reply, (gpointer)endpoint);

				if (comm_check == FALSE)
				{
					g_critical("\nInitital receiving check failed on Client endpoint\n\n");
				}
				else
				{
					printf("\nCLIENT: Initial data transfer check succeeded\n\n"); //debug
					fflush(stdout);
				}
			}

			/*
			op_count = j_message_get_count(reply);

			for (guint i = 0; i < op_count; i++)
			{
				gchar const* backend;

				backend = j_message_get_string(reply);

				if (g_strcmp0(backend, "object") == 0)
				{
					//g_print("Server has object backend.\n");
				}
				else if (g_strcmp0(backend, "kv") == 0)
				{
					//g_print("Server has kv backend.\n");
				}
				else if (g_strcmp0(backend, "db") == 0)
				{
					//g_print("Server has db backend.\n");
				}
			}
			*/
		}
		else
		{
			g_atomic_int_add(count, -1);
		}
	}

	if (endpoint != NULL)
	{
		return endpoint;
	}

	endpoint = g_async_queue_pop(queue);

	return endpoint;
}

static void
j_connection_pool_push_internal(GAsyncQueue* queue, JEndpoint* endpoint)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(queue != NULL);
	g_return_if_fail(endpoint != NULL);

	g_async_queue_push(queue, endpoint);
}

gpointer
j_connection_pool_pop(JBackendType backend, guint index)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(j_connection_pool != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			g_return_val_if_fail(index < j_connection_pool->object_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->object_queues[index].queue, &(j_connection_pool->object_queues[index].count), j_configuration_get_server(j_connection_pool->configuration, J_BACKEND_TYPE_OBJECT, index));
		case J_BACKEND_TYPE_KV:
			g_return_val_if_fail(index < j_connection_pool->kv_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->kv_queues[index].queue, &(j_connection_pool->kv_queues[index].count), j_configuration_get_server(j_connection_pool->configuration, J_BACKEND_TYPE_KV, index));
		case J_BACKEND_TYPE_DB:
			g_return_val_if_fail(index < j_connection_pool->db_len, NULL);
			return j_connection_pool_pop_internal(j_connection_pool->db_queues[index].queue, &(j_connection_pool->db_queues[index].count), j_configuration_get_server(j_connection_pool->configuration, J_BACKEND_TYPE_DB, index));
		default:
			g_assert_not_reached();
	}
	//g_printf("\nconnection pool pop internal done\n");
	return NULL;
}

void
j_connection_pool_push(JBackendType backend, guint index, gpointer connection)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(j_connection_pool != NULL);
	g_return_if_fail(connection != NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			g_return_if_fail(index < j_connection_pool->object_len);
			j_connection_pool_push_internal(j_connection_pool->object_queues[index].queue, connection);
			break;
		case J_BACKEND_TYPE_KV:
			g_return_if_fail(index < j_connection_pool->kv_len);
			j_connection_pool_push_internal(j_connection_pool->kv_queues[index].queue, connection);
			break;
		case J_BACKEND_TYPE_DB:
			g_return_if_fail(index < j_connection_pool->db_len);
			j_connection_pool_push_internal(j_connection_pool->db_queues[index].queue, connection);
			break;
		default:
			g_assert_not_reached();
	}
}

/**
* makes an IPV4 lookup for the given hostname + port,
* returns it into the addrinfo_ret structure and gives info about how many entries there are in the structure via size parameter
* returns FALSE if given input could not be resolved
*/
gboolean
hostname_resolver(const char* hostname, const char* service, struct addrinfo** addrinfo_ret, uint* size)
{
	int error;
	gboolean ret;
	struct addrinfo* hints;
	hints = malloc(sizeof(struct addrinfo));
	memset(hints, 0, sizeof(struct addrinfo));

	ret = FALSE;

	error = getaddrinfo(hostname, service, hints, addrinfo_ret);
	if (error != 0)
	{
		g_critical("\ngetaddrinfo did not resolve hostname\n");
		goto end;
	}

	if (*addrinfo_ret != NULL)
	{
		uint tmp_size;
		struct addrinfo* tmp_addr;

		tmp_size = 1;
		tmp_addr = *addrinfo_ret;

		while (tmp_addr->ai_next)
		{
			tmp_addr = tmp_addr->ai_next;
			tmp_size++;
		}

		*size = tmp_size;
		ret = TRUE;
	}
	else
	{
		g_critical("\naddrinfo_ret empty\n");
		goto end;
	}

end:
	freeaddrinfo(hints);
	return ret;
}

/**
* builds an libfabric endpoint + associated ressources, connects it to given hostname + port and returns said ressources JEndpoint argument
* returns FALSE if anything went wrong during the buildup, more precise information is given via seperate error messages.
* TODO has a nasty workaround for ubuntus split between local machine name IP and localhost, FIXME
*/
gboolean
hostname_connector(const char* hostname, const char* service, JEndpoint* endpoint)
{
	gboolean ret;
	uint size;
	struct addrinfo* addrinfo;

	size_t connection_entry_length;

	ret = FALSE;

	if (hostname_resolver(hostname, service, &addrinfo, &size) != TRUE)
	{
		g_critical("\nCLIENT: Hostname was not resolved into a addrinfo representation\n");
		goto end;
	}

	connection_entry_length = sizeof(struct fi_eq_cm_entry) + 128;

	for (uint i = 0; i < size; i++)
	{
		int error;
		struct fid_ep* tmp_ep;
		struct fid_eq* tmp_eq;
		struct fid_cq* tmp_cq_rcv;
		struct fid_cq* tmp_cq_transmit;

		ssize_t ssize_t_error;
		struct sockaddr_in* address;

		struct fi_eq_err_entry event_queue_err_entry;

		struct fi_eq_cm_entry* connection_entry;
		uint32_t eq_event;

		RefCountedDomain* rc_domain;

		error = 0;

		if (!domain_request(j_fabric, j_info, j_connection_pool->configuration, &rc_domain, domain_manager))
		{
			g_critical("\nDomain request failed on client side.\n");
			goto end;
		}

		address = (struct sockaddr_in*)addrinfo->ai_addr;
		//TODO change bloody ubuntu workaround ot something senseable

		if (g_strcmp0(inet_ntoa(address->sin_addr), "127.0.1.1") == 0)
		{
			inet_aton("127.0.0.1", &address->sin_addr);
		}

		//Allocate Endpoint and related ressources
		error = fi_endpoint(rc_domain->domain, j_info, &tmp_ep, NULL);
		if (error != 0)
		{
			g_critical("\nProblem initing tmp endpoint in resolver on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_eq_open(j_fabric, j_configuration_get_fi_eq_attr(j_connection_pool->configuration), &tmp_eq, NULL);
		if (error != 0)
		{
			g_critical("\nProblem opening tmp event queue in resolver on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_cq_open(rc_domain->domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &tmp_cq_transmit, NULL);
		if (error != 0)
		{
			g_critical("\nProblem opening tmp transmit transmit completion queue on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_cq_open(rc_domain->domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &tmp_cq_rcv, NULL);
		if (error != 0)
		{
			g_critical("\nProblem opening tmp transmit receive completion queue on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}

		//Bind resources to Endpoint
		error = fi_ep_bind(tmp_ep, &tmp_eq->fid, 0);
		if (error != 0)
		{
			g_critical("\nProblem while binding tmp event queue to endpoint. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_ep_bind(tmp_ep, &tmp_cq_rcv->fid, FI_RECV);
		if (error != 0)
		{
			g_critical("\nProblem while binding completion queue to endpoint as receive queue. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_ep_bind(tmp_ep, &tmp_cq_transmit->fid, FI_TRANSMIT);
		if (error != 0)
		{
			g_critical("\nProblem while binding completion queue to endpoint as transmit queue. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}

		//enable Endpoint
		error = fi_enable(tmp_ep);
		if (error != 0)
		{
			g_critical("\nProblem while enabling endpoint. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}

		printf("\nClient: Target information:\n   hostname: %s\n   IP: %s\n", hostname, inet_ntoa(((struct sockaddr_in*)addrinfo->ai_addr)->sin_addr)); //debug
		fflush(stdout);

		error = fi_connect(tmp_ep, address, NULL, 0);
		if (error == -111)
		{
			g_printf("\nConnection refused with %s resolved to %s\nEntry %d out of %d\n", hostname, inet_ntoa(address->sin_addr), i + 1, size);
		}
		else if (error != 0)
		{
			g_critical("\nProblem with fi_connect call. Client Side.\nDetails:\nIP-Addr: %s\n%s", inet_ntoa(address->sin_addr), fi_strerror(error));
			error = 0;
		}
		else
		{
			//check whether connection accepted
			eq_event = 0;
			ssize_t_error = 0;
			connection_entry = malloc(connection_entry_length);

			ssize_t_error = fi_eq_sread(tmp_eq, &eq_event, connection_entry, connection_entry_length, -1, 0);

			if (ssize_t_error < 0) //FIX: this should not be error !=0, but ssize_t_error != 0
			{
				if (ssize_t_error == FI_EBUSY)
				{
					g_critical("\nCLIENT: EQ still busy \nDetails:\n%s\nssize_t_error: %ld", fi_strerror(ssize_t_error), ssize_t_error);
					ssize_t_error = 0;
				}
				else if (ssize_t_error == -FI_EAVAIL)
				{
					ssize_t_error = fi_eq_readerr(tmp_eq, &event_queue_err_entry, 0);
					if (ssize_t_error < 0)
					{
						g_critical("\nCLIENT: Error occoured while reading tmp_eq for error message.\nDetails:\n%s", fi_strerror(ssize_t_error));
						ssize_t_error = 0;
						goto end;
					}
					else if (event_queue_err_entry.prov_errno != -FI_ECONNREFUSED)
					{
						g_critical("\nCLIENT: Error on tmp_eq while reading for FI_CONNECTED.\nDetails:\n%s\nErrno: %d\nFI_ECONNREFUSED: %d\n", fi_eq_strerror(tmp_eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0), event_queue_err_entry.prov_errno, FI_ECONNREFUSED);
						goto end;
					}
					else
					{
						printf("\nCLIENT: Connection refused with %s\n", inet_ntoa(address->sin_addr));
						fflush(stdout);
					}
				}
				else if (ssize_t_error == -FI_EAGAIN)
				{
					g_critical("\nCLIENT: No Event data on tmp_eq while reading for FI_CONNECTED.\n");
					goto end;
				}
				else if (ssize_t_error < 0)
				{
					g_critical("\nCLIENT: Other error while reading for tmp_eq for FI_CONNECTED.\nDetails:\n%s", fi_strerror(ssize_t_error));
					goto end;
				}
			}
			else
			{
				if (eq_event != FI_CONNECTED)
				{
					g_critical("\nCLIENT: FI_CONNECTED: %d\neq_event: %d\nClient endpoint did not receive FI_CONNECTED to establish a connection.\n\nAfter Fix IP:%s\nBefore Fix IP:%s", FI_CONNECTED, eq_event, inet_ntoa(address->sin_addr), inet_ntoa(((struct sockaddr_in*)addrinfo->ai_addr)->sin_addr));
				}
				else
				{
					endpoint->endpoint = tmp_ep;
					endpoint->event_queue = tmp_eq;
					endpoint->completion_queue_transmit = tmp_cq_transmit;
					endpoint->completion_queue_receive = tmp_cq_rcv;
					endpoint->rc_domain = rc_domain;
					ret = TRUE;
					printf("\nCLIENT: Connected event on client even queue\n"); //debug
					fflush(stdout);
					free(connection_entry);
					break;
				}
			}
			free(connection_entry);
		}

		error = 0;
		error = fi_close(&tmp_ep->fid);
		if (error != 0)
		{
			g_critical("\nProblem closing tmp endpoint on client\nDetails:\n%s", fi_strerror(error));
		}
		error = fi_close(&tmp_cq_rcv->fid);
		if (error != 0)
		{
			g_critical("\nProblem closing tmp endpoint completion queue (receive) on client\nDetails:\n%s", fi_strerror(error));
		}
		error = fi_close(&tmp_cq_transmit->fid);
		if (error != 0)
		{
			g_critical("\nProblem closing tmp endpoint completion queue (transmit) on client\nDetails:\n%s", fi_strerror(error));
		}
		error = fi_close(&tmp_eq->fid);
		if (error != 0)
		{
			g_critical("\nProblem closing tmp endpoint event queue on client\nDetails:\n%s", fi_strerror(error));
		}

		domain_unref(rc_domain, domain_manager, "tmp endpoint on client");

		addrinfo = addrinfo->ai_next;
	}

end:
	freeaddrinfo(addrinfo);
	return ret;
}

/**
 * @}
 **/
