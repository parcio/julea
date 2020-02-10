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

//hostname to ip resolver includes
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <glib/gprintf.h>

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
static struct fid_domain* j_domain;
static struct fid_eq* j_domain_event_queue;
//libfabric config structures
static struct fi_info* j_info;

/*
char*
hostname_resolver(const char* hostname, const char* service);
*/


gboolean
hostname_resolver(const char* hostname, const char* service, struct addrinfo** addrinfo_ret, uint* amount);

gboolean
hostname_connector(const char* hostname, const char* service, JEndpoint* endpoint);


void
j_connection_pool_init (JConfiguration* configuration)
{

	// Pool Init
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

	//Parameter for fabric init
	int error;
	struct fi_info* fi_hints;

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

	fi_hints = fi_dupinfo(j_configuration_fi_get_hints(configuration));

	if(fi_hints == NULL)
	{
		g_critical("\nAllocating empty hints did not work");
	}

	//inits j_info
	//gives a linked list of available provider details into j_info
	error = fi_getinfo(j_configuration_get_fi_version(configuration),
										 j_configuration_get_fi_node(configuration),
										 j_configuration_get_fi_service(configuration),
										 j_configuration_get_fi_flags(configuration, 0),
										 fi_hints,
										 &j_info);
	fi_freeinfo(fi_hints); //hints only used for config //TODO test whether stable with no additional fi_hints, but direct read from config
	if(error != 0)
	{
		goto end;
	}
	if(j_info == NULL)
	{
		g_critical("\nAllocating Client j_info did not work");
	}

	//validating juleas needs here

	if(j_info->domain_attr->ep_cnt < (pool->object_len + pool->kv_len + pool->db_len) * pool->max_count + 1)
	{
		g_critical("\nDemand for connections exceeds the max number of endpoints available through domain/provider");
	}
	/*
	TODO: activate again
	if(j_info->domain_attr->cq_cnt < (pool->object_len + pool->kv_len + pool->db_len) * pool->max_count * 2 + 1) //1 active endpoint has 2 completion queues, 1 receive, 1 transmit
	{
		g_warning("\nDemand for connections exceeds the optimal number of completion queues available through domain/provider");
	}
	*/


	//init fabric
	error = fi_fabric(j_info->fabric_attr, &j_fabric, NULL);
	if(error != FI_SUCCESS)
	{
		g_critical("\nAllocating fabric on client side did not work\nDetails:\n %s", fi_strerror(error));
		goto end;
	}

	//init domain
	error = fi_domain(j_fabric, j_info, &j_domain, NULL);
	if(error != 0) //WHO THE HELL DID DESIGN THOSE ERROR CODES?! SUCCESS IS SOMETIMES 0, SOMETIMES A DEFINED BUT NOT DOCUMENTED VALUE, ARGH
	{
		g_critical("\nAllocating domain on client side did not work\nDetails:\n %s", fi_strerror(error));
		goto end;
	}
	//build event queue for domain
	error = fi_eq_open(j_fabric, j_configuration_get_fi_eq_attr(configuration), &j_domain_event_queue, NULL);
	if(error != 0)
	{
		g_critical("\nProblem with opening event queue for domain during connection pool init\nDetails:\n %s", fi_strerror(error));
		goto end;
	}
	//bind an event queue to domain
	error = fi_domain_bind(j_domain, &j_domain_event_queue->fid, 0);
	if(error != 0)
	{
		g_critical("\nProblem with binding event queue to domain during connection pool init\nDetails:\n %s", fi_strerror(error));
		goto end;
	}

	end:
	;
}

/*
/closes the connection_pool and all associated objects
*/
void
j_connection_pool_fini (void)
{
	J_TRACE_FUNCTION(NULL);

	int error;
	JConnectionPool* pool;
	JEndpoint* endpoint;

	g_return_if_fail(j_connection_pool != NULL);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	fi_freeinfo(j_info);

	for (guint i = 0; i < pool->object_len; i++)
	{
		endpoint = NULL;
		error = 0;

		while ((endpoint = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			error = fi_shutdown(endpoint->endpoint,0);
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during object connection shutdown.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->endpoint->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->completion_queue_receive->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint receive completion queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->completion_queue_transmit->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint transmit completion queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->event_queue->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint event queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}
			free(endpoint);
			endpoint = NULL;
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		error = 0;

		while ((endpoint = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			error = fi_shutdown(endpoint->endpoint,0);
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during kv connection shutdown.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->endpoint->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->completion_queue_receive->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint receive completion queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->completion_queue_transmit->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint transmit completion queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->event_queue->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint event queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}
			free(endpoint);
			endpoint = NULL;
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		error = 0;

		while ((endpoint = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			error = fi_shutdown(endpoint->endpoint,0);
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during db connection shutdown.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->endpoint->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->completion_queue_receive->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint receive completion queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->completion_queue_transmit->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint transmit completion queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}

			error = fi_close(&(endpoint->event_queue->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint event queue.\n Details:\n %s", fi_strerror(error));
				error = 0;
			}
			free(endpoint);
			endpoint = NULL;
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	error = 0;

	error = fi_close(&(j_domain_event_queue->fid));
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during closing domain event queue.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&(j_domain->fid));
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during closing domain.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	error = fi_close(&(j_fabric->fid));
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during closing fabric.\n Details:\n %s", fi_strerror(error));
		error = 0;
	}

	g_free(pool->object_queues);
	g_free(pool->kv_queues);
	g_free(pool->db_queues);

	g_slice_free(JConnectionPool, pool);
}

/*
/
/ Returns first Element from respective queue or  if none found
/
*/
static
gpointer
j_connection_pool_pop_internal (GAsyncQueue* queue, guint* count, gchar const* server)
{
	J_TRACE_FUNCTION(NULL);

	JEndpoint* endpoint;

	g_return_val_if_fail(queue != NULL, NULL);
	g_return_val_if_fail(count != NULL, NULL);

	endpoint = NULL;
	endpoint = g_async_queue_try_pop(queue);

	if (endpoint != NULL)
	{
		return endpoint;
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

			if(hostname_connector(server, j_configuration_get_fi_service(j_connection_pool->configuration), endpoint) != TRUE)
			{
				g_critical("\nhostname_connector could not connect Endpoint to given hostname\n");
			}

			comm_check = FALSE;
			message = j_message_new(J_MESSAGE_PING, 0);
			comm_check = j_message_send(message, (gpointer) endpoint);
			if(comm_check == FALSE)
			{
				g_critical("\nInitital sending check failed on Client endpoint\n\n");
			}
			else
			{
				//g_printf("\nInitial sending check succeeded\n\n");
				reply = j_message_new_reply(message);

				comm_check = j_message_receive(reply, (gpointer) endpoint);

				if(comm_check == FALSE)
				{
					g_critical("\nInitital receiving check failed on Client endpoint\n\n");
				}
				else
				{
					//g_printf("\nInitial receiving check succeeded\n\n");
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

static
void
j_connection_pool_push_internal (GAsyncQueue* queue, JEndpoint* endpoint)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(queue != NULL);
	g_return_if_fail(endpoint != NULL);

	g_async_queue_push(queue, endpoint);
}

gpointer
j_connection_pool_pop (JBackendType backend, guint index)
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


//TODO deal with sever connection shutdown
void
j_connection_pool_push (JBackendType backend, guint index, gpointer connection)
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
	if(error != 0)
	{
		g_critical("\ngetaddrinfo did not resolve hostname\n");
		goto end;
	}

	if(* addrinfo_ret != NULL)
	{
		uint tmp_size;
		struct addrinfo* tmp_addr;

		tmp_size = 1;
		tmp_addr = * addrinfo_ret;

		while(tmp_addr->ai_next)
		{
			tmp_addr = tmp_addr->ai_next;
			tmp_size++;
		}

		* size = tmp_size;
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

	if(hostname_resolver(hostname, service, &addrinfo, &size) != TRUE)
	{
		g_critical("\nHostname was not resolved into a addrinfo representation\n");
		goto end;
	}

	connection_entry_length = sizeof(struct fi_eq_cm_entry) + 256;

	for (uint i = 0; i < size; i++)
	{
		int error;
		struct fid_ep* tmp_ep;
		struct fid_eq* tmp_eq;
		struct fid_cq* tmp_cq_rcv;
		struct fid_cq* tmp_cq_transmit;

		ssize_t ssize_t_error;
		struct sockaddr_in address;

		struct fi_eq_cm_entry* connection_entry;
		uint32_t eq_event;

		error = 0;

		//Allocate Endpoint and related ressources
		error = fi_endpoint(j_domain, j_info, &tmp_ep, NULL);
		if(error != 0)
		{
			g_critical("\nProblem initing tmp endpoint in resolver on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_eq_open(j_fabric, j_configuration_get_fi_eq_attr(j_connection_pool->configuration), &tmp_eq, NULL);
		if(error != 0)
		{
			g_critical("\nProblem opening tmp event queue in resolver on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_cq_open(j_domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &tmp_cq_transmit, NULL);
		if(error != 0)
		{
			g_critical("\nProblem opening tmp transmit transmit completion queue on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_cq_open(j_domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &tmp_cq_rcv, NULL);
		if(error != 0)
		{
			g_critical("\nProblem opening tmp transmit receive completion queue on client. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}

		//Bind resources to Endpoint
		error = fi_ep_bind(tmp_ep, &tmp_eq->fid, 0);
		if(error != 0)
		{
			g_critical("\nProblem while binding tmp event queue to endpoint. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_ep_bind(tmp_ep, &tmp_cq_rcv->fid, FI_RECV);
		if(error != 0)
		{
			g_critical("\nProblem while binding completion queue to endpoint as receive queue. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}
		error = fi_ep_bind(tmp_ep, &tmp_cq_transmit->fid, FI_TRANSMIT);
		if(error != 0)
		{
			g_critical("\nProblem while binding completion queue to endpoint as transmit queue. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}

		//enable Endpoint
		error = fi_enable(tmp_ep);
		if(error != 0)
		{
			g_critical("\nProblem while enabling endpoint. \nDetails:\n%s", fi_strerror((int)error));
			error = 0;
		}

		//g_printf("\nAfter Resolver:\n   hostname: %s\n   IP: %s\n", hostname, inet_ntoa(( (struct sockaddr_in* ) addrinfo->ai_addr)->sin_addr));

		//prepare a
		address.sin_family = AF_INET;
		address.sin_port = htons(atoi(j_configuration_get_fi_service(j_connection_pool->configuration))); //4711
		//TODO change bloody ubuntu workaround ot something senseable
		if(g_strcmp0(inet_ntoa(((struct sockaddr_in*) addrinfo->ai_addr)->sin_addr), "127.0.1.1" ) == 0)
		{
			inet_aton("127.0.0.1", &address.sin_addr);
		}
		else
		{
			address.sin_addr = ( (struct sockaddr_in*) addrinfo->ai_addr)->sin_addr;
		}

		error = fi_connect(tmp_ep, &address, NULL, 0);
		if(error == -111)
		{
			g_printf("\nConnection refused with %s resolved to %s\nEntry %d out of %d\n", hostname, inet_ntoa(address.sin_addr), i + 1, size);
		}
		else if(error != 0)
		{
			g_critical("\nProblem with fi_connect call. Client Side.\nDetails:\nIP-Addr: %s\n%s", inet_ntoa(address.sin_addr), fi_strerror(error));
			error = 0;
		}
		else
		{
			//check whether connection accepted
			eq_event = 0;
			ssize_t_error = 0;
			connection_entry = malloc(connection_entry_length);
			ssize_t_error = fi_eq_sread(tmp_eq, &eq_event, connection_entry, connection_entry_length, -1, 0);
			if(error != 0)
			{
				g_critical("\nClient Problem reading event queue \nDetails:\n%s", fi_strerror(ssize_t_error));
				ssize_t_error = 0;
				goto tmp_free;
			}
			if (eq_event != FI_CONNECTED)
			{
				g_critical("\n\nFI_CONNECTED: %d\neq_event: %d\nClient endpoint did not receive FI_CONNECTED to establish a connection.\n\n", FI_CONNECTED, eq_event);
				goto tmp_free;
			}
			else
			{
				endpoint->endpoint = tmp_ep;
				endpoint->event_queue = tmp_eq;
				endpoint->completion_queue_transmit = tmp_cq_transmit;
				endpoint->completion_queue_receive = tmp_cq_rcv;
				ret = TRUE;
				//g_printf("\nYEAH, CONNECTED EVENT ON CLIENT EVENT QUEUE\n");
				free(connection_entry);
				break;
			}
			free(connection_entry);
		}

		tmp_free:
		error = 0;
		error = fi_close(&tmp_ep->fid);
		if(error != 0)
		{
			g_critical("\nProblem closing tmp endpoint on client\nDetails:\n%s", fi_strerror(error));
		}

		addrinfo = addrinfo->ai_next;
	}

	end:
	freeaddrinfo(addrinfo);
	return ret;
}


/**
* uses getaddrinfo to get info about the environment of hostname including hostname IP and
* inet_ntoa to get the IP into a doted IPV4 representation for libfabric usage
*/
/*
char*
hostname_resolver(const char* hostname, const char* service)
{
	int error;
	char* ip;
	char* ret;
	struct addrinfo* hints;
	struct addrinfo* addrinfo_ret;

	hints = malloc(sizeof(struct addrinfo));
	memset(hints, 0, sizeof(struct addrinfo));
	ret = NULL;
	ip = NULL;

	error = getaddrinfo(hostname, service, hints, &addrinfo_ret);
	if(error != 0)
	{
		g_critical("\ngetaddrinfo did not resolve hostname\n");
		goto end;
	}

	if(ip == NULL)
	{
		g_critical("\nIP not parsed\n");
		goto end;
	}

	ret = g_strdup(ip);
	g_printf("\nhostname: %s\nIP: %s\n", hostname, ret);

	end:
	freeaddrinfo(hints);
	freeaddrinfo(addrinfo_ret);
	return g_strdup("127.0.0.1"); //return ret;
}
*/

/**
 * @}
 **/
