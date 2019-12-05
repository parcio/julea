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

#include <sys/socket.h>
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


void
j_connection_pool_init (JConfiguration* configuration)
{

	// Pool Init
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

	//Parameter for fabric init
	int error;
	int version = FI_VERSION(1, 5); //versioning Infos from libfabric, should be hardcoded so server and client run same versions, not the available ones
	const char* node = "127.0.0.1"; //NULL if addressing Format defined, otherwise can somehow be used to parse hostnames
	const char* service = "4711"; //target port (in future maybe not hardcoded)
	uint64_t flags = 0;// Alternatives: FI_NUMERICHOST (defines node to be a doted IP) // FI_SOURCE (source defined by node+service)

	struct fi_info* fi_hints = NULL; //config object

	struct fi_eq_attr event_queue_attr = {10, 0, FI_WAIT_SET, 0, NULL};



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
	flags = 0;

	fi_hints = fi_allocinfo(); //initiated object is zeroed

	if(fi_hints == NULL)
	{
		g_critical("\nAllocating empty hints did not work");
	}

	//TODO read hints from config (and define corresponding fields there) + or all given caps
	fi_hints->caps = FI_MSG; // | FI_SEND | FI_RECV;
	fi_hints->fabric_attr->prov_name = g_strdup("sockets"); //sets later returned providers to socket providers, TODO for better performance not socket, but the best (first) available
	fi_hints->addr_format = FI_SOCKADDR_IN; //Server-Adress Format IPV4. TODO: Change server Definition in Config or base system of name resolution
	//TODO future support to set modes
	//fabri_hints.mode = 0;
	fi_hints->domain_attr->threading = FI_THREAD_UNSPEC; //FI_THREAD_COMPLETION or FI_THREAD_FID or FI_THREAD_SAFE

	//inits j_info
	//gives a linked list of available provider details into j_info
	error = fi_getinfo(version, node, service, flags,	fi_hints, &j_info);
	fi_freeinfo(fi_hints); //hints only used for config
	if(error != 0)
	{
		goto end;
	}
	if(j_info == NULL)
	{
		g_critical("\nAllocating Client j_info did not work");
	}

	//validating juleas needs here
	//PERROR: through no casting (size_t and guint)

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
		goto end;
	}
	if(j_fabric == NULL)
	{
		g_critical("\nAllocating j_fabric did not work");
	}

	//init domain
	error = fi_domain(j_fabric, j_info, &j_domain, NULL);
	if(error != 0) //WHO THE HELL DID DESIGN THOSE ERROR CODES?! SUCESS IS SOMETIMES 0, SOMETIMES A DEFINED BUT NOT DOCUMENTED VALUE, ARGH
	{
		goto end;
	}
	if(j_fabric == NULL)
	{
		g_critical("\nAllocating j_fabric did not work");
	}

	//build event queue for domain
	//TODO read event_queue attributes from julea config
	//PERROR: Wrong formatting of event queue attributes
	error = fi_eq_open(j_fabric, &event_queue_attr, &j_domain_event_queue, NULL);
	if(error != 0)
	{
		goto end;
	}
	//bind an event queue to domain
	//PERROR: 0 is possibly not an acceptable parameter for fi_domain_bind (what exactly is acceptable, except 1 possible flag, is not mentioned in man)
	error = fi_domain_bind(j_domain, &j_domain_event_queue->fid, 0);
	if(error != 0)
	{
		goto end;
	}


	end:
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during init.\n Details:\n %s", fi_strerror(error));
	}

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
			}
			error = 0;


			error = fi_close(&(endpoint->endpoint->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->completion_queue_receive->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint receive completion queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->completion_queue_transmit->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint transmit completion queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->event_queue->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing object-Endpoint event queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;
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
			}
			error = 0;

			error = fi_close(&(endpoint->endpoint->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			//PERROR: you should be able to give a complete object into fi_close, but lets see whether this works too
			error = fi_close(&(endpoint->completion_queue_receive->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint receive completion queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->completion_queue_transmit->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint transmit completion queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->event_queue->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing kv-Endpoint event queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;
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
			}
			error = 0;

			error = fi_close(&(endpoint->endpoint->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->completion_queue_receive->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint receive completion queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->completion_queue_transmit->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint transmit completion queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;

			error = fi_close(&(endpoint->event_queue->fid));
			if(error != 0)
			{
				g_critical("\nSomething went horribly wrong during closing db-Endpoint event queue.\n Details:\n %s", fi_strerror(error));
			}
			error = 0;
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
	}

	error = fi_close(&(j_domain->fid));
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during closing domain.\n Details:\n %s", fi_strerror(error));
	}

	error = fi_close(&(j_fabric->fid));
	if(error != 0)
	{
		g_critical("\nSomething went horribly wrong during closing fabric.\n Details:\n %s", fi_strerror(error));
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
			uint32_t eq_event;
			int error;
			ssize_t ssize_t_error;

			//struct fid_ep* tmp_endpoint;
			//struct fid_eq* tmp_event_queue;
			//struct fid_cq* tmp_receive_queue;
			//struct fid_cq* tmp_transmit_queue;

			//Endpoint related definitions
			struct fi_eq_attr event_queue_attr = {10, 0, FI_WAIT_MUTEX_COND, 0, NULL}; //TODO read eq attributes from config
			struct fi_cq_attr completion_queue_attr = {0, 0, FI_CQ_FORMAT_MSG, FI_WAIT_MUTEX_COND, 0, FI_CQ_COND_NONE, 0}; //TODO read cq attributes from config

			//connection related definitions
			struct sockaddr_in address;

			//Event queue structs
			struct fi_eq_cm_entry* connection_entry;
			size_t connection_entry_length;

			//fi_connection data

			g_autoptr(JMessage) message = NULL;
			g_autoptr(JMessage) reply = NULL;

			connection_entry_length = sizeof(struct fi_eq_cm_entry) + 128;

			endpoint = malloc(sizeof(struct JEndpoint));
			error = 0;


			// guint op_count;

			endpoint->max_msg_size = j_info->ep_attr->max_msg_size;

			//Allocate Endpoint and related ressources
			//PERROR: last param of fi_endpoint maybe mandatory. If so, build context struct with every info in it
			error = fi_endpoint(j_domain, j_info, &endpoint->endpoint, NULL);
			if(error != 0)
			{
				g_critical("\nProblem during client endpoint init. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			error = fi_eq_open(j_fabric, &event_queue_attr, &endpoint->event_queue, NULL);
			if(error != 0)
			{
				g_critical("\nProblem during client endpoint event queue opening. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			error = fi_cq_open(j_domain, &completion_queue_attr, &endpoint->completion_queue_transmit, NULL);
			if(error != 0)
			{
				g_critical("\nProblem during client endpoint completion queue 1 (transmit) opening. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			error = fi_cq_open(j_domain, &completion_queue_attr, &endpoint->completion_queue_receive, NULL);
			if(error != 0)
			{
				g_critical("\nProblem during client endpoint completion queue 2 (receive) opening. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			//Bind resources to Endpoint
			error = fi_ep_bind(endpoint->endpoint, &endpoint->event_queue->fid, 0);
			if(error != 0)
			{
				g_critical("\nProblem while binding event queue to endpoint. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			error = fi_ep_bind(endpoint->endpoint, &endpoint->completion_queue_receive->fid, FI_RECV);
			if(error != 0)
			{
				g_critical("\nProblem while binding completion queue 1 to endpoint as receive queue. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			error = fi_ep_bind(endpoint->endpoint, &endpoint->completion_queue_transmit->fid, FI_TRANSMIT);
			if(error != 0)
			{
				g_critical("\nProblem while binding completion queue 2 to endpoint as transmit queue. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}

			//enable Endpoint
			error = fi_enable(endpoint->endpoint);
			if(error != 0)
			{
				g_critical("\nProblem while enabling endpoint. \nDetails:\n%s", fi_strerror((int)error));
				error = 0;
			}


			//Connect Endpoint to server via port 4711
			address.sin_family = AF_INET;
			address.sin_port = htons(4711);
			inet_aton("127.0.0.1", &address.sin_addr); //TODO Resolve server-variable per glib resolver + insert here, most likely use aton or g_inet_address_to_bytes

			//PERROR: User specified data maybe required to be set
			//g_printf("\n\nValue of error before fi_connect: %d\n", error);
			error = fi_connect(endpoint->endpoint, &address, NULL, 0);
			if(error != 0)
			{
				g_critical("\nProblem with fi_connect call. Client Side.\nDetails:\nIP-Addr: %s\n%s", inet_ntoa(address.sin_addr), fi_strerror((int)error));
				error = 0;
			}

			g_printf("\nCLIENT REACHED POINT AFTER CONNECTION READ!\n");

			//check whether connection accepted
			eq_event = 0;
			ssize_t_error = 0;
			connection_entry = malloc(connection_entry_length);
			ssize_t_error = fi_eq_sread(endpoint->event_queue, &eq_event, connection_entry, connection_entry_length, -1, 0);
			if(error != 0)
			{
				g_critical("\nClient Problem reading event queue \nDetails:\n%s", fi_strerror(ssize_t_error));
				ssize_t_error = 0;
			}
			if (eq_event != FI_CONNECTED)
			{
				g_critical("\n\nFI_CONNECTED: %d\neq_event: %d\nClient endpoint did not receive FI_CONNECTED to establish a connection.\n\n", FI_CONNECTED, eq_event);
			}
			else
			{
				g_printf("\nYEAH, CONNECTED EVENT ON CLIENT EVENT QUEUE\n");
			}
			g_printf("\nCLIENT REACHED POINT AFTER CONNECTED READ!\n");
			free(connection_entry);

			// j_helper_set_nodelay(connection, TRUE); //irrelevant at the moment, since function aims at g_socket_connection

			message = j_message_new(J_MESSAGE_PING, 0);
			j_message_send(message, endpoint);

			reply = j_message_new_reply(message);
			j_message_receive(reply, endpoint);

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
 * @}
 **/
