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
#include <rdma/fi_domain.h> //includes cqs and eqs
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h> //connection management
#include <rdma/fi_errno.h> //translation error numbers

#include <netinet/in.h> //for target address

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
static fid_fabric* j_fid_fabric;
static fid_domain* j_fid_domain;
static fid_eq* j_fid_domain_eventqueue;
//libfabric config structures
static fi_info* j_fi_info;


void
j_connection_pool_init (JConfiguration* configuration)
{

	// Pool Init
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

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
	//Parameter for fabric init
	int error = 0;
	int version = FI_VERSION(FI_MAJOR(1, 6); //versioning Infos from libfabric, should be hardcoded so server and client run same versions, not the available ones
	const char* node = NULL; //TODO read target Server from config and put into node
	const char* service = "4711"; //target port (in future maybe not hardcoded)
	uint64_t flags = 0;// Alternatives: FI_NUMERICHOST (defines node to be a doted IP) // FI_SOURCE (source defined by node+service)

	fi_info* fi_hints = NULL; //config object



	fi_hints = fi_allocinfo(); //initiated object is zeroed

	if(fi_hints == NULL)
	{
		g_critical("Allocating empty hints did not work");
	}

	//TODO read hints from config (and define corresponding fields there) + or all given caps
	fi_hints.caps = FI_MSG | FI_SEND | FI_RECV;
	fi_hints->fabric_attr->prov_name = "sockets"; //sets later returned providers to socket providers, TODO for better performance not socket, but the best (first) available
	fi_hints->adr_format = FI_SOCKADDR_IN; //Server-Adress Format IPV4. TODO: Change server Definition in Config or base system of name resolution
	//TODO future support to set modes
	//fabri_hints.mode = 0;

	//inits j_fi_info
	//gives a linked list of available provider details into j_fi_info
	error = fi_getinfo(version, node, service, flags,	fi_hints, &j_fi_info)
	fi_freeinfo(fi_hints); //hints only used for config
	if(error != 0)
	{
		goto end;
	}
	if(j_fi_info == NULL)
	{
		g_critical("Allocating j_fi_info did not work");
	}

	//validating juleas needs here
	//PERROR: through no casting (size_t and guint)
	if(j_fi_info->domain_attr->ep_cnt < pool->object_len * 3 + 1)
	{
		g_critical("Demand for connections exceeds the max number of endpoints available through domain/provider");
	}
	if(j_fi_info->domain_attr->cq_cnt < (pool->object_len * 3) * 2 + 1) //1 active endpoint has 2 completion queues, 1 receive, 1 transmit
	{
		g_warning("Demand for connections exceeds the optimal number of completion queues available through domain/provider");
	}


	//inits fabric
	error = fi_fabric(j_fi_info->fabric_attr, &j_fid_fabric, NULL);
	if(error != FI_SUCCESS)
	{
		goto end;
	}
	if(j_fid_fabric == NULL)
	{
		g_critical("Allocating j_fid_fabric did not work");
	}

	//inits domain
	error = fi_domain(j_fid_fabric, j_fi_info, &j_fid_domain, NULL);
	if(error != 0) //WHO THE HELL DID DESIGN THOSE ERROR CODES?! SUCESS IS SOMETIMES 0, SOMETIMES A DEFINED BUT NOT DOCUMENTED VALUE, ARGH
	{
		goto end;
	}
	if(j_fid_fabric == NULL)
	{
		g_critical("Allocating j_fid_fabric did not work");
	}

	//build event queue for domain
	//TODO read eventqueue attributes from julea config
	//PERROR: Wrong formatting of event queue attributes
	struct fi_eq_attr eventqueue_attr = {50, FI_WRITE, FI_WAIT_UNSPEC, 0, NULL};
	error = fi_eq_open(j_fid_fabric, &eventqueue_attr, &j_fid_domain_eventqueue, NULL);
	if(error != 0)
	{
		goto end;
	}
	//bind an event queue to domain
	//PERROR: FI_WRITE is not a acceptable parameter for fi_domain_bind (what exactly is acceptable, is not mentioned in man)
	error = fi_domain_bind(j_fid_domain, j_fid_domain_eventqueue, FI_WRITE);
	if(error != 0)
	{
		goto end;
	}


	end:
	if(error != 0)
	{
		g_critical("Something went horribly wrong during init.\n Details:\n %s", fi_strerror(error));
	}

}

void
j_connection_pool_fini (void)
{
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;

	g_return_if_fail(j_connection_pool != NULL);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	for (guint i = 0; i < pool->object_len; i++)
	{
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		GSocketConnection* connection;

		while ((connection = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			g_io_stream_close(G_IO_STREAM(connection), NULL, NULL);
			g_object_unref(connection);
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

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

	endpoint = g_async_queue_try_pop(queue);

	if (endpoint != NULL)
	{
		return endpoint;
	}

	if ((guint)g_atomic_int_get(count) < j_connection_pool->max_count)
	{
		if ((guint)g_atomic_int_add(count, 1) < j_connection_pool->max_count)
		{
			endpoint = NULL;

			int error = 0;

			//Endpoint related definitions
			struct fi_eq_attr event_queue_attr = {10, 0, FI_WAIT_UNSPEC, 0, NULL}; //TODO read eq attributes from config
			struct fi_cq_attr completion_queue_attr = {0, 0, FI_CQ_FORMAT_MSG, 0, 0, FI_CQ_COND_NONE, 0}; //TODO read cq attributes from config

			fid_ep* tmp_endpoint = NULL;
			fid_eq* tmp_event_queue = NULL;
			fid_cq* tmp_receive_queue = NULL;
			fid_cq* tmp_transmit_queue = NULL;

			//connection related definitions
			struct sockaddr_in address;
			uint32_t* eq_event;

			//Test Message related definitions
			g_autoptr(JMessage) message = NULL;
			g_autoptr(JMessage) reply = NULL;

			// guint op_count;

			//Allocate Endpoint and related ressources
			//PERROR: last param of fi_endpoint maybe mandatory. If so, build context struct with every info in it
			error = fi_endpoint(j_fid_domain, j_fi_info, &tmp_endpoint, NULL);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			error = fi_eq_open(j_fid_fabric, &event_queue_attr, &tmp_event_queue, NULL);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			error = fi_cq_open(j_fid_domain, &completion_queue_attr, &tmp_transmit_queue, NULL);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			error = fi_cq_open(j_fid_domain, &completion_queue_attr, &tmp_receive_queue, NULL);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			//Bind resources to Endpoint
			error = fi_ep_bind(tmp_endpoint, tmp_event_queue, 0);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			error = fi_ep_bind(tmp_endpoint, tmp_receive_queue, FI_RECV);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			error = fi_ep_bind(tmp_endpoint, tmp_receive_queue, FI_TRANSMIT);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			//enable Endpoint
			error = fi_enable(tmp_endpoint);
			if(error != 0)
			{
				goto ConnectionTest;
			}


			//Connect Endpoint to server via port 4711
			address.sin_family = AF_INET;
			address.sin_port = htons(4711);
			address.sin_addr = htonl(server); //TODO server ist atm not an IPV4 address

			//PERROR: User specified data maybe required to be set
			error = fi_connect(tmp_endpoint, &address, NULL, NULL);
			if(error != 0)
			{
				goto ConnectionTest;
			}

			//check whether connection accepted
			error = (int) fi_eq_read(tmp_endpoint, eq_event, NULL, 0, 0); //PERROR: fi_eq_read could need a buffer to report infos about the event (even if it is irrelevant here)
			if(error != 0)
			{
				goto ConnectionTest;
			}
			if (eq_event != FI_CONNECTED)
			{
				g_critical("TMP-Endpoint has no established connection towards: %s [%d].", server, g_atomic_int_get(count));
			}


			//bind endpoint to the tmp_structures
			endpoint->endpoint = tmp_endpoint;
			endpoint->event_queue = tmp_event_queue;
			endpoint->completion_queue_receive = tmp_receive_queue;
			endpoint->completion_queue_transmit = tmp_transmit_queue;


			ConnectionTest:
			if (error != 0)
			{
				g_critical("%s", *fi_strerror((int)error));
			}

			if(endpoint == NULL)
			{
				g_critical("Endpoint-binding did not work.");
			}

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
j_connection_pool_push_internal (GAsyncQueue* queue, GSocketConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(queue != NULL);
	g_return_if_fail(connection != NULL);

	g_async_queue_push(queue, connection);
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
