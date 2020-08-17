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
static JFabric* jfabric;

static JDomainManager* domain_manager;

void
j_connection_pool_init(JConfiguration* configuration)
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
	if (!j_fabric_init(&jfabric, J_CLIENT, configuration, "CLIENT"))
	{
		goto end;
	}

	domain_manager = j_domain_manager_init();

end:;
}

/*
/closes the connection_pool and all associated objects
*/
void
j_connection_pool_fini(void)
{
	J_TRACE_FUNCTION(NULL);

	JConnectionPool* pool;
	JEndpoint* jendpoint;

	g_return_if_fail(j_connection_pool != NULL);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	for (guint i = 0; i < pool->object_len; i++)
	{
		while ((jendpoint = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(jendpoint, domain_manager, "CLIENT");
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		while ((jendpoint = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(jendpoint, domain_manager, "CLIENT");
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		while ((jendpoint = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(jendpoint, domain_manager, "CLIENT");
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	j_fabric_fini(jfabric, "CLIENT");

	g_free(pool->object_queues);
	g_free(pool->kv_queues);
	g_free(pool->db_queues);

	j_domain_manager_fini(domain_manager);

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

	JEndpoint* jendpoint;

	g_return_val_if_fail(queue != NULL, NULL);
	g_return_val_if_fail(count != NULL, NULL);

start:
	jendpoint = NULL;
	jendpoint = g_async_queue_try_pop(queue);

	if (jendpoint != NULL)
	{
		if (j_endpoint_shutdown_test(jendpoint, "CLIENT"))
		{
			g_warning("\nCLIENT: Shutdown Event present on this endpoint. Server most likely ended, thus no longer available.\n");
			j_endpoint_fini(jendpoint, domain_manager, "CLIENT");
			goto start;
		}
		else
		{
			return jendpoint;
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

			jendpoint = NULL;

			if (hostname_connector(server, j_configuration_get_fi_service(j_connection_pool->configuration), &jendpoint) != TRUE)
			{
				g_critical("\nCLIENT: hostname_connector could not connect JEndpoint to given hostname\n");
			}

			comm_check = FALSE;
			message = j_message_new(J_MESSAGE_PING, 0);
			comm_check = j_message_send(message, (gpointer)jendpoint);
			if (comm_check == FALSE)
			{
				g_critical("\nCLIENT: Initital sending check failed\n\n");
			}
			else
			{
				reply = j_message_new_reply(message);

				comm_check = j_message_receive(reply, (gpointer)jendpoint);

				if (comm_check == FALSE)
				{
					g_critical("\nCLIENT: Initital receiving check failed on jendpoint\n\n");
				}
				else
				{
					//printf("\nCLIENT: Initial data transfer check succeeded\n\n"); //debug
					//fflush(stdout);
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

	if (jendpoint != NULL)
	{
		return jendpoint;
	}

	jendpoint = g_async_queue_pop(queue);

	return jendpoint;
}

static void
j_connection_pool_push_internal(GAsyncQueue* queue, JEndpoint* jendpoint)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(queue != NULL);
	g_return_if_fail(jendpoint != NULL);

	g_async_queue_push(queue, jendpoint);
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
		g_critical("\nCLIENT: getaddrinfo did not resolve hostname\n");
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
hostname_connector(const char* hostname, const char* service, JEndpoint** jendpoint)
{
	gboolean ret;
	uint size;
	struct addrinfo* addrinfo;
	JEndpoint* tmp_jendpoint;
	JConnectionRet con_ret;
	struct sockaddr_in* address;

	ret = FALSE;

	if (hostname_resolver(hostname, service, &addrinfo, &size) != TRUE)
	{
		g_critical("\nCLIENT: Hostname was not resolved into a addrinfo representation\n");
		goto end;
	}

	for (uint i = 0; i < size; i++)
	{
		address = (struct sockaddr_in*)addrinfo->ai_addr;

		//TODO change ubuntu workaround to something senseable
		if (g_strcmp0(inet_ntoa(address->sin_addr), "127.0.1.1") == 0)
		{
			inet_aton("127.0.0.1", &address->sin_addr);
		}

		if (!j_endpoint_init(&tmp_jendpoint,
				     jfabric,
				     domain_manager,
				     j_connection_pool->configuration,
				     j_get_info(jfabric, J_MSG),
				     j_get_info(jfabric, J_RDMA),
				     "CLIENT"))
		{
			g_critical("\nCLIENT: tmp_jendpoint init failed\n");
			j_endpoint_fini(tmp_jendpoint, domain_manager, "CLIENT");
			goto end;
		}

		//printf("\nClient: Target information:\n   hostname: %s\n   IP: %s\n", hostname, inet_ntoa(((struct sockaddr_in*)addrinfo->ai_addr)->sin_addr)); //debug
		//fflush(stdout);
		con_ret = j_endpoint_connect(tmp_jendpoint, address, "CLIENT");
		if (con_ret == J_CON_ACCEPTED)
		{
			(*jendpoint) = tmp_jendpoint;
			ret = TRUE;
			break;
		}
		else if (con_ret == J_CON_MSG_REFUSED)
		{
			j_endpoint_fini(tmp_jendpoint, domain_manager, "CLIENT");
			g_critical("\nCLIENT: msg tmp_jendpoint connreq was refused\n");
			goto end;
		}
		else if (con_ret == J_CON_RDMA_REFUSED)
		{
			j_endpoint_fini(tmp_jendpoint, domain_manager, "CLIENT");
			g_critical("\nCLIENT: rdma tmp_jendpoint connreq was refused\n");
			goto end;
		}
		else
		{
			break;
		}

		addrinfo = addrinfo->ai_next;
	}

end:
	freeaddrinfo(addrinfo);
	return ret;
}

/**
 * @}
 **/
