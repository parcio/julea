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

static DomainManager* domain_manager;

void
j_connection_pool_init(JConfiguration* configuration)
{
	struct fi_info* info;

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

	//inits info
	error = fi_getinfo(j_configuration_get_fi_version(configuration),
			   NULL,
			   j_configuration_get_fi_service(configuration),
			   j_configuration_get_fi_flags(configuration, J_CLIENT),
			   j_configuration_fi_get_hints(configuration, J_MSG),
			   &info);

	if (error < 0)
	{
		goto end;
	}
	if (info == NULL)
	{
		g_critical("\nCLIENT: allocating info for fabric did not work");
	}

	//init fabric
	error = fi_fabric(info->fabric_attr, &j_fabric, NULL);
	if (error != FI_SUCCESS)
	{
		g_critical("\nCLIENT: Allocating fabric did not work\nDetails:\n %s", fi_strerror(abs(error)));
		goto end;
	}

	fi_freeinfo(info);

	domain_manager = domain_manager_init();

end:;
}

/**
* Checks whether endpoint has a shutdown message on event queue
*
*/
gboolean
j_endpoint_shutdown_test(JEndpoint* jendpoint, const gchar* location)
{
	int error;
	gboolean ret;

	uint32_t event;
	struct fi_eq_cm_entry* event_entry;
	struct fi_eq_err_entry event_queue_err_entry;

	ret = FALSE;
	error = 0;

	event_entry = malloc(sizeof(struct fi_eq_cm_entry) + 128);
	error = fi_eq_read(jendpoint->msg.eq, &event, event_entry, sizeof(struct fi_eq_cm_entry) + 128, 0);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr(jendpoint->msg.eq, &event_queue_err_entry, 0);
			if (error < 0)
			{
				g_critical("\nCLIENT: Error occurred while reading Error Message from Event queue (%s) reading for shutdown.\nDetails:\n%s", fi_strerror(abs(error)), location);
			}
			else
			{
				g_critical("\nCLIENT: Error Message on Event queue (%s) reading for shutdown .\nDetails:\n%s", location, fi_eq_strerror(jendpoint->msg.eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
			}
		}
		else if (error == -FI_EAGAIN)
		{
			ret = FALSE;
		}
		else if (error < 0)
		{
			g_critical("\nCLIENT: Error while reading event queue (%s) after reading for shutdown.\nDetails:\n%s", fi_strerror(abs(error)), location);
		}
	}
	free(event_entry);
	if (event == FI_SHUTDOWN)
	{
		ret = TRUE;
	}
	return ret;
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
	JEndpoint* jendpoint;
	JMessage* message;

	g_return_if_fail(j_connection_pool != NULL);

	pool = g_atomic_pointer_get(&j_connection_pool);
	g_atomic_pointer_set(&j_connection_pool, NULL);

	message = j_message_new(J_MESSAGE_NONE, 0);

	server_shutdown_happened = FALSE;

	if ((jendpoint = g_async_queue_try_pop(pool->object_queues[0].queue)) != NULL)
	{
		server_shutdown_happened = j_endpoint_shutdown_test(jendpoint, "object");
		g_async_queue_push_front(pool->object_queues[0].queue, jendpoint);
	}

	else if ((jendpoint = g_async_queue_try_pop(pool->kv_queues[0].queue)) != NULL)
	{
		server_shutdown_happened = j_endpoint_shutdown_test(jendpoint, "kv");
		g_async_queue_push_front(pool->kv_queues[0].queue, jendpoint);
	}
	else if ((jendpoint = g_async_queue_try_pop(pool->db_queues[0].queue)) != NULL)
	{
		server_shutdown_happened = j_endpoint_shutdown_test(jendpoint, "db");
		g_async_queue_push_front(pool->db_queues[0].queue, jendpoint);
	}

	for (guint i = 0; i < pool->object_len; i++)
	{
		while ((jendpoint = g_async_queue_try_pop(pool->object_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(jendpoint, message, !server_shutdown_happened, "object");
		}

		g_async_queue_unref(pool->object_queues[i].queue);
	}

	for (guint i = 0; i < pool->kv_len; i++)
	{
		while ((jendpoint = g_async_queue_try_pop(pool->kv_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(jendpoint, message, !server_shutdown_happened, "kv");
		}

		g_async_queue_unref(pool->kv_queues[i].queue);
	}

	for (guint i = 0; i < pool->db_len; i++)
	{
		while ((jendpoint = g_async_queue_try_pop(pool->db_queues[i].queue)) != NULL)
		{
			j_endpoint_fini(jendpoint, message, !server_shutdown_happened, "db");
		}

		g_async_queue_unref(pool->db_queues[i].queue);
	}

	j_configuration_unref(pool->configuration);

	j_message_unref(message);

	error = 0;

	error = fi_close(&j_fabric->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing fabric.\n Details:\n %s", fi_strerror(abs(error)));
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

	JEndpoint* jendpoint;

	g_return_val_if_fail(queue != NULL, NULL);
	g_return_val_if_fail(count != NULL, NULL);

start:
	jendpoint = NULL;
	jendpoint = g_async_queue_try_pop(queue);

	if (jendpoint != NULL)
	{
		if (j_endpoint_shutdown_test(jendpoint, "pop"))
		{
			g_warning("\nCLIENT: Shutdown Event present on this endpoint. Server most likely ended, thus no longer available.\n");
			j_endpoint_fini(jendpoint, NULL, FALSE, "pop");
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

			jendpoint = malloc(sizeof(struct JEndpoint));

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
* Inits a JEndpoint with all ressources, both msg and rdma endpoint including their needed ressources
* returns TRUE if successful and false if an error happened
*/
gboolean
j_endpoint_init(JEndpoint* jendpoint)
{
	gboolean ret;
	int error;

	ret = FALSE;
	error = 0;

	error = fi_getinfo(j_configuration_get_fi_version(j_connection_pool->configuration),
			   NULL,
			   NULL,
			   j_configuration_get_fi_flags(j_connection_pool->configuration, J_CLIENT),
			   j_configuration_fi_get_hints(j_connection_pool->configuration, J_MSG),
			   &jendpoint->msg.info);
	if (error < 0)
	{
		g_critical("\nCLIENT: fi_getinfo for msg info failed\n");
		goto end;
	}

	error = fi_getinfo(j_configuration_get_fi_version(j_connection_pool->configuration),
			   NULL,
			   NULL,
			   j_configuration_get_fi_flags(j_connection_pool->configuration, J_CLIENT),
			   j_configuration_fi_get_hints(j_connection_pool->configuration, J_RDMA),
			   &jendpoint->rdma.info);
	if (error < 0)
	{
		g_critical("\nCLIENT: fi_getinfo for rdma info failed\n");
		goto end;
	}

	if (!domain_request(j_fabric, jendpoint->msg.info, j_connection_pool->configuration, &jendpoint->msg.rc_domain, domain_manager))
	{
		g_critical("\nCLIENT: msg-Domain request failed.\n");
		goto end;
	}

	if (!domain_request(j_fabric, jendpoint->rdma.info, j_connection_pool->configuration, &jendpoint->rdma.rc_domain, domain_manager))
	{
		g_critical("\nCLIENT: rdma-Domain request failed.\n");
		goto end;
	}

	//Allocate Endpoint and related ressources
	error = fi_endpoint(jendpoint->msg.rc_domain->domain, jendpoint->msg.info, &jendpoint->msg.ep, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem with init of msg jendpoint. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_eq_open(j_fabric, j_configuration_get_fi_eq_attr(j_connection_pool->configuration), &jendpoint->msg.eq, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem opening msg event queue.\nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open(jendpoint->msg.rc_domain->domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &jendpoint->msg.cq_transmit, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem opening msg transmit completion queue.\nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open(jendpoint->msg.rc_domain->domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &jendpoint->msg.cq_receive, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem opening msg receive completion queue.\nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_endpoint(jendpoint->rdma.rc_domain->domain, jendpoint->rdma.info, &jendpoint->rdma.ep, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem with init of rdma jendpoint. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_eq_open(j_fabric, j_configuration_get_fi_eq_attr(j_connection_pool->configuration), &jendpoint->rdma.eq, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem opening rdma event queue.\nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open(jendpoint->rdma.rc_domain->domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &jendpoint->rdma.cq_transmit, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem opening rdma transmit completion queue.\nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_cq_open(jendpoint->rdma.rc_domain->domain, j_configuration_get_fi_cq_attr(j_connection_pool->configuration), &jendpoint->rdma.cq_receive, NULL);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem opening rdma receive completion queue.\nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}

	//Bind resources to Endpoint
	error = fi_ep_bind(jendpoint->msg.ep, &jendpoint->msg.eq->fid, 0);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while binding msg event queue to endpoint. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind(jendpoint->msg.ep, &jendpoint->msg.cq_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while binding msg completion queue to endpoint as receive queue. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind(jendpoint->msg.ep, &jendpoint->msg.cq_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while binding msg completion queue to endpoint as transmit queue. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_ep_bind(jendpoint->rdma.ep, &jendpoint->rdma.eq->fid, 0);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while binding rdma event queue to endpoint. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind(jendpoint->rdma.ep, &jendpoint->rdma.cq_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while binding rdma completion queue to endpoint as receive queue. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind(jendpoint->rdma.ep, &jendpoint->rdma.cq_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while binding rdma completion queue to endpoint as transmit queue. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}

	//enable Endpoint
	error = fi_enable(jendpoint->msg.ep);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while enabling msg endpoint. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}

	error = fi_enable(jendpoint->rdma.ep);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem while enabling rdma endpoint. \nDetails:\n%s", fi_strerror(abs(error)));
		goto end;
	}

	// set connection flags

	jendpoint->msg.is_connected = FALSE;
	jendpoint->rdma.is_connected = FALSE;

	// TODO build special rdma Ressources

	ret = TRUE;
end:
	return ret;
}

/**
* closes the JEndpoint given and all associated objects
*
*/
void
j_endpoint_fini(JEndpoint* jendpoint, JMessage* message, gboolean send_shutdown_message, const gchar* location)
{
	int error = 0;

	//empty wakeup message for server Thread shutdown
	if (send_shutdown_message && (jendpoint->msg.is_connected || jendpoint->rdma.is_connected))
	{
		gboolean berror;
		berror = j_message_send(message, jendpoint);
		if (berror == FALSE)
		{
			g_critical("\nCLIENT: Sending wakeup message while connection pool (%s queues) shutdown did not work.\n", location);
		}
		if (jendpoint->msg.is_connected)
		{
			error = fi_shutdown(jendpoint->msg.ep, 0);
			if (error != 0)
			{
				g_critical("\nCLIENT: Error during %s msg connection shutdown.\n Details:\n %s", location, fi_strerror(abs(error)));
				error = 0;
			}
		}
		else if (jendpoint->rdma.is_connected)
		{
			error = fi_shutdown(jendpoint->rdma.ep, 0);
			if (error != 0)
			{
				g_critical("\nCLIENT: Error during %s rdma connection shutdown.\n Details:\n %s", location, fi_strerror(abs(error)));
				error = 0;
			}
		}
	}

	error = fi_close(&jendpoint->msg.ep->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s msg Endpoint.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.cq_receive->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s msg Endpoint receive completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.cq_transmit->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s msg Endpoint transmit completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.eq->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s msg Endpoint event queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	fi_freeinfo(jendpoint->msg.info);

	domain_unref(jendpoint->msg.rc_domain, domain_manager, "msg client");

	error = fi_close(&jendpoint->rdma.ep->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s rdma Endpoint.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.cq_receive->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s rdma Endpoint receive completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.cq_transmit->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s rdma Endpoint transmit completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.eq->fid);
	if (error != 0)
	{
		g_critical("\nCLIENT: Problem closing %s rdma Endpoint event queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	fi_freeinfo(jendpoint->rdma.info);

	domain_unref(jendpoint->rdma.rc_domain, domain_manager, "rdma client");

	//TODO close jendpoint->rmda rdma ressources

	free(jendpoint);
}

/**
* Connects the JEndpoint responding to the connection type in con_data
* returns TRUE if successful, FALSE is an error happened
*/
JConnectionRet
j_endpoint_connect(JEndpoint* jendpoint, const char* hostname, struct sockaddr_in* address)
{
	JConnectionRet ret;
	int error;
	ssize_t ssize_t_error;

	struct fi_eq_err_entry event_queue_err_entry;
	struct fi_eq_cm_entry* connection_entry;
	uint32_t eq_event;
	size_t connection_entry_length;

	JConData* con_data;
	gchar* tmp_uuid;

	error = 0;
	ssize_t_error = 0;
	ret = J_CON_FAILED;
	connection_entry_length = sizeof(struct fi_eq_cm_entry) + 128; //TODO use this to get additional info, like keys, etc
	connection_entry = malloc(connection_entry_length);

	con_data = malloc(sizeof(struct JConData));
	con_data->type = J_MSG;
	tmp_uuid = g_uuid_string_random(); //direct insert sadly leads to memory leaks due to not freed original.
	g_strlcpy(con_data->uuid, tmp_uuid, 37); //gchar* representation of uuid string is of 37 bytes
	g_free(tmp_uuid);

	// connect msg endpoint
	error = fi_connect(jendpoint->msg.ep, address, (void*)con_data, sizeof(struct JConData));
	if (error == -FI_ECONNREFUSED)
	{
		g_printf("\nCLIENT: Connection refused on msg endpoint with %s resolved to %s\n", hostname, inet_ntoa(address->sin_addr));
	}
	else if (error != 0)
	{
		g_critical("\nCLIENT: Problem with fi_connect call on msg endpoint. Client Side.\nDetails:\nIP-Addr: %s\n%s", inet_ntoa(address->sin_addr), fi_strerror(abs(error)));
		error = 0;
	}

	// connect rdma endpoint
	con_data->type = J_RDMA;
	error = fi_connect(jendpoint->rdma.ep, address, (void*)con_data, sizeof(struct JConData));
	if (error == -FI_ECONNREFUSED)
	{
		g_printf("\nCLIENT: Connection refused on rdma endpoint with %s resolved to %s\n", hostname, inet_ntoa(address->sin_addr));
	}
	else if (error != 0)
	{
		g_critical("\nCLIENT: Problem with fi_connect call on rdma endpoint.\nDetails:\nIP-Addr: %s\n%s", inet_ntoa(address->sin_addr), fi_strerror(abs(error)));
		error = 0;
	}

	//check whether msg connection accepted
	ssize_t_error = fi_eq_sread(jendpoint->msg.eq, &eq_event, connection_entry, connection_entry_length, -1, 0);
	if (ssize_t_error < 0)
	{
		if (ssize_t_error == FI_EBUSY)
		{
			g_critical("\nCLIENT: msg EQ still busy \nDetails:\n%s", fi_strerror(labs(ssize_t_error)));
			ssize_t_error = 0;
		}
		else if (ssize_t_error == -FI_EAVAIL)
		{
			ssize_t_error = fi_eq_readerr(jendpoint->msg.eq, &event_queue_err_entry, 0);
			if (ssize_t_error < 0)
			{
				g_critical("\nCLIENT: Error occoured while reading msg eq for error message during connection.\nDetails:\n%s", fi_strerror(labs(ssize_t_error)));
				ssize_t_error = 0;
				goto end;
			}
			else if (event_queue_err_entry.prov_errno != -FI_ECONNREFUSED)
			{
				g_critical("\nCLIENT: Error on msg eq while reading for FI_CONNECTED during connection.\nDetails:\n%s\n", fi_eq_strerror(jendpoint->msg.eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
			else
			{
				printf("\nCLIENT: msg Connection refused with %s\n", inet_ntoa(address->sin_addr));
				fflush(stdout);
				ret = J_CON_MSG_REFUSED;
				goto end;
			}
		}
		else if (ssize_t_error == -FI_EAGAIN)
		{
			g_critical("\nCLIENT: No Event data on msg eq while reading for FI_CONNECTED while connecting.\n");
			goto end;
		}
		else if (ssize_t_error < 0)
		{
			g_critical("\nCLIENT: Other error while reading for msg eq for FI_CONNECTED while connecting.\nDetails:\n%s", fi_strerror(labs(ssize_t_error)));
			goto end;
		}
	}
	else
	{
		if (eq_event != FI_CONNECTED)
		{
			g_critical("\nCLIENT: msg Endpoint did not receive FI_CONNECTED to establish a connection.\n");
		}
		else
		{
			jendpoint->msg.is_connected = TRUE;
			//printf("\nCLIENT: Connected event on msg client even queue\n"); //debug
			//fflush(stdout);
		}
	}

	ssize_t_error = fi_eq_sread(jendpoint->rdma.eq, &eq_event, connection_entry, connection_entry_length, -1, 0);
	if (ssize_t_error < 0)
	{
		if (ssize_t_error == FI_EBUSY)
		{
			g_critical("\nCLIENT: rdma EQ still busy \nDetails:\n%s", fi_strerror(labs(ssize_t_error)));
			ssize_t_error = 0;
		}
		else if (ssize_t_error == -FI_EAVAIL)
		{
			ssize_t_error = fi_eq_readerr(jendpoint->rdma.eq, &event_queue_err_entry, 0);
			if (ssize_t_error < 0)
			{
				g_critical("\nCLIENT: Error occoured while reading rdma eq for error message during connection.\nDetails:\n%s", fi_strerror(labs(ssize_t_error)));
				ssize_t_error = 0;
				goto end;
			}
			else if (event_queue_err_entry.prov_errno != -FI_ECONNREFUSED)
			{
				g_critical("\nCLIENT: Error on rdma eq while reading for FI_CONNECTED during connection.\nDetails:\n%s\n", fi_eq_strerror(jendpoint->rdma.eq, event_queue_err_entry.prov_errno, event_queue_err_entry.err_data, NULL, 0));
				goto end;
			}
			else
			{
				printf("\nCLIENT: rdma Connection refused with %s\n", inet_ntoa(address->sin_addr));
				fflush(stdout);
				ret = J_CON_RDMA_REFUSED;
				goto end;
			}
		}
		else if (ssize_t_error == -FI_EAGAIN)
		{
			g_critical("\nCLIENT: No Event data on rdma eq while reading for FI_CONNECTED while connecting.\n");
			goto end;
		}
		else if (ssize_t_error < 0)
		{
			g_critical("\nCLIENT: Other error while reading for rdma eq for FI_CONNECTED while connecting.\nDetails:\n%s", fi_strerror(labs(ssize_t_error)));
			goto end;
		}
	}
	else
	{
		if (eq_event != FI_CONNECTED)
		{
			g_critical("\nCLIENT: rdma Endpoint did not receive FI_CONNECTED to establish a connection.\n");
		}
		else
		{
			//printf("\nCLIENT: Connected event on client even queue\n"); //debug
			//fflush(stdout);
			jendpoint->rdma.is_connected = TRUE;
		}
	}

	ret = J_CON_ACCEPTED;
end:
	free(con_data);
	free(connection_entry);
	return ret;
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

		tmp_jendpoint = malloc(sizeof(JEndpoint));
		if (!j_endpoint_init(tmp_jendpoint))
		{
			g_critical("\nCLIENT: tmp_jendpoint init failed\n");
			j_endpoint_fini(tmp_jendpoint, NULL, FALSE, "client tmp jendpoint build");
			goto end;
		}

		//printf("\nClient: Target information:\n   hostname: %s\n   IP: %s\n", hostname, inet_ntoa(((struct sockaddr_in*)addrinfo->ai_addr)->sin_addr)); //debug
		//fflush(stdout);
		con_ret = j_endpoint_connect(tmp_jendpoint, hostname, address);
		if (con_ret == J_CON_ACCEPTED)
		{
			(*jendpoint) = tmp_jendpoint;
			ret = TRUE;
			break;
		}
		else if (con_ret == J_CON_MSG_REFUSED)
		{
			g_critical("\nCLIENT: msg tmp_jendpoint connreq was refused\n");
			j_endpoint_fini(tmp_jendpoint, NULL, FALSE, "client msg tmp jendpoint connect");
			goto end;
		}
		else if (con_ret == J_CON_RDMA_REFUSED)
		{
			JMessage* message;

			g_critical("\nCLIENT: rdma tmp_jendpoint connreq was refused\n");
			message = j_message_new(J_MESSAGE_NONE, 0); // TODO use a wakeup message with rdma communication
			j_endpoint_fini(tmp_jendpoint, NULL, FALSE, "client rdma tmp jendpoint connect");
			j_message_unref(message);
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
