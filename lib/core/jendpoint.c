/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
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

#include <julea.h>

#include <jendpoint.h>

struct JEndpoint
{
	// msg structures
	struct
	{
		gboolean connection_status;
		struct fi_info* info; // info struct, contains close to all information for a certain libfabric config
		struct fid_ep* ep; // ep = endpoint
		struct fid_eq* eq; // eq = event queue
		struct fid_cq* cq_transmit; // cq = completion queue
		struct fid_cq* cq_receive;
		JRefCountedDomain* rc_domain;
	} msg;

	// rdma structures
	struct
	{
		gboolean connection_status;
		struct fi_info* info;
		struct fid_ep* ep;
		struct fid_eq* eq;
		struct fid_cq* cq_transmit;
		struct fid_cq* cq_receive;
		JRefCountedDomain* rc_domain;
	} rdma;
};

struct JConData
{
	gchar uuid[37]; // a glib UUID is 37 byte
	JConnectionType type;
};
typedef struct JConData JConData;

/**
* Inits a JEndpoint with all ressources, both msg and rdma endpoint including their needed ressources
* returns TRUE if successful and false if an error happened
*/
gboolean
j_endpoint_init(JEndpoint** jendpoint,
		JFabric* jfabric,
		JDomainManager* domain_manager,
		JConfiguration* configuration,
		struct fi_info* msg_info,
		struct fi_info* rdma_info,
		const gchar* location)
{
	gboolean ret;
	int error;

	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(msg_info != NULL, FALSE);
	g_return_val_if_fail(rdma_info != NULL, FALSE);

	ret = FALSE;
	error = 0;

	*jendpoint = malloc(sizeof(JEndpoint));

	(*jendpoint)->msg.connection_status = FALSE;
	(*jendpoint)->rdma.connection_status = FALSE;

	(*jendpoint)->msg.info = fi_dupinfo(msg_info);
	(*jendpoint)->rdma.info = fi_dupinfo(rdma_info);

	// get domains
	if (!j_domain_request(j_get_fabric(jfabric, J_MSG),
			      j_endpoint_get_info(*jendpoint, J_MSG),
			      configuration,
			      &(*jendpoint)->msg.rc_domain,
			      domain_manager,
			      location,
			      "msg"))
	{
		g_critical("\n%s: msg-Domain request failed.\n", location);
		goto end;
	}

	if (!j_domain_request(j_get_fabric(jfabric, J_RDMA),
			      j_endpoint_get_info(*jendpoint, J_RDMA),
			      configuration,
			      &(*jendpoint)->rdma.rc_domain,
			      domain_manager,
			      location,
			      "rdma"))
	{
		g_critical("\n%s: rdma-Domain request failed.\n", location);
		goto end;
	}

	//Allocate Endpoint and related ressources
	error = fi_endpoint(j_endpoint_get_domain(*jendpoint, J_MSG),
			    (*jendpoint)->msg.info,
			    &(*jendpoint)->msg.ep,
			    NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem with init of msg endpoint. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_eq_open(j_get_fabric(jfabric, J_MSG),
			   j_configuration_get_fi_eq_attr(configuration),
			   &(*jendpoint)->msg.eq,
			   NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem opening msg endpoint event queue.\nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_cq_open(j_endpoint_get_domain(*jendpoint, J_MSG),
			   j_configuration_get_fi_cq_attr(configuration),
			   &(*jendpoint)->msg.cq_transmit,
			   NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem opening msg endpoint transmit completion queue.\nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_cq_open(j_endpoint_get_domain(*jendpoint, J_MSG),
			   j_configuration_get_fi_cq_attr(configuration),
			   &(*jendpoint)->msg.cq_receive,
			   NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem opening msg endpoint receive completion queue.\nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_endpoint(j_endpoint_get_domain(*jendpoint, J_RDMA),
			    (*jendpoint)->rdma.info,
			    &(*jendpoint)->rdma.ep,
			    NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem with init of rdma endpoint. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_eq_open(j_get_fabric(jfabric, J_RDMA),
			   j_configuration_get_fi_eq_attr(configuration),
			   &(*jendpoint)->rdma.eq,
			   NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem opening rdma endpoint event queue.\nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_cq_open(j_endpoint_get_domain(*jendpoint, J_RDMA),
			   j_configuration_get_fi_cq_attr(configuration),
			   &(*jendpoint)->rdma.cq_transmit,
			   NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem opening rdma endpoint transmit completion queue.\nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_cq_open(j_endpoint_get_domain(*jendpoint, J_RDMA),
			   j_configuration_get_fi_cq_attr(configuration),
			   &(*jendpoint)->rdma.cq_receive,
			   NULL);
	if (error != 0)
	{
		g_critical("\n%s: Problem opening rdma endpoint receive completion queue.\nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	//Bind resources to Endpoint
	error = fi_ep_bind((*jendpoint)->msg.ep, &(*jendpoint)->msg.eq->fid, 0);
	if (error != 0)
	{
		g_critical("\n%s: Problem while binding msg event queue to endpoint. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->msg.ep, &(*jendpoint)->msg.cq_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\n%s: Problem while binding msg completion queue to endpoint as receive queue. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->msg.ep, &(*jendpoint)->msg.cq_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\n%s: Problem while binding msg completion queue to endpoint as transmit queue. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_ep_bind((*jendpoint)->rdma.ep, &(*jendpoint)->rdma.eq->fid, 0);
	if (error != 0)
	{
		g_critical("\n%s: Problem while binding rdma event queue to endpoint. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->rdma.ep, &(*jendpoint)->rdma.cq_receive->fid, FI_RECV);
	if (error != 0)
	{
		g_critical("\n%s: Problem while binding rdma completion queue to endpoint as receive queue. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}
	error = fi_ep_bind((*jendpoint)->rdma.ep, &(*jendpoint)->rdma.cq_transmit->fid, FI_TRANSMIT);
	if (error != 0)
	{
		g_critical("\n%s: Problem while binding rdma completion queue to endpoint as transmit queue. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	//enable Endpoint
	error = fi_enable((*jendpoint)->msg.ep);
	if (error != 0)
	{
		g_critical("\n%s: Problem while enabling msg endpoint. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	error = fi_enable((*jendpoint)->rdma.ep);
	if (error != 0)
	{
		g_critical("\n%s: Problem while enabling rdma endpoint. \nDetails:\n%s", location, fi_strerror(abs(error)));
		goto end;
	}

	ret = TRUE;

end:
	return ret;
}

/**
* closes the JEndpoint given and all associated objects
* needs the jendpoint, the respective domain_manager and the information whether the peer is expected to be waiting for input, so a wakeup needs to happen
*/
void
j_endpoint_fini(JEndpoint* jendpoint, JDomainManager* domain_manager, gboolean shutdown_test, gboolean wakeup_needed, const gchar* location)
{
	int error;

	error = 0;

	//empty wakeup message for server Thread shutdown
	if (jendpoint->msg.connection_status || jendpoint->rdma.connection_status)
	{
		if (wakeup_needed)
		{
			gboolean berror;
			JMessage* message;

			berror = FALSE;
			message = j_message_new(J_MESSAGE_NONE, 0);

			berror = j_message_send(message, jendpoint);
			if (!berror)
			{
				g_critical("\n%s: Sending wakeup message while jendpoint shutdown did not work.\n", location);
			}
			j_message_unref(message);
		}

		if (!shutdown_test)
		{
			if (jendpoint->msg.connection_status)
			{
				error = fi_shutdown(jendpoint->msg.ep, 0);
				if (error != 0)
				{
					g_critical("\n%s: Error during msg connection shutdown.\n Details:\n %s", location, fi_strerror(abs(error)));
					error = 0;
				}
			}
			if (jendpoint->rdma.connection_status)
			{
				error = fi_shutdown(jendpoint->rdma.ep, 0);
				if (error != 0)
				{
					g_critical("\n%s: Error during rdma connection shutdown.\n Details:\n %s", location, fi_strerror(abs(error)));
					error = 0;
				}
			}
		}
	}

	// close msg part
	error = fi_close(&jendpoint->msg.ep->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing msg endpoint.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.cq_receive->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing msg endpoint receive completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.cq_transmit->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing msg endpoint transmit completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->msg.eq->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing msg endpoint event queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	fi_freeinfo(jendpoint->msg.info);

	j_domain_unref(jendpoint->msg.rc_domain, domain_manager, location, "msg");

	// close rdma part
	error = fi_close(&jendpoint->rdma.ep->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing rdma Endpoint.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.cq_receive->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing rdma Endpoint receive completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.cq_transmit->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing rdma Endpoint transmit completion queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	error = fi_close(&jendpoint->rdma.eq->fid);
	if (error != 0)
	{
		g_critical("\n%s: Problem closing rdma Endpoint event queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	fi_freeinfo(jendpoint->rdma.info);

	j_domain_unref(jendpoint->rdma.rc_domain, domain_manager, location, "rdma");

	free(jendpoint);
}

/**
* Connects the JEndpoint to the target address given
* returns TRUE if successful, FALSE is an error happened
*/
JConnectionRet
j_endpoint_connect(JEndpoint* jendpoint, struct sockaddr_in* address, const gchar* location)
{
	JConnectionRet ret;
	int error;

	struct fi_eq_err_entry event_queue_err_entry;
	struct fi_eq_cm_entry* connection_entry;
	uint32_t eq_event;
	size_t connection_entry_length;

	JConData* con_data;

	error = 0;
	ret = J_CON_FAILED;
	connection_entry_length = sizeof(struct fi_eq_cm_entry) + 128;
	connection_entry = malloc(connection_entry_length);

	con_data = j_con_data_new();
	j_con_data_set_con_type(con_data, J_MSG);

	// connect msg endpoint
	error = fi_connect(jendpoint->msg.ep, address, (void*)con_data, sizeof(struct JConData));
	if (error == -FI_ECONNREFUSED)
	{
		printf("\n%s: Connection refused on msg endpoint with %s\n", location, inet_ntoa(address->sin_addr));
	}
	else if (error != 0)
	{
		g_critical("\n%s: Problem with fi_connect call on msg endpoint. Client Side.\nDetails:\nIP-Addr: %s\n%s", location, inet_ntoa(address->sin_addr), fi_strerror(abs(error)));
		error = 0;
	}

	// connect rdma endpoint
	j_con_data_set_con_type(con_data, J_RDMA);
	error = fi_connect(jendpoint->rdma.ep, address, (void*)con_data, sizeof(struct JConData));
	if (error == -FI_ECONNREFUSED)
	{
		printf("\n%s: Connection refused on rdma endpoint with %s\n", location, inet_ntoa(address->sin_addr));
	}
	else if (error != 0)
	{
		g_critical("\n%s: Problem with fi_connect call on rdma endpoint.\nDetails:\nIP-Addr: %s\n%s", location, inet_ntoa(address->sin_addr), fi_strerror(abs(error)));
		error = 0;
	}

	//check whether msg connection accepted
	switch (j_endpoint_read_event_queue(jendpoint->msg.eq, &eq_event, connection_entry, connection_entry_length, -1, &event_queue_err_entry, "CLIENT", "msg CONNECTED"))
	{
		case J_READ_QUEUE_ERROR:
			if (event_queue_err_entry.prov_errno == -FI_ECONNREFUSED)
			{
				g_critical("%s: Connection refused with %s", location, inet_ntoa(address->sin_addr));
				ret = J_CON_MSG_REFUSED;
			}
			goto end;
			break;
		case J_READ_QUEUE_SUCCESS:
			if (eq_event != FI_CONNECTED)
			{
				g_critical("\n%s: msg Endpoint did not receive FI_CONNECTED to establish a connection.\n", location);
			}
			else
			{
				jendpoint->msg.connection_status = TRUE;
				//printf("\%s: Connected event on msg client even queue\n", location); //debug
				//fflush(stdout);
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

	//check whether rdma connection accepted
	switch (j_endpoint_read_event_queue(jendpoint->rdma.eq, &eq_event, connection_entry, connection_entry_length, -1, &event_queue_err_entry, "CLIENT", "rdma CONNECTED"))
	{
		case J_READ_QUEUE_ERROR:
			if (event_queue_err_entry.prov_errno == -FI_ECONNREFUSED)
			{
				g_critical("%s: Connection refused with %s", location, inet_ntoa(address->sin_addr));
				ret = J_CON_RDMA_REFUSED;
			}
			goto end;
			break;
		case J_READ_QUEUE_SUCCESS:
			if (eq_event != FI_CONNECTED)
			{
				g_critical("\n%s: rdma Endpoint did not receive FI_CONNECTED to establish a connection.\n", location);
			}
			else
			{
				jendpoint->rdma.connection_status = TRUE;
				//printf("\%s: Connected event on rdma client even queue\n", location); //debug
				//fflush(stdout);
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

	ret = J_CON_ACCEPTED;
end:
	j_con_data_free(con_data);
	free(connection_entry);
	return ret;
}

/**
* returns TRUE on successful read
* TODO build a returntype for a) successful read, b) error and c) nothing there/canceled
*/
JReadQueueRet
j_endpoint_read_completion_queue(struct fid_cq* completion_queue,
				 int timeout,
				 const gchar* broad_location,
				 const gchar* specific_location)
{
	JReadQueueRet ret;
	ssize_t error;

	struct fi_cq_entry completion_queue_data;
	struct fi_cq_err_entry completion_queue_err_entry;

	ret = J_READ_QUEUE_ERROR;
	error = 0;

	error = fi_cq_sread(completion_queue, &completion_queue_data, 1, 0, timeout);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_cq_readerr(completion_queue, &completion_queue_err_entry, 0);
			g_critical("\n%s: Error on completion queue (%s).\nDetails:\n%s", broad_location, specific_location, fi_cq_strerror(completion_queue, completion_queue_err_entry.prov_errno, completion_queue_err_entry.err_data, NULL, 0));
			goto end;
		}
		else if (error == -FI_EAGAIN)
		{
			ret = J_READ_QUEUE_NO_EVENT;
			goto end;
		}
		else if (error == -FI_ECANCELED)
		{
			// g_critical("\nShutdown initiated while receiving Data, last transfer most likely not completed.\n"); TODO put this into relevant cases after call
			ret = J_READ_QUEUE_CANCELED;
			goto end;
		}
		else if (error < 0)
		{
			g_critical("\n%s: Error while reading completion queue (%s).\nDetails:\n%s", broad_location, specific_location, fi_strerror(labs(error)));
			goto end;
		}
	}

	ret = J_READ_QUEUE_SUCCESS;
end:
	return ret;
}

/**
* wrapper for reading endpoint bound event queues
* reads given event queue, info about the event are located in the event parameter and event_entry (more info about those in the fi_eq man)
* negative timeouts for the read will result in indefinite waiting time.
* returns true on successful read.
* TODO expand cases
*/
JReadQueueRet
j_endpoint_read_event_queue(struct fid_eq* event_queue,
			    uint32_t* event,
			    void* event_entry,
			    size_t event_entry_size,
			    int timeout,
			    struct fi_eq_err_entry* event_queue_err_entry,
			    const gchar* broad_location,
			    const gchar* specific_location)
{
	JReadQueueRet ret;
	int error;

	ret = J_READ_QUEUE_ERROR;
	error = 0;

	error = fi_eq_sread(event_queue, event, event_entry, event_entry_size, timeout, 0);
	if (error < 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_eq_readerr(event_queue, event_queue_err_entry, 0);
			if (error < 0)
			{
				g_critical("\n%s: Error occurred while reading Error Message from Event queue (%s).\nDetails:\n%s", broad_location, fi_strerror(abs(error)), specific_location);
				error = 0;
				goto end;
			}
			else
			{
				g_critical("\n%s: Error Message on Event Queue (%s).\nDetails:\n%s", broad_location, fi_eq_strerror(event_queue, event_queue_err_entry->prov_errno, event_queue_err_entry->err_data, NULL, 0), specific_location);
				goto end;
			}
		}
		else if (error == -FI_EAGAIN)
		{
			ret = J_READ_QUEUE_NO_EVENT;
			goto end;
		}
		else if (error == -FI_ECANCELED)
		{
			ret = J_READ_QUEUE_CANCELED;
			goto end;
		}
		else
		{
			g_critical("\n%s: Undefined error while reading Event Queue (%s).\nDetails:\n%s", broad_location, fi_strerror(abs(error)), specific_location);
			goto end;
		}
	}

	ret = J_READ_QUEUE_SUCCESS;
end:
	return ret;
}

/**
* Checks whether one of the 2 endpoint has a shutdown message on event queue
*/
gboolean
j_endpoint_shutdown_test(JEndpoint* jendpoint, const gchar* location)
{
	gboolean ret;

	uint32_t event;
	size_t event_entry_size;
	struct fi_eq_cm_entry* event_entry;
	struct fi_eq_err_entry event_queue_err_entry;

	ret = FALSE;
	event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
	event_entry = malloc(event_entry_size);

	switch (j_endpoint_read_event_queue(j_endpoint_get_event_queue(jendpoint, J_MSG),
					    &event,
					    event_entry,
					    event_entry_size,
					    1,
					    &event_queue_err_entry,
					    location,
					    "msg shutdown test"))
	{
		case J_READ_QUEUE_ERROR:
			g_critical("\n%s: Error with shutdown Test for msg endpoint\n", location);
			break;
		case J_READ_QUEUE_SUCCESS:
			break;
		case J_READ_QUEUE_NO_EVENT:
			break;
		case J_READ_QUEUE_CANCELED:
			break;
		default:
			g_assert_not_reached();
	}
	if (event == FI_SHUTDOWN)
	{
		ret = TRUE;
		goto end;
	}

	switch (j_endpoint_read_event_queue(j_endpoint_get_event_queue(jendpoint, J_RDMA),
					    &event,
					    event_entry,
					    event_entry_size,
					    1,
					    &event_queue_err_entry,
					    location,
					    "rdma shutdown test"))
	{
		case J_READ_QUEUE_ERROR:
			g_critical("\n%s: Error with shutdown Test for rdma endpoint\n", location);
			break;
		case J_READ_QUEUE_SUCCESS:
			break;
		case J_READ_QUEUE_NO_EVENT:
			break;
		case J_READ_QUEUE_CANCELED:
			break;
		default:
			g_assert_not_reached();
	}
	if (event == FI_SHUTDOWN)
	{
		ret = TRUE;
	}

end:
	free(event_entry);
	return ret;
}

/**
* returns TRUE if endpoint of respective connection type is connected, otherwise returns FALSE
*/
gboolean
j_endpoint_get_connection_status(JEndpoint* jendpoint, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, FALSE);

	switch (con_type)
	{
		case J_MSG:
			return jendpoint->msg.connection_status;
		case J_RDMA:
			return jendpoint->rdma.connection_status;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* returns fid_ep of respective connection type
* returns NULL if nothing found
*/
struct fid_ep*
j_endpoint_get_endpoint(JEndpoint* jendpoint, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.ep != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.ep != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return jendpoint->msg.ep;
		case J_RDMA:
			return jendpoint->rdma.ep;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* returns fi_info of respective connection type
* returns NULL if nothing found
*/
struct fi_info*
j_endpoint_get_info(JEndpoint* jendpoint, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.info != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.info != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return jendpoint->msg.info;
		case J_RDMA:
			return jendpoint->rdma.info;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* returns event queue of respective connection type
* returns NULL if nothing found
*/
struct fid_eq*
j_endpoint_get_event_queue(JEndpoint* jendpoint, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.eq != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.eq != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return jendpoint->msg.eq;
		case J_RDMA:
			return jendpoint->rdma.eq;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* returns completion queue of respective connection type (rma or msg) and the defined connection part the completion queue is designated for (receive or transmit)
* returns NULL if nothing found
*/
struct fid_cq*
j_endpoint_get_completion_queue(JEndpoint* jendpoint, JConnectionType con_type, uint64_t con_part)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.cq_transmit != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.cq_transmit != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.cq_receive != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.cq_receive != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			switch (con_part)
			{
				case FI_TRANSMIT:
					return jendpoint->msg.cq_transmit;
				case FI_RECV:
					return jendpoint->msg.cq_receive;
				default:
					g_assert_not_reached();
			}
			break;
		case J_RDMA:
			switch (con_part)
			{
				case FI_TRANSMIT:
					return jendpoint->rdma.cq_transmit;
				case FI_RECV:
					return jendpoint->rdma.cq_receive;
				default:
					g_assert_not_reached();
			}
			break;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* returns the fi_domain of the respective connection type
*/
struct fid_domain*
j_endpoint_get_domain(JEndpoint* jendpoint, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.rc_domain != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.rc_domain != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return j_get_domain(jendpoint->msg.rc_domain);
		case J_RDMA:
			return j_get_domain(jendpoint->rdma.rc_domain);
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* returns the event queue of the domain for the respective connection type
*/
struct fid_eq*
j_endpoint_get_domain_eq(JEndpoint* jendpoint, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jendpoint != NULL, NULL);
	g_return_val_if_fail(jendpoint->msg.rc_domain != NULL, NULL);
	g_return_val_if_fail(jendpoint->rdma.rc_domain != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return j_get_domain_eq(jendpoint->msg.rc_domain);
		case J_RDMA:
			return j_get_domain_eq(jendpoint->rdma.rc_domain);
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

/**
* sets the connection status of the JEndpoint in the respective connection type to TRUE
* returns TRUE on success
*/
gboolean
j_endpoint_set_connected(JEndpoint* jendpoint, JConnectionType con_type)
{
	g_return_val_if_fail(jendpoint != NULL, FALSE);

	switch (con_type)
	{
		case J_MSG:
			jendpoint->msg.connection_status = TRUE;
			break;
		case J_RDMA:
			jendpoint->rdma.connection_status = TRUE;
			break;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}

	return TRUE;
}

JConData*
j_con_data_new(void)
{
	JConData* con_data;
	gchar* tmp_uuid;

	con_data = malloc(sizeof(struct JConData));
	con_data->type = J_UNDEFINED;
	tmp_uuid = g_uuid_string_random(); //direct insert sadly leads to memory leaks due to not freed original.
	g_strlcpy(con_data->uuid, tmp_uuid, 37); //gchar* representation of uuid string is of 37 bytes
	g_free(tmp_uuid);

	return con_data;
}

void
j_con_data_free(JConData* con_data)
{
	free(con_data);
}

void
j_con_data_set_con_type(JConData* con_data, JConnectionType type)
{
	con_data->type = type;
}

JConnectionType
j_con_data_get_con_type(JConData* con_data)
{
	return con_data->type;
}

gchar*
j_con_data_get_uuid(JConData* con_data)
{
	return con_data->uuid;
}

size_t
j_con_data_get_size(void)
{
	return sizeof(JConData);
}

/**
* gets JConData* from a fi_eq_cm_entry
* retrieved JConData is part of the fi_eq_cm_entry data block, so will be freed when it is freed
*
* the JConData part of a fi_eq_cm_entry is the last part. Thus the pointer to JConData is the whole size of the entry minus the size of JConData
* conreq_size is returned of successful read of a passive endpoint
*/
JConData*
j_con_data_retrieve(struct fi_eq_cm_entry* event_entry, ssize_t conreq_size)
{
	return (JConData*)(((char*)event_entry) + (conreq_size - sizeof(struct JConData)));
}
