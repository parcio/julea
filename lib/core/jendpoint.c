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


struct JEndpoint_
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


/**
* Inits a JEndpoint_ with all ressources, both msg and rdma endpoint including their needed ressources
* returns TRUE if successful and false if an error happened
*/
gboolean j_endpoint_init_(JEndpoint_** jendpoint,
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

  *jendpoint = malloc(sizeof(JEndpoint_));

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
* closes the JEndpoint_ given and all associated objects
*/
void
j_endpoint_fini_(JEndpoint_* jendpoint, JDomainManager* domain_manager, const gchar* location)
{
  int error;

  error = 0;

	//empty wakeup message for server Thread shutdown
  if(jendpoint->msg.connection_status || jendpoint->rdma.connection_status)
  {
    if(j_endpoint_shutdown_test_(jendpoint, location))
    {
      gboolean berror;
      JMessage* message;

      berror = FALSE;
      message = j_message_new(J_MESSAGE_NONE, 0);

  		berror = j_message_send(message, jendpoint);
  		if (berror == FALSE)
  		{
  			g_critical("\n%s: Sending wakeup message while jendpoint shutdown did not work.\n", location);
  		}
  		if (jendpoint->msg.connection_status)
  		{
  			error = fi_shutdown(jendpoint->msg.ep, 0);
  			if (error != 0)
  			{
  				g_critical("\n%s: Error during msg connection shutdown.\n Details:\n %s", location, fi_strerror(abs(error)));
  				error = 0;
  			}
  		}
  		else if (jendpoint->rdma.connection_status)
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
* returns TRUE on successful read
*/
gboolean
j_endpoint_read_completion_queue(struct fid_cq* completion_queue,
                                 int timeout,
                                 const gchar* broad_location,
                                 const gchar* specific_location)
{
  gboolean ret;
  ssize_t error;

  struct fi_cq_entry completion_queue_data;
	struct fi_cq_err_entry completion_queue_err_entry;

  ret = FALSE;
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
    else if (error == -FI_EAGAIN || error == -FI_ECANCELED)
    {
      goto end;
    }
    else if (error < 0)
    {
      g_critical("\n%s: Error while reading completion queue (%s).\nDetails:\n%s", broad_location, specific_location, fi_strerror(labs(error)));
      goto end;
    }
  }

  ret = TRUE;
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
gboolean
j_endpoint_read_event_queue(struct fid_eq* event_queue,
                            uint32_t* event,
                            void* event_entry,
                            size_t event_entry_size,
                            int timeout,
                            struct fi_eq_err_entry* event_queue_err_entry,
                            const gchar* broad_location,
                            const gchar* specific_location)
{
  gboolean ret;
  int error;

  ret = FALSE;
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
			g_critical("\n%s: No Event data on Event Queue (%s).\n", broad_location, specific_location);
			goto end;
		}
		else
		{
			g_critical("\n%s: Undefined error while reading Event Queue (%s).\nDetails:\n%s", broad_location, fi_strerror(abs(error)), specific_location);
			goto end;
		}
	}

  ret = TRUE;
  end:
  return ret;
}


/**
* Checks whether one of the 2 endpoint has a shutdown message on event queue
*/
gboolean
j_endpoint_shutdown_test_(JEndpoint_* jendpoint, const gchar* location)
{
	gboolean ret;

	uint32_t event;
  size_t event_entry_size;
	struct fi_eq_cm_entry* event_entry;
	struct fi_eq_err_entry event_queue_err_entry;

	ret = FALSE;
  event_entry_size = sizeof(struct fi_eq_cm_entry) + 128;
  event_entry = malloc(event_entry_size);

  if(!j_endpoint_read_event_queue(j_endpoint_get_event_queue(jendpoint, J_MSG),
                                  &event,
                                  event_entry,
                                  event_entry_size,
                                  1000,
                                  &event_queue_err_entry,
                                  location,
                                  "msg shutdown test"))
  {
    g_critical("\n%s: Error with shutdown Test for msg endpoint\n", location);
  }

	if (event == FI_SHUTDOWN)
	{
		ret = TRUE;
    goto end;
  }

  if(!j_endpoint_read_event_queue(j_endpoint_get_event_queue(jendpoint, J_RDMA),
                                  &event,
                                  event_entry,
                                  event_entry_size,
                                  1000,
                                  &event_queue_err_entry,
                                  location,
                                  "rdma shutdown test"))
  {
    g_critical("\n%s: Error with shutdown Test for rdma endpoint\n", location);
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
j_endpoint_get_connection_status(JEndpoint_* jendpoint, JConnectionType con_type)
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
j_endpoint_get_endpoint(JEndpoint_* jendpoint, JConnectionType con_type)
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
j_endpoint_get_info(JEndpoint_* jendpoint, JConnectionType con_type)
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
j_endpoint_get_event_queue(JEndpoint_* jendpoint, JConnectionType con_type)
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
j_endpoint_get_completion_queue(JEndpoint_* jendpoint, JConnectionType con_type, uint64_t con_part)
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
j_endpoint_get_domain(JEndpoint_* jendpoint, JConnectionType con_type)
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
j_endpoint_get_domain_eq(JEndpoint_* jendpoint, JConnectionType con_type)
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
