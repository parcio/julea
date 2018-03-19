/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include "j_mercury.h"


/**
 * Creates a new mercury connection.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param address Address to listen or send to.
 * \param listen  Run as a server or client?
 *
 * \return A new connection. Should be freed with j_mercury_final().
 **/
JMercuryConnection* j_mercury_init (const char* local_addr, gboolean listen)
{
	JMercuryConnection* connection = g_slice_new(JMercuryConnection);
	connection->shutdown_flag = FALSE;

	/* Initialize RPC Layer */
	connection->class = HG_Init(local_addr, (na_bool_t) listen);
	g_assert(connection->class);

	/* Create an RPC context */
	connection->context = HG_Context_create(connection->class);
	g_assert(connection->context);

	/* Start a thread to manage progress */
	g_assert(pthread_create(&connection->thread_id, NULL, j_mercury_process, connection) == 0);

	return connection;
}


/**
 * Terminates an existing mercury connection.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param connection JMercuryConnection* struct.
 **/
void j_mercury_final (JMercuryConnection* connection)
{
	/* Tell progress thread to terminate */
	connection->shutdown_flag = TRUE;

	/* Wait for thread to shutdown */
	g_assert(pthread_join(connection->thread_id, NULL) == 0);

	/* Free allocated memory */
	g_slice_free(JMercuryConnection, connection);

	return;
}


/**
 * An async lookup for an address.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param connection JMercuryConnection* struct.
 * \param name Connection string, e.g. "tcp://127.0.0.1:1111".
 * \param callback Pointer of callback function.
 * \param arg Arguments of callback function.
 **/
void j_mercury_lookup (JMercuryConnection* connection, const char* name, hg_cb_t callback, void* arg)
{
	g_assert(HG_Addr_lookup(connection->context, callback, arg, name, HG_OP_ID_IGNORE) == NA_SUCCESS);

	return;
}


/**
 * Create a handle for a connection.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param connection JMercuryConnection* struct.
 * \param address The address to an rpc server, found via j_mercury_lookup().
 * \param rpc_id The ID of the RPC we want to use.
 * \param handle Pointer where to put handler to.
 **/
void j_mercury_create_handle (JMercuryConnection* connection, na_addr_t address, hg_id_t rpc_id, hg_handle_t* handle)
{
	g_assert(HG_Create(connection->context, address, rpc_id, handle) == HG_SUCCESS);

	return;
}


/**
 * Manage a Mercury process.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param connection JMercuryConnection* struct.
 *
 * \return NULL pointer.
 **/
static void* j_mercury_process (void* connection)
{
	hg_return_t ret;
	guint count;

	while(!((JMercuryConnection*) connection)->shutdown_flag)
	{
		do
		{
			ret = HG_Trigger(((JMercuryConnection*) connection)->context, 0, 1, &count);
		}
		while((ret == HG_SUCCESS) && count && !((JMercuryConnection*) connection)->shutdown_flag);

		if(!((JMercuryConnection*) connection)->shutdown_flag)
		{
			HG_Progress(((JMercuryConnection*) connection)->context, 100);
		}
	}

	return (NULL);
}


/**
 * Register specified rpc type.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param connection JMercuryConnection* struct.
 *
 * \return ID of RPC handle.
 **/
hg_id_t j_mercury_rpc_register (JMercuryConnection* connection)
{
	return MERCURY_REGISTER(connection->class, "j_mercury_rpc", JMercuryRPCIn, JMercuryRPCOut, j_mercury_rpc_target);
}


/**
 * Handler for an rpc request.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param handle RPC handle from MERCURY_REGISTER, used in j_mercury_rpc_register().
 *
 * \return Zero on success.
 **/
static hg_return_t j_mercury_rpc_target (hg_handle_t handle)
{
	struct JMercuryRPCState* rpc_state;
	const struct hg_info* handle_info;

	rpc_state = g_malloc0(sizeof(*rpc_state));
	g_assert(rpc_state);

	rpc_state->handle = handle;

	/* Decode RPC input */
	g_assert(HG_Get_input(handle, &rpc_state->in) == HG_SUCCESS);

	/* Input value of RPC request is our buffer size for bulk transfer */
	rpc_state->size = rpc_state->in.input_val;
	g_assert(rpc_state->size > 0);
	g_printf("[ RPC  ]  Bulk transfer size: %d\n", rpc_state->size);

	/* Allocating a target buffer for bulk transfer */
	rpc_state->buffer = g_malloc0(rpc_state->size * sizeof(gchar));
	g_assert(rpc_state->buffer);

	/* Register local target buffer for bulk transfer */
	handle_info = HG_Get_info(handle);
	g_assert(handle_info);

	/* Create handle for bulk transfer */
	g_assert(HG_Bulk_create(handle_info->hg_class, 1, &rpc_state->buffer, &rpc_state->size,
		HG_BULK_WRITE_ONLY, &rpc_state->bulk_handle) == 0);

	/* Initiate bulk transfer from client to server */
	g_assert(HG_Bulk_transfer(handle_info->context, j_mercury_rpc_bulk_handler, rpc_state,
		HG_BULK_PULL, handle_info->addr, rpc_state->in.bulk_handle, 0, rpc_state->bulk_handle, 0,
		rpc_state->size, HG_OP_ID_IGNORE) == 0);

	return 0;
}


/**
 * Handler for a bulk rpc request.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param info RPC callback handle, used in j_mercury_rpc_target().
 *
 * \return Zero on success.
 **/
/* callback triggered upon completion of bulk transfer */
static hg_return_t j_mercury_rpc_bulk_handler (const struct hg_cb_info* info)
{
	struct JMercuryRPCState *rpc_state = info->arg;
	JMercuryRPCOut rpc_out;

	rpc_out.ret = 42;

	g_assert(info->ret == 0);

	g_printf("[ DATA ]  %s", rpc_state->buffer);

	/* Send ACK to client */
	g_assert(HG_Respond(rpc_state->handle, NULL, NULL, &rpc_out) == HG_SUCCESS);

	/* Cleanup */
	HG_Bulk_free(rpc_state->bulk_handle);
	HG_Destroy(rpc_state->handle);
	g_free(rpc_state->buffer);
	g_free(rpc_state);

	return 0;
}
