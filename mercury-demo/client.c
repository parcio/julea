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

#include <unistd.h>
#include <pthread.h>
#include <glib.h>
#include <glib/gprintf.h>

#include "j_mercury.h"


/* Threading
   Count connection to unlock mutex and finalize */
static gint thread_counter = 0;
static pthread_cond_t thread_condition = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Define to call in main() */
static hg_return_t j_mercury_rpc_origin_callback(const struct hg_cb_info*);
static hg_return_t j_mercury_rpc_origin(const struct hg_cb_info*);

/* Mercury connection and RPC ID */
JMercuryConnection* connection;
static hg_id_t rpc_id;
static gint number_of_rpcs = 20;


/**
 * Starts a client.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \return Exit code. Should be zero, otherwise something did not worked well.
 **/
int main(void)
{

	/* Initialize connection
	   We define only plugin and protocoll, because this should not
	   listen to a network connection */
	connection = j_mercury_init("cci+tcp", FALSE);
	rpc_id = j_mercury_rpc_register(connection);

	/* Send 20 RPC requests to server */
	for(gint i = 0; i < number_of_rpcs; i++)
	{

		/* Set transfer size of following bulk transfer*/
		gint* rpc_value = g_malloc(sizeof(*rpc_value));
		g_assert(rpc_value);
		*rpc_value = 4096;

		/* Lookup server address and call j_mercury_rpc_origin() */
		j_mercury_lookup(connection, "cci+tcp://127.0.0.1:4242", j_mercury_rpc_origin, rpc_value);
	}

	/* Wait for RPC requests to finish */
	pthread_mutex_lock(&thread_mutex);
	while(thread_counter < number_of_rpcs)
	{
		pthread_cond_wait(&thread_condition, &thread_mutex);
	}
	pthread_mutex_unlock(&thread_mutex);

	/* Finalize client connection */
	j_mercury_final(connection);

	return(0);
}


/**
 * Callback function to start a bulk transfer.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param info A struct containing RPC value and connection information
 *
 * \return NA_SUCCESS if successfull.
 **/
static hg_return_t j_mercury_rpc_origin(const struct hg_cb_info* info)
{
	JMercuryRPCState* rpc_state;
	JMercuryRPCIn in;
	const struct hg_info* handle_info;

	g_assert(info->ret == 0);

	rpc_state = g_malloc(sizeof(*rpc_state));
	g_assert(rpc_state);

	/* Set RPC value as size */
	rpc_state->size = *((gint*) info->arg);
	rpc_state->value = *((gint*) info->arg);

	/* Allocating a buffer for bulk transfer */
	rpc_state->buffer = g_malloc0(rpc_state->size * sizeof(gchar));
	g_assert(rpc_state->buffer);

	/* Write example text to buffer */
	g_sprintf((gchar*) rpc_state->buffer, "Lorem ipsum dolor sit amet! %d\n", g_rand_int(g_rand_new()));

	/* Cleanup */
	g_free(info->arg);

	/* Create handle for this RPC request */
	j_mercury_create_handle(connection, info->info.lookup.addr, rpc_id, &rpc_state->handle);

	/* Register a bulk transfer */
	handle_info = HG_Get_info(rpc_state->handle);
	g_assert(handle_info);

	/* Create handle for bulk transfer */
	g_assert(HG_Bulk_create(handle_info->hg_class, 1, &rpc_state->buffer, &rpc_state->size,
		HG_BULK_READ_ONLY, &in.bulk_handle) == 0);
	rpc_state->bulk_handle = in.bulk_handle;

	/* Send RPC and also start bulk transfer */
	in.input_val = rpc_state->value;
	g_assert(HG_Forward(rpc_state->handle, j_mercury_rpc_origin_callback, rpc_state, &in) == 0);

	return(NA_SUCCESS);
}


/**
 * Callback function triggered after a bulk transfer to cleanup.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \param info A struct containing RPC value and connection information
 *
 * \return HG_SUCCESS if successfull.
 **/
static hg_return_t j_mercury_rpc_origin_callback(const struct hg_cb_info* info)
{
	JMercuryRPCOut out;
	JMercuryRPCState* rpc_state = info->arg;

	g_assert(info->ret == HG_SUCCESS);

	/* Decode RPC input */
	g_assert(HG_Get_output(info->info.forward.handle, &out) == 0);

	g_printf("[ RPC  ]  Got response: %d\n", out.ret);

	/* Cleanup */
	HG_Bulk_free(rpc_state->bulk_handle);
	HG_Free_output(info->info.forward.handle, &out);
	HG_Destroy(info->info.forward.handle);
	g_free(rpc_state->buffer);
	g_free(rpc_state);

	/* Signal to main() */
	pthread_mutex_lock(&thread_mutex);
	thread_counter++;
	pthread_cond_signal(&thread_condition);
	pthread_mutex_unlock(&thread_mutex);

	return(HG_SUCCESS);
}
