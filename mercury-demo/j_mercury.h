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

#ifndef JULEA_MERCURY_H
#define JULEA_MERCURY_H

// #if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
// #error "Only <julea.h> can be included directly."
// #endif

#include <glib.h>
#include <glib/gprintf.h>
#include <pthread.h>
#include <unistd.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>

MERCURY_GEN_PROC(JMercuryRPCOut, ((int32_t)(ret)))
MERCURY_GEN_PROC(JMercuryRPCIn, ((int32_t)(input_val))((hg_bulk_t)(bulk_handle)))

typedef struct JMercuryConnection
{
	/**
	 * The class.
	 * This holds the Mercury class.
	 **/
	hg_class_t* class;

	/**
	 * The context.
	 **/
	hg_context_t* context;

	/**
	 * The shutdown-flag.
	 * This terminates a running connection, if its set to TRUE.
	 **/
	gboolean shutdown_flag;

	/**
	 * The thread-id.
	 * This thread manages the own connection.
	 **/
	pthread_t thread_id;
} JMercuryConnection;

typedef struct JMercuryRPCState
{
	/**
	 * The RPC handle.
	 * Store the given RPC handle to this field.
	 */
	hg_handle_t handle;

	/**
	 * The RPC bulks handle
	 * This contains the handle for bulk transfers.
	 */
	hg_bulk_t bulk_handle;

	/**
	 * The size.
	 * Contains the size of target buffer for bulk transfer.
	 **/
	hg_size_t size;

	/**
	 * The buffer.
	 * Pointer of the buffer for bulk transfer.
	 **/
	void* buffer;

	union
	{
		/**
		 * The RPC input.
		 * A struct containing data from RPC call.
		 */
		JMercuryRPCIn in;

		/**
		 * The value
		 */
		gint value;
	};
} JMercuryRPCState;

JMercuryConnection* j_mercury_init (const char*, gboolean);
void j_mercury_final (JMercuryConnection*);
void j_mercury_lookup (JMercuryConnection*, const char*, hg_cb_t, void*);
void j_mercury_create_handle (JMercuryConnection*, na_addr_t, hg_id_t, hg_handle_t*);
static void* j_mercury_process (void*);
hg_id_t j_mercury_rpc_register (JMercuryConnection*);
static hg_return_t j_mercury_rpc_target (hg_handle_t);
static hg_return_t j_mercury_rpc_bulk_handler (const struct hg_cb_info*);

#endif
