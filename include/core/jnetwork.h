/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021 Julian Benda
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

/// \todo update to JULEA style

#ifndef JULEA_NETWORK_H
#define JULEA_NETWORK_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gio/gio.h>

//#include <rdma/fabric.h>

G_BEGIN_DECLS

/**
 * \defgroup JNetwork Network
 *
 * @{
 **/

/**
 * A paired connection over a network.
 * Connections are used to send data from server to client or vis-a-vis.
 * They support usage for:
 * * Messaging: Easy to serialize and verify.
 * * Direct memory read: Sender must "protect memory" until receiver verifies completion.
 *
 * \par Example for client-side connection usage
 * Success of each function has to be checked individually; omitted to reduce the code length.
 *
 * \code
 * JNetworkConnection* connection;
 * int ack;
 * int data_size = 12, rdata_size = 16;
 * void* data_a = malloc(data_size), data_b = malloc(data_size), rdata = malloc(rdata_size);
 * // fill data !
 *
 * j_network_connection_init_client(config, J_BACKEND_TYPE_OBJECT, &connection);
 *
 * // message exchange
 * j_network_connection_recv(connection, rdata, rdata_size);
 * j_network_connection_wait_for_completion(connection);
 * // handle message and write result in data
 * j_network_connection_send(connection, data_a, data_size);
 * j_network_connection_send(connection, data_b, data_size);
 * j_network_connection_wait_for_completion(connection);
 *
 * // provide memory for rma read action of other party.
 * JNetworkConnectionMemory rma_mem;
 * JNetworkConnectionMemoryID rma_mem_id;
 * j_network_connection_rma_register(connection, data, data_size, &rma_mem);
 * j_network_connection_memory_get_id(rma_mem, &rma_mem_id);
 * j_network_connection_send(connection, &rma_mem_id, sizeof(rma_mem_id));
 * // wait for other party to signal that they finished reading
 * j_network_connection_recv(connection, &ack, sizeof(ack));
 * j_network_connection_wait_for_completion(connection);
 * j_network_connection_rma_unregister(connection, &rma_mem);
 *
 * // rma read
 * j_network_connection_recv(connection, &rma_mem_id, sizeof(rma_mem_id));
 * j_network_connection_wait_for_completion(connection);
 * j_network_connection_rma_read(connection, &rma_mem_id, data);
 * j_network_connection_wait_for_completion(connection);
 *
 * j_network_connection_fini(connection);
 * \endcode
 *
 * \par Example for server-side connection usage
 * Success of each function has to be checked individually; omitted to reduce the code length.
 * This code should be placed in the TCP server connection request handler.
 *
 * \code
 * JConfiguration* config; JNetworkFabric* fabric; // are probably initialized
 * GSocketConnection* gconnection = [>newly established g_socket connection<];
 * JNetworkConnection* jconnection;
 * j_network_connection_init_server(config, fabric, gconnection, &jconnection);
 *
 * // same communication code then client side
 *
 * j_network_connection_fini(jconnection);
 * \endcode
 *
 * \todo expose more connection possibilities (e.g. connection with a JNetworkFabricAddr)
 */
struct JNetworkConnection;

typedef struct JNetworkConnection JNetworkConnection;

/**
 * Acknowledgment value.
 * Value used to send an ack for any reason.
 * May be used to verify that the remote memory was successful read.
 **/
enum JNetworkConnectionAck
{
	J_NETWORK_CONNECTION_ACK = 42
};

typedef enum JNetworkConnectionAck JNetworkConnectionAck;

/**
 * Handle for memory regions available via RMA.
 **/
struct JNetworkConnectionMemory
{
	struct fid_mr* memory_region;
	guint64 addr;
	guint64 size;
};

typedef struct JNetworkConnectionMemory JNetworkConnectionMemory;

/**
 * Identifier to read memory.
 **/
struct JNetworkConnectionMemoryID
{
	guint64 key; ///< access key for authentication
	guint64 offset; ///< used to identifie memory region
	guint64 size; ///< size in bytes of memery region
};

typedef struct JNetworkConnectionMemoryID JNetworkConnectionMemoryID;

/**
 * Manage access to network.
 * Mandatory to communicate via the network and to initialize connections.
 * Also handles unpaired communication (connection requests).
 **/
struct JNetworkFabric;

typedef struct JNetworkFabric JNetworkFabric;

G_END_DECLS

#include <core/jbackend.h>
#include <core/jconfiguration.h>

G_BEGIN_DECLS

/**
 * Initializes a fabric for the server side.
 * In difference to j_network_fabric_init_client, the resulting fabric is capable of reciving connection requests.
 * Therefore, events can be read via j_fabric_sread_event.
 *
 * \param[in] configuration A configuration.
 * \param[out] instance A pointer to the resulting fabric.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_network_fabric_init_server(JConfiguration* configuration, JNetworkFabric** instance);

/// Get identifier of memory region
/** \public \memberof JNetworkConnectionMemory
 * \retval FALSE on failure
 * \sa j_network_connection_rma_read, j_network_connection_rma_register, j_network_connection_rma_unregister
 */
gboolean
j_network_connection_memory_get_id(JNetworkConnectionMemory* this,
			   JNetworkConnectionMemoryID* id ///< [out] of registerd memory
);

/// Builds a connection to an active fabric (direct).
/** \public \memberof JNetworkConnection
 *
 * This constructor will fetch the fabric address via TCP and then building an paired
 * connection to this, which later allows for messaging and RMA data transfer.  \n
 * This will also construct a fabric for that connection, which is freed when the connection is finished.
 *
 * \attention this function may reduces j_configuration_max_operation_size()
 * accordingly to network capabilities.
 *
 * \msc "Connection process"
 * Client, Server;
 * Client => Server [label="g_socket_client_connect_to_host(...)"];
 * Server => Client [label="JNetworkFabricAddr"];
 * Client => Server [label="g_socket_close(...)"];
 * Client => Server [label="J_FABRIC_EVENT_CONNECTION_REQUEST"];
 * Server => Client [label="J_CONNECTION_EVENT_CONNECTED"];
 * Server => Server [label="J_CONNECTION_EVENT_CONNECTED"];
 * \endmsc
 * \retval FALSE if building a connection failed
 * \sa j_network_connection_init_server
 * \todo document index parameter!
 */
gboolean
j_network_connection_init_client(
	JConfiguration* configuration, ///< [in] for server address, and network configuration
	JBackendType backend, ///< [in] backend server to connect to
	guint index, ///< [in] ??
	JNetworkConnection** instance ///< [out] constructed instance
);

/// Establish connection to client based on established GSocketConnection.
/** \public \memberof JNetworkConnection
 *
 * The GSocketConnection will be used to send the server fabric data.
 * For the connection process see j_network_connection_init_client().
 *
 * \attention this function may reduces j_configuration_max_operation_size()
 * accordingly to network capabilities.
 *
 * \retval FALSE if establishing the connection failed
 * \sa j_network_connection_init_client
 */
gboolean
j_network_connection_init_server(
	JNetworkFabric* fabric, ///< [in] via which the connection should be established
	GSocketConnection* gconnection, ///< [in] valid GSocketConnection for address exchange
	JNetworkConnection** instance ///< [out] constructed instance
);
/// Closes a connection and free all related resources.
/** \public \memberof JNetworkConnection
 *
 * If this an client side connection with private fabric, it will also clear the fabric.
 * \retval FALSE if closing the connection failed. The connection will still be unusable!
 */
gboolean
j_network_connection_fini(JNetworkConnection* this);

/// Async send data via MSG connection
/** \public \memberof JNetworkConnection
 *
 * Asynchronous sends a message, to recognize for completion use j_network_connection_wait_for_completion().\n
 * If the message is small enough it can "injected" to the network, in that case the actions finished
 * immediate (j_network_connection_wait_for_completion() still works).
 *
 * \todo feedback if message was injected!
 *
 * \attention it is only allowed to have J_CONNECTION_MAX_SEND send
 * operation pending at the same time. Each has a max size
 * of j_configuration_max_operation_size() (the connection initialization may changes this value!).
 *
 * \retval FALSE if an error occurred.
 * \sa j_network_connection_recv, j_network_connection_wait_for_completion
 */
gboolean
j_network_connection_send(JNetworkConnection* this,
		  gconstpointer data, ///< [in] to send
		  gsize data_len ///< [in] in bytes
);

/// Asynchronous receive data via MSG connection.
/** \public \memberof JNetworkConnection
 *
 * Asynchronous receive a message, to wait for completion use j_network_connection_wait_for_completion().
 *
 * \attention it is only allowed to have J_CONNECTION_MAX_RECV recv
 * operation pending at the same time.  Each has a max size
 * of j_configuration_max_operation_size() (the connection initialization may has changed this value!).
 *
 * \retval FALSE if an error occurred
 * \sa j_network_connection_send, j_network_connection_wait_for_completion
 */
gboolean
j_network_connection_recv(JNetworkConnection* this,
		  gsize data_len, ///< [in] in bytes to receive
		  gpointer data ///< [out] received
);

/// Async direct memory read.
/** \public \memberof JNetworkConnection
 *
 * Initiate an direct memory read, to wait for completion use j_network_connection_wait_for_completion().
 * \retval FALSE if an error occurred -> handle will then also invalid
 * \todo evaluate if paralisation possible
 */
gboolean
j_network_connection_rma_read(JNetworkConnection* this,
		      JNetworkConnectionMemoryID const* memoryID, ///< [in] for segment which should be copied
		      gpointer data ///< [out] received
);

/// Wait until operations initiated at his connection finished.
/** \public \memberof JNetworkConnection
 * \retval FALSE if waiting finished. This may occures because the connection was closed: check this with: j_connection_active()
 * \sa j_network_connection_rma_read, j_network_connection_send, j_network_connection_recv
 */
gboolean
j_network_connection_wait_for_completion(JNetworkConnection* this);

/// Check if the connection was closed from the other party.
/** \sa j_network_connection_wait_for_completion */
gboolean
j_network_connection_closed(JNetworkConnection* this);

/// Register memory to make it rma readable.
/** \public \memberof JNetworkConnection
 * Memory access rights must changed to allow for an rma read of other party.
 * There for before an j_network_connection_rma_read() can succeed the provider of the data must
 * register the memory first!
 */
gboolean
j_network_connection_rma_register(JNetworkConnection* this,
			  gconstpointer data, ///< [in] begin of memory region to share
			  gsize data_len, ///< [in] size of memory region in bytes
			  JNetworkConnectionMemory* handle ///< [out] for memory region to unregister with j_network_connection_rma_unregister
);

/// Unregister memory from rma availablity.
/** \public \memberof JNetworkConnection
 * Counterpart to j_network_connection_rma_register().
 */
gboolean
j_network_connection_rma_unregister(JNetworkConnection* this,
			    JNetworkConnectionMemory* handle ///< [in] for memory region to unregister
);

/**
 * @}
 **/

G_END_DECLS

#endif
