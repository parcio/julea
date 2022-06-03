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
gboolean j_network_fabric_init_server(JConfiguration* configuration, JNetworkFabric** instance);

/**
 * Gets identifier of memory region.
 *
 * \param this A memory region.
 * \param[out] id ID of registered memory.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_network_connection_memory_get_id(JNetworkConnectionMemory* this, JNetworkConnectionMemoryID* id);

/**
 * Builds a direct connection to an active fabric.
 *
 * This constructor will fetch the fabric address via TCP and then build a paired connection to this, which later allows for messaging and RMA data transfer.
 * This will also construct a fabric for that connection, which is freed when the connection is finished.
 *
 * \attention This function may reduce j_configuration_max_operation_size according to network capabilities.
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
 *
 * \param[in] configuration A configuration.
 * \param[in] backend A backend type.
 * \param[in] index An index.
 * \param[out] instance A network connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_network_connection_init_client(JConfiguration* configuration, JBackendType backend, guint index, JNetworkConnection** instance);

/**
 * Establish connection to client based on established GSocketConnection.
 * The GSocketConnection will be used to send the server fabric data.
 * For the connection process see j_network_connection_init_client().
 *
 * \attention This function may reduces j_configuration_max_operation_size according to network capabilities.
 *
 * \param[in] fabric Fabric via which the connection should be established.
 * \param[in] connection GSocketConnection for address exchange.
 * \param[out] connection A new connection.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_network_connection_init_server(JNetworkFabric* fabric, GSocketConnection* gconnection, JNetworkConnection** connection);

/**
 * Closes a connection and free all related resources. *
 * If this an client side connection with private fabric, it will also clear the fabric.
 *
 * \todo params
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_fini(JNetworkConnection* connection);

/**
 * Async send data via MSG connection.
 * Asynchronously sends a message.
 * To recognize completion, use j_network_connection_wait_for_completion().
 * If the message is small enough it can be injected to the network, in that case the actions finishes immediately (j_network_connection_wait_for_completion() still works).
 *
 * \todo feedback if message was injected
 * \todo make data a gconstpointer if possible
 *
 * \attention It is only allowed to have J_CONNECTION_MAX_SEND send operations pending at the same time. Each has a maximum size of j_configuration_max_operation_size() (the connection initialization may change this value).
 *
 * \param[in] connection A connection.
 * \param[in] data A data buffer to send.
 * \param[in] length A length in bytes.
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_send(JNetworkConnection* connection, gpointer data, gsize length);

/**
 * Asynchronously receives data via MSG connection.
 * Asynchronously receive a message.
 * To wait for completion, use j_network_connection_wait_for_completion().
 *
 * \attention It is only allowed to have J_CONNECTION_MAX_RECV receive operations pending at the same time. Each has a maximum size of j_configuration_max_operation_size() (the connection initialization may change this value).
 *
 * \param[in] connection A connection.
 * \param[in] length A length in bytes.
 * \param[out] data A data buffer to receive into.
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_recv(JNetworkConnection* connection, gsize length, gpointer data);

/**
 * Asynchronous direct memory read.
 * Initiate an direct memory read.
 * To wait for completion, use j_network_connection_wait_for_completion().
 *
 * \todo evaluate if paralisation possible
 *
 * \param[in] connection A connection.
 * \param[in] memory_id A memory ID for the segment that should be copied.
 * \param[out] data A data buffer to receive into.
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_rma_read(JNetworkConnection* connection, JNetworkConnectionMemoryID const* memory_id, gpointer data);

/**
 * Waits until operations initiated at his connection finished.
 *
 * \attention FALSE if waiting finished. This may occur if the connection was closed. Check this with j_connection_active().
 *
 * \todo params
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_wait_for_completion(JNetworkConnection* connection);

/**
 * Check if the connection was closed by the other party.
 **/
gboolean j_network_connection_closed(JNetworkConnection* connection);

/**
 * Registers memory to make it RMA-readable.
 * Memory access rights must changed to allow for an RMA read by the other party.
 * Therefore, before a j_network_connection_rma_read() can succeed, the provider of the data must register the memory first!
 *
 * \param[in] connection A connection.
 * \param[in] data A data buffer to share.
 * \param[in] length A length in bytes.
 * \param[out] handle A handle for the memory region to unregister with j_network_connection_rma_unregister().
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_rma_register(JNetworkConnection* connection, gconstpointer data, gsize length, JNetworkConnectionMemory* handle);

/**
 * Unregisters memory from RMA availablity.
 * Counterpart to j_network_connection_rma_register().
 *
 * \param[in] connection A connection.
 * \param[in] handle A handle for the memory region to unregister.
 *
 * \return TRUE on success, FALSE if an error occurred.
 */
gboolean j_network_connection_rma_unregister(JNetworkConnection* connection, JNetworkConnectionMemory* handle);

/**
 * @}
 **/

G_END_DECLS

#endif
