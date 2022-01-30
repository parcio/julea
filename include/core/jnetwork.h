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

#include <core/jbackend.h>

struct JConfiguration;
struct fi_eq_cm_entry;

/** \defgroup network Network
 * 	Interface for communication over network
 */

/** \struct JFabricConnectionRequest lib/jnetwork.h
 *  \brief A connection request read from an event queue.
 *
 * Connection request are mendetory to establish a connection from server side.
 * \sa j_connection_init_server, j_fabric_sread_event
 */
typedef struct fi_eq_cm_entry JFabricConnectionRequest;

/** \struct JFabricAddr lib/jnetwork.h
 *  \public \memberof JFabric
 *  \brief Fabrics address data needed to send an connection request.
 *  \sa j_fabric_init_client
 */
struct JFabricAddr;
typedef struct JFabricAddr JFabricAddr;

/// Possible events for paired connections
/** \ingroup network
 *  \sa j_connection_sread_event, j_connection_read_event
 */
typedef enum
{
	J_CONNECTION_EVENT_ERROR = 0, ///< An error was reported, the connection is probably in an invalid state!
	J_CONNECTION_EVENT_TIMEOUT, ///< there was no event to read in the given time frame
	J_CONNECTION_EVENT_CONNECTED, ///< First event after successful established connection.
	J_CONNECTION_EVENT_SHUTDOWN ///< connection was closed
} JConnectionEvents;

/// Possible events for listening fabrics
/** \public \memberof JFabric
 *  \sa j_fabric_sread_event
 */
typedef enum
{
	J_FABRIC_EVENT_ERROR = 0, ///< An error was reported, fabric is probably in a invalid state!
	J_FABRIC_EVENT_TIMEOUT, ///< No event received in the given time frame.
	J_FABRIC_EVENT_CONNECTION_REQUEST, ///< A connection request was received.
	J_FABRIC_EVENT_SHUTDOWN ///< fabric socket was closed
} JFabricEvents;

/** \class JFabric lib/jnetwork.h
 *  \brief Manage access to network.
 *
 * Mandetory to communicate via a network and to initelize connections.
 * Also handles unpaired communication (connection requests!)
 * \ingroup network
 */
struct JFabric;
typedef struct JFabric JFabric;

/// Initelize a fabric for the server side.
/** \public \memberof JFabric
 *
 * In differents to j_fabric_init_client(), the resulting fabric is capable of reciving connection requests.
 * There fore events can be readed via j_fabric_sread_event()
 * \retval FALSE on failure
 */
gboolean
j_fabric_init_server(
	struct JConfiguration* configuration, ///< [in] of JULEA, to fetch details
	struct JFabric** instance ///< [out] pointer to resulting fabric
);

/// Initelize a fabric for the client side.
/** \public \memberof JFabric
 *
 * Contains data to building a paired connection.
 * Addess of JULEA server is needed, to enforce that both communicate via the same network.
 * \warning will be called from j_connection_init_client(), so no need for explicit a call
 * \retval FALSE on failure
 */
gboolean
j_fabric_init_client(
	struct JConfiguration* configuration, ///< [in] of JULEA to fetch details
	struct JFabricAddr* addr, ///< [in] of JULEA server fabric
	struct JFabric** instance ///< [out] pointer to resulting fabric
);

/// Closes a fabirc and frees used memory.
/** \public \memberof JFabric
 *  \retval FALSE on failure
 *  \pre Finish all connections created from this fabric!
 */
gboolean
j_fabric_fini(struct JFabric* this);

/// Read a event of a listening fabric.
/** \public \memberof JFabric
 * \warning only usable for fabrics initelized with j_fabric_init_server()
 * \retval TRUE if fetching succeed or the request timeouted
 * \retval FALSE if fetching an event fails
 */
gboolean
j_fabric_sread_event(struct JFabric* this,
		     int timeout, ///< [in] set to -1 for no timeout.
		     JFabricEvents* event, ///< [out] reeded from event queue
		     JFabricConnectionRequest* con_req ///< [out] contains connection request,
		     /// if event == J_FABRIC_EVENT_CONNECTION_REQUEST
);

/** \class JConnection jnetwork.h
 *  \brief A paired connection over a network.
 *
 * Connections are used to send data from server to client or vis a vi.
 * They support usage for:
 * * messaging: easy to serialize and verify
 * * direct memory read: sender must "protect memory" until receiver verifies completion.
 * \ingroup network
 * \par Example for client side connection usage.
 * You must check the success of each function individual, we spare this to reduce the code length!
 * \code
 * struct JConnection* connection;
 * int ack;
 * int data_size = 12, rdata_size = 16;
 * void* data_a = malloc(data_size), data_b = malloc(data_size), rdata = malloc(rdata_size);
 * // fill data !
 *
 * j_connection_init_client(config, J_BACKEND_TYPE_OBJECT, &connection);
 *
 * // message exchange
 * j_connection_recv(connection, rdata, rdata_size);
 * j_connection_wait_for_completion(connection);
 * // handle message and write result in data
 * j_connection_send(connection, data_a, data_size);
 * j_connection_send(connection, data_b, data_size);
 * j_connection_wait_for_completion(connection);
 *
 * // provide memory for rma read action of other party.
 * JConnectionMemory rma_mem;
 * struct JConnectionMemoryID rma_mem_id;
 * j_connection_rma_register(connection, data, data_size, &rma_mem);
 * j_connection_memory_get_id(rma_mem, &rma_mem_id);
 * j_connection_send(connection, &rma_mem_id, sizeof(rma_mem_id));
 * // wait for other party to signal that they finished reading
 * j_connection_recv(connection, &ack, sizeof(ack));
 * j_connection_wait_for_completion(connection);
 * j_connection_rma_unregister(connection, &rma_mem);
 *
 * // rma read
 * j_connection_recv(connection, &rma_mem_id, sizeof(rma_mem_id));
 * j_connection_wait_for_completion(connection);
 * j_connection_rma_read(connection, &rma_mem_id, data);
 * j_connection_wait_for_completion(connection);
 *
 * j_connection_fini(connection);
 * \endcode
 *
 * \par Establish a connection from server side
 * You must check the success of each function individual, we spare this to reduce the code length!
 * This code should be placed in the TCP server connection request handler.
 * \code
 * JConfiguration* config; JFabric* fabric; // are probably initialized
 * GSocketConnection* gconnection = [>newly established g_socket connection<];
 * JConnection* jconnection;
 * j_connection_init_server(config, fabric, gconnection, &jconnection);
 *
 * // same communication code then client side
 *
 * j_connection_fini(jconnection);
 * \endcode
 * \todo expose more connection posibilities (eg. connection with an JFabricAddr)
 */
struct JConnection;

/// Highest number of j_connection_send() calls before a j_connection_wait_for_completion().
/** \public \memberof JConnection */
#define J_CONNECTION_MAX_SEND 2

/// Highest number of j_connection_recv() calls before a j_connection_wait_for_completion().
/** \public \memberof JConnection */
#define J_CONNECTION_MAX_RECV 1

/// Acknowladgement value.
/** \public \memberof JConnection
 * Value used to send an ack for any reason. Maybe to verify that the remote memory was successful readed.
 * \sa j_connection_rma_unregister, j_connection_rma_read, j_connection_rma_register, JConnectionAck
 */
#define J_CONNECTION_ACK 42

/// type of ack value.
/** \public \memberof JConnection
 * \sa J_CONNECTION_ACK
 */
typedef int JConnectionAck;

/// Handle for memory regions available via rma.
/** \public \memberof JConnection
 * \sa j_connection_rma_read, j_connection_rma_register, j_connection_rma_unregister
 */
struct JConnectionMemory;

/// Identifier to read memory.
/** \public \memberof JConnectionMemory
 * \sa j_connection_rma_read, j_connection_rma_register, j_connection_memory_get_id
 */
struct JConnectionMemoryID
{
	uint64_t key; ///< access key for authentication
	uint64_t offset; ///< used to identifie memory region
	uint64_t size; ///< size in bytes of memery region
};

/// Get identifier of memory region
/** \public \memberof JConnectionMemory
 * \retval FALSE on failure
 * \sa j_connection_rma_read, j_connection_rma_register, j_connection_rma_unregister
 */
gboolean
j_connection_memory_get_id(struct JConnectionMemory* this,
			   struct JConnectionMemoryID* id ///< [out] of registerd memory
);

/// Builds a connection to an active fabric (direct).
/** \public \memberof JConnection
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
 * Server => Client [label="JFabricAddr"];
 * Client => Server [label="g_socket_close(...)"];
 * Client => Server [label="J_FABRIC_EVENT_CONNECTION_REQUEST"];
 * Server => Client [label="J_CONNECTION_EVENT_CONNECTED"];
 * Server => Server [label="J_CONNECTION_EVENT_CONNECTED"];
 * \endmsc
 * \retval FALSE if building a connection failed
 * \sa j_connection_init_server
 * \todo document index parameter!
 */
gboolean
j_connection_init_client(
	struct JConfiguration* configuration, ///< [in] for server address, and network configuration
	enum JBackendType backend, ///< [in] backend server to connect to
	guint index, ///< [in] ??
	struct JConnection** instance ///< [out] constructed instance
);

/// Establish connection to client based on established GSocketConnection.
/** \public \memberof JConnection
 *
 * The GSocketConnection will be used to send the server fabric data.
 * For the connection process see j_connection_init_client().
 *
 * \attention this function may reduces j_configuration_max_operation_size()
 * accordingly to network capabilities.
 *
 * \retval FALSE if establishing the connection failed
 * \sa j_connection_init_client
 */
gboolean
j_connection_init_server(
	struct JFabric* fabric, ///< [in] via which the connection should be established
	GSocketConnection* gconnection, ///< [in] valid GSocketConnection for address exchange
	struct JConnection** instance ///< [out] constructed instance
);
/// Closes a connection and free all related resources.
/** \public \memberof JConnection
 *
 * If this an client side connection with private fabric, it will also clear the fabric.
 * \retval FALSE if closing the connection failed. The connection will still be unusable!
 */
gboolean
j_connection_fini(struct JConnection* this);

/// Check if the connection is still valid.
/** \public \memberof JConnection
 *
 * Check if the connection was established and if was no J_CONNECTION_EVENT_SHUTDOWN recognized.
 * \attention events will only be checked if j_connection_read_event() is called!.
 * \retval FALSE if connection is not longer valid
 * \retval TRUE if connection was created and no shutdown event was reported.
 * \todo advance connection checking!
 */
gboolean
j_connection_check_connection(struct JConnection* this);

/// blocking read one event from connection
/** \public \memberof JConnection
 * Used to check wait for connected state, and recognized errors and shutdown signals.
 * For a version which returns immediate when no event exists see j_connection_read_event()
 * \retval TRUE if fetching succeed or the request timeouted
 * \retval FALSE if fetching event fails
 */
gboolean
j_connection_sread_event(struct JConnection* this,
			 int timeout, ///< [in] set to -1 for no timeout
			 JConnectionEvents* event ///< [out] reeded from queue
);

/// check for event on connection
/** \public \memberof JConnection
 * Returns a J_CONNECTION_EVENT_TIMEOUT event if no event is ready to report.
 * \retval TRUE if fetching succeed or no events to record
 * \retval FALSE if fetching failed
 */
gboolean
j_connection_read_event(struct JConnection* this,
			JConnectionEvents* event ///< [out] reeded from queue
);

/// Async send data via MSG connection
/** \public \memberof JConnection
 *
 * Asynchronous sends a message, to recognize for completion use j_connection_wait_for_completion().\n
 * If the message is small enough it can "injected" to the network, in that case the actions finished
 * immediate (j_connection_wait_for_completion() still works).
 *
 * \todo feedback if message was injected!
 *
 * \attention it is only allowed to have J_CONNECTION_MAX_SEND send
 * operation pending at the same time. Each has a max size
 * of j_configuration_max_operation_size() (the connection initialization may changes this value!).
 *
 * \retval FALSE if an error occurred.
 * \sa j_connection_recv, j_connection_wait_for_completion
 */
gboolean
j_connection_send(struct JConnection* this,
		  const void* data, ///< [in] to send
		  size_t data_len ///< [in] in bytes
);

/// Asynchronous receive data via MSG connection.
/** \public \memberof JConnection
 *
 * Asynchronous receive a message, to wait for completion use j_connection_wait_for_completion().
 *
 * \attention it is only allowed to have J_CONNECTION_MAX_RECV recv
 * operation pending at the same time.  Each has a max size
 * of j_configuration_max_operation_size() (the connection initialization may has changed this value!).
 *
 * \retval FALSE if an error occurred
 * \sa j_connection_send, j_connection_wait_for_completion
 */
gboolean
j_connection_recv(struct JConnection* this,
		  size_t data_len, ///< [in] in bytes to receive
		  void* data ///< [out] received
);

/// Async direct memory read.
/** \public \memberof JConnection
 *
 * Initiate an direct memory read, to wait for completion use j_connection_wait_for_completion().
 * \retval FALSE if an error occurred -> handle will then also invalid
 * \todo evaluate if paralisation possible
 */
gboolean
j_connection_rma_read(struct JConnection* this,
		      const struct JConnectionMemoryID* memoryID, ///< [in] for segment which should be copied
		      void* data ///< [out] received
);

/// Wait until operations initiated at his connection finished.
/** \public \memberof JConnection
 * \retval FALSE if waiting finished. This may occures because the connection was closed: check this with: j_connection_active()
 * \sa j_connection_rma_read, j_connection_send, j_connection_recv
 */
gboolean
j_connection_wait_for_completion(struct JConnection* this);

/// Check if the connection was closed from the other party.
/** \sa j_connection_wait_for_completion */
gboolean
j_connection_closed(struct JConnection* this);

/// Register memory to make it rma readable.
/** \public \memberof JConnection
 * Memory access rights must changed to allow for an rma read of other party.
 * There for before an j_connection_rma_read() can succeed the provider of the data must
 * register the memory first!
 */
gboolean
j_connection_rma_register(struct JConnection* this,
			  const void* data, ///< [in] begin of memory region to share
			  size_t data_len, ///< [in] size of memory region in bytes
			  struct JConnectionMemory* handle ///< [out] for memory region to unregister with j_connection_rma_unregister
);

/// Unregister memory from rma availablity.
/** \public \memberof JConnection
 * Counterpart to j_connection_rma_register().
 */
gboolean
j_connection_rma_unregister(struct JConnection* this,
			    struct JConnectionMemory* handle ///< [in] for memory region to unregister
);

struct JConfiguration*
j_connection_get_configuration(struct JConnection* this);

#endif
