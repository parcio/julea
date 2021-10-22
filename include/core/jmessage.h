/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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

#ifndef JULEA_MESSAGE_H
#define JULEA_MESSAGE_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gio/gio.h>

G_BEGIN_DECLS

/** \defgroup JMessage Message
 * Sends message between server and client
 */
/** \public \memberof JMessage
 *	\sa j_message_new, j_message_get_type
 */
enum JMessageType
{
	J_MESSAGE_NONE,
	J_MESSAGE_PING,
	J_MESSAGE_STATISTICS,
	J_MESSAGE_OBJECT_CREATE,
	J_MESSAGE_OBJECT_DELETE,
	J_MESSAGE_OBJECT_GET_ALL,
	J_MESSAGE_OBJECT_GET_BY_PREFIX,
	J_MESSAGE_OBJECT_READ,
	J_MESSAGE_OBJECT_STATUS,
	J_MESSAGE_OBJECT_SYNC,
	J_MESSAGE_OBJECT_WRITE,
	J_MESSAGE_KV_PUT,
	J_MESSAGE_KV_DELETE,
	J_MESSAGE_KV_GET,
	J_MESSAGE_KV_GET_ALL,
	J_MESSAGE_KV_GET_BY_PREFIX,
	J_MESSAGE_DB_SCHEMA_CREATE,
	J_MESSAGE_DB_SCHEMA_GET,
	J_MESSAGE_DB_SCHEMA_DELETE,
	J_MESSAGE_DB_INSERT,
	J_MESSAGE_DB_UPDATE,
	J_MESSAGE_DB_DELETE,
	J_MESSAGE_DB_QUERY
};
typedef enum JMessageType JMessageType;

/** \class JMessage lib/jmessage.h
 * 	\ingroup network JMessage
 */
struct JMessage;
typedef struct JMessage JMessage;

G_END_DECLS

#include <core/jsemantics.h>
#include <core/jnetwork.h>

G_BEGIN_DECLS

/// Creates a new Message
/** \public \memberof JMessage
 * 	\param op_type[in] message type 
 *  \param length[in]  message body size.
 *  \return A new message. Should be freed with j_message_unref().
 **/
JMessage* j_message_new(JMessageType op_type, gsize length);

/// Creates a new reply message
/** \public \memberof JMessage
 *  \param message[in] message to reply to 
 *  \return A new reply message. Should be freed with j_message_unref().
 **/
JMessage* j_message_new_reply(JMessage* message);

/// Increases a message's reference count.
/** \public \memberof JMessage
 *  \return this
 *  \sa j_message_unref
 **/
JMessage* j_message_ref(JMessage* this);

/// Decreases a message's reference count.
/** When the reference count reaches zero, frees the memory allocated for the message.
 *  \public \memberof JMessage
 *  \sa j_message_ref
 */
void j_message_unref(JMessage* this);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JMessage, j_message_unref)

/// Returns a message's type.
/** \public \memberof JMessage
 *  \return The message's operation type.
 */
JMessageType j_message_get_type(JMessage const* this);

/// Returns a message's operation count.
/** \public \memberof JMessage
  */
guint32 j_message_get_count(JMessage const* this);

/// Appends 1 byte to a message.
/** \public \memberof JMessage
 *  \param data    Data to append.
 *  \retval TRUE on success.
 **/
gboolean j_message_append_1(JMessage* this, gconstpointer data);

/// Appends 4 byte to a message.
/** \public \memberof JMessage
 *  \param data    Data to append.
 *  \retval TRUE on success.
 **/
gboolean j_message_append_4(JMessage* this, gconstpointer data);

/// Appends 8 byte to a message.
/** \public \memberof JMessage
 *  \param data    Data to append.
 *  \retval TRUE on success.
 **/
gboolean j_message_append_8(JMessage* this, gconstpointer data);

///Appends a number of bytes to a message.
/** \public \memberof JMessage
 * \code
 * int data[32];
 * ...
 * j_message_append_n(message, data, 32);
 * \endcode
 *
 * \param data    Data to append.
 * \param length  Length of data.
 * \retval TRUE on success.
 * \sa j_message_append_string, j_message_append_memory_id
 **/
gboolean j_message_append_n(JMessage* message, gconstpointer data, gsize length);

/// Appends a string to a message.
/** \public \memberof JMessage
 * \code
 * gchar* str = "Hello world!";
 * ...
 * j_message_append_string(message, str);
 * \endcode
 *
 * \param str     String to append.
 * \retval TRUE on success.
 **/
gboolean j_message_append_string(JMessage* this, gchar const* str);

/// Appends a memory identifier to a message
/** \public \memberof JMessage
 * \retval TRUE on success.
 */
gboolean j_message_append_memory_id(JMessage* this, const struct JConnectionMemoryID* memory_id);

/// Gets 1 byte from a message.
/** \public \memberof JMessage */
gchar j_message_get_1(JMessage* this);

/// Gets 4 bytes from a message.
/** The bytes are converted from little endian automatically.
 * \public \memberof JMessage
 * \attention byte order depends on endian on system.
 * \return 4 byte integer containing data
 **/
gint32 j_message_get_4(JMessage* this);

/// Gets 8 bytes from a message.
/** The bytes are converted from little endian automatically.
 * \public \memberof JMessage
 * \attention byte order depends on endian
 * \return An 8-bytes integer containing data.
 **/
gint64 j_message_get_8(JMessage* this);

/// Gets n bytes from a message.
/** \public \memberof JMessage
 * \param length  Number of bytes to get.
 * \attention the data are still owned and managed by the message!
 * \return A pointer to the data.
 **/
gpointer j_message_get_n(JMessage* this, gsize length);

/// Gets a string from a message.
/** \public \memberof JMessage
 * \attention the string is still owned and managed by the message!
 * \return A string.
 **/
gchar const* j_message_get_string(JMessage* this);

/// Gets an memory identifier from a message.
/** \public \memberof JMessage
 * \attention The memory is still owned and managed by the message!
 */
const struct JConnectionMemoryID* j_message_get_memory_id(JMessage*);

/// Add new data block to send with message.
/** \public \memberof JMessage
 * \param data,length Data segment to send.
 * \param header,h_size header data (included in message)
 **/
void j_message_add_send(JMessage* this, gconstpointer data, guint64 length, void* header, guint64 h_size);

/// Adds a new operation to a message.
/** \public \memberof JMessage
 *  \remark Operation data must appended with the j_message_append functions.
 *  \param length length of operation.
 *  \sa j_message_append_1, j_message_append_4, j_message_append_8, j_message_append_n, j_message_append_string, j_message_append_memory_id.
 **/
void j_message_add_operation(JMessage* this, gsize length);

/// Sends message via connection
/** \public \memberof JMessage
 * \remark append one MemoryID for each data segment send with the message
 * \retval TRUE on success.
 */
gboolean j_message_send(JMessage* this, struct JConnection* connection);

/// Reads a message from connection.
/* \private \memberof JMessage
 * \retval TRUE on success.
 * \public \memberof JMessage
 **/
gboolean j_message_receive(JMessage* this, struct JConnection* connection);

/// signal when rma read actions are finished.
/** Sends ACK flag, to signal that host can free data memory. Also wait for all network actions to complete!
 *	\param message checks if message has memory regions, only then sends ack. Can be NULL to force ack sending
 *	\retval TRUE on success
 */
gboolean j_message_send_ack(JMessage* this, struct JConnection* connection);

///Reads a message from the network.
/** \public \memberof
 * \retval TRUE on success.
 * \attention for internal usage only, please use: j_message_receive()
 **/
gboolean j_message_read(JMessage* this, struct JConnection* connection);

/// Writes a message to the network.
/**\private \memberof JMessage
 * \return TRUE on success, FALSE if an error occurred.
 * \attention for internal usage only, please use: j_message_send()
 **/
gboolean j_message_write(JMessage* this, struct JConnection* connection);

/// Set the semantics of a message.
/** \public \memberof JMessage */
void j_message_set_semantics(JMessage* this, JSemantics* semantics);

/// get the semantics of a message.
/** \public \memberof JMessage */
JSemantics* j_message_get_semantics(JMessage* this);

G_END_DECLS

#endif
