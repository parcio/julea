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

/**
 * \file
 **/

#ifndef JULEA_MESSAGE_H
#define JULEA_MESSAGE_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gio/gio.h>

G_BEGIN_DECLS

/**
 * \defgroup JMessage Message
 *
 * @{
 **/

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

struct JMessage;
struct JConnection;

typedef struct JMessage JMessage;

G_END_DECLS

#include <core/jsemantics.h>

G_BEGIN_DECLS

/**
 * Creates a new message.
 *
 * \code
 * \endcode
 *
 * \param op_type An operation type.
 * \param length  A length.
 *
 * \return A new message. Should be freed with j_message_unref().
 **/
JMessage* j_message_new(JMessageType op_type, gsize length);

/**
 * Creates a new reply message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return A new reply message. Should be freed with j_message_unref().
 **/
JMessage* j_message_new_reply(JMessage* message);

/**
 * Increases a message's reference count.
 *
 * \code
 * JMessage* m;
 *
 * j_message_ref(m);
 * \endcode
 *
 * \param message A message.
 *
 * \return \p message.
 **/
JMessage* j_message_ref(JMessage* message);

/**
 * Decreases a message's reference count.
 * When the reference count reaches zero, frees the memory allocated for the message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 **/
void j_message_unref(JMessage* message);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JMessage, j_message_unref)

/**
 * Returns a message's type.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return The message's operation type.
 **/
JMessageType j_message_get_type(JMessage const* message);

/**
 * Returns a message's count.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return The message's operation count.
 **/
guint32 j_message_get_count(JMessage const* message);

/**
 * Appends 1 byte to a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param data    Data to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_append_1(JMessage* message, gconstpointer data);

/**
 * Appends 4 bytes to a message.
 * The bytes are converted to little endian automatically.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param data    Data to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_append_4(JMessage* message, gconstpointer data);

/**
 * Appends 8 bytes to a message.
 * The bytes are converted to little endian automatically.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param data    Data to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_append_8(JMessage* message, gconstpointer data);

/**
 * Appends a number of bytes to a message.
 *
 * \code
 * gchar* str = "Hello world!";
 * ...
 * j_message_append_n(message, str, strlen(str) + 1);
 * \endcode
 *
 * \param message A message.
 * \param data    Data to append.
 * \param length  Length of data.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_append_n(JMessage* message, gconstpointer data, gsize length);

/**
 * Appends a string to a message.
 *
 * \code
 * gchar* str = "Hello world!";
 * ...
 * j_message_append_string(message, str);
 * \endcode
 *
 * \param message A message.
 * \param str     String to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_append_string(JMessage* message, gchar const* str);

/**
 * Gets 1 byte from a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return A character.
 **/
gchar j_message_get_1(JMessage* message);

/**
 * Gets 4 bytes from a message.
 * The bytes are converted from little endian automatically.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return A 4-bytes integer.
 **/
gint32 j_message_get_4(JMessage* message);

/**
 * Gets 8 bytes from a message.
 * The bytes are converted from little endian automatically.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return An 8-bytes integer.
 **/
gint64 j_message_get_8(JMessage* message);

/**
 * Gets n bytes from a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  Number of bytes to get.
 *
 * \return A pointer to the data.
 **/
gpointer j_message_get_n(JMessage* message, gsize length);

/**
 * Gets a string from a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return A string.
 **/
gchar const* j_message_get_string(JMessage* message);

/**
 * Adds new data to send to a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param data    Data.
 * \param length  A length.
 **/
void j_message_add_send(JMessage* message, gconstpointer data, guint64 length);

/**
 * Adds a new operation to a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  A length.
 **/
void j_message_add_operation(JMessage* message, gsize length);

/**
 * Writes a message to the network.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param stream  A network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_send(JMessage*, struct JConnection*);

/**
 * Reads a message from the network.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param stream  A network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_receive(JMessage*, struct JConnection*);

/**
 * Reads a message from the network.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param stream  A network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_read(JMessage*, struct JConnection*);

/**
 * Writes a message to the network.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param stream  A network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_message_write(JMessage*, struct JConnection*);

/**
 * Set the semantics of a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param semantics  A semantics object.
 **/
void j_message_set_semantics(JMessage* message, JSemantics* semantics);

/**
 * get the semantics of a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return A semantics object.
 **/
JSemantics* j_message_get_semantics(JMessage* message);

/**
 * @}
 **/

G_END_DECLS

#endif
