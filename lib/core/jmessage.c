/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <math.h>
#include <string.h>

#include <jmessage.h>

#include <jhelper-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jsemantics.h>
#include <jtrace.h>

#include <glib/gprintf.h>

/**
 * \defgroup JMessage Message
 *
 * @{
 **/

enum JMessageSemantics
{
	J_MESSAGE_SEMANTICS_ATOMICITY_BATCH = 1 << 0,
	J_MESSAGE_SEMANTICS_ATOMICITY_OPERATION = 1 << 1,
	J_MESSAGE_SEMANTICS_ATOMICITY_NONE = 1 << 2,
	J_MESSAGE_SEMANTICS_CONCURRENCY_OVERLAPPING = 1 << 3,
	J_MESSAGE_SEMANTICS_CONCURRENCY_NON_OVERLAPPING = 1 << 4,
	J_MESSAGE_SEMANTICS_CONCURRENCY_NONE = 1 << 5,
	J_MESSAGE_SEMANTICS_CONSISTENCY_IMMEDIATE = 1 << 6,
	J_MESSAGE_SEMANTICS_CONSISTENCY_EVENTUAL = 1 << 7,
	J_MESSAGE_SEMANTICS_CONSISTENCY_NONE = 1 << 8,
	J_MESSAGE_SEMANTICS_ORDERING_STRICT = 1 << 9,
	J_MESSAGE_SEMANTICS_ORDERING_SEMI_RELAXED = 1 << 10,
	J_MESSAGE_SEMANTICS_ORDERING_RELAXED = 1 << 11,
	J_MESSAGE_SEMANTICS_PERSISTENCY_IMMEDIATE = 1 << 12,
	J_MESSAGE_SEMANTICS_PERSISTENCY_EVENTUAL = 1 << 13,
	J_MESSAGE_SEMANTICS_PERSISTENCY_NONE = 1 << 14,
	J_MESSAGE_SEMANTICS_SAFETY_STORAGE = 1 << 15,
	J_MESSAGE_SEMANTICS_SAFETY_NETWORK = 1 << 16,
	J_MESSAGE_SEMANTICS_SAFETY_NONE = 1 << 17,
	J_MESSAGE_SEMANTICS_SECURITY_STRICT = 1 << 18,
	J_MESSAGE_SEMANTICS_SECURITY_NONE = 1 << 19
};

typedef enum JMessageSemantics JMessageSemantics;

/**
 * Additional message data.
 **/
struct JMessageData
{
	/**
	 * The data.
	 **/
	gconstpointer data;

	/**
	 * The data length.
	 **/
	guint64 length;
};

typedef struct JMessageData JMessageData;

/**
 * A message header.
 **/
#pragma pack(4)
struct JMessageHeader
{
	/**
	 * The message length.
	 **/
	guint32 length;

	/**
	 * The message ID.
	 **/
	guint32 id;

	/**
	 * The semantics.
	 **/
	guint32 semantics;

	/**
	 * The operation type.
	 **/
	guint32 op_type;

	/**
	 * The operation count.
	 **/
	guint32 op_count;

	JConnectionType comm_type; // contains information over the requested connection type for the peer

	size_t first_key; //position of the first key in data
	guint32 key_number; // number of keys located in data
};
#pragma pack()

typedef struct JMessageHeader JMessageHeader;

G_STATIC_ASSERT(sizeof(JMessageHeader) == 6 * sizeof(guint32) + sizeof(JConnectionType) + sizeof(size_t));

/**
 * A message.
 **/
struct JMessage
{
	/**
	 * The header.
	 */
	JMessageHeader header;

	/**
	 * The current size.
	 **/
	gsize size;

	/**
	 * The data.
	 **/
	gchar* data;

	/**
	 * The current position within #data.
	 **/
	gchar* current;

	/**
	 * The list of additional data to send in j_message_write().
	 * Contains JMessageData elements.
	 **/
	JList* send_list;

	/**
	 * The original message.
	 * Set if the message is a reply, NULL otherwise.
	 **/
	JMessage* original_message;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Returns a message's length.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return The message's length.
 **/
static gsize
j_message_length(JMessage const* message)
{
	J_TRACE_FUNCTION(NULL);

	guint32 length;

	length = message->header.length;

	return GUINT32_FROM_LE(length);
}

static void
j_message_data_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	g_slice_free(JMessageData, data);
}

/**
 * Checks whether it is possible to append data to a message.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  A length.
 *
 * \return TRUE if it is possible, FALSE otherwise.
 **/
static gboolean
j_message_can_append(JMessage const* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	return (message->current + length <= message->data + message->size);
}

/**
 * Checks whether it is possible to get data from a message.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  A length.
 *
 * \return TRUE if it is possible, FALSE otherwise.
 **/
static gboolean
j_message_can_get(JMessage const* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	return (message->current + length <= message->data + j_message_length(message));
}

static void
j_message_extend(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	gsize factor = 1;
	gsize current_length;
	gsize position;
	guint32 count;

	if (length == 0)
	{
		return;
	}

	current_length = j_message_length(message);

	if (current_length + length <= message->size)
	{
		return;
	}

	count = j_message_get_count(message);

	if (count > 10)
	{
		factor = pow(10, floor(log10(count)));
	}

	message->size += length * factor;

	position = message->current - message->data;
	message->data = g_realloc(message->data, message->size);
	message->current = message->data + position;
}

static void
j_message_ensure_size(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	gsize position;

	if (length <= message->size)
	{
		return;
	}

	message->size = length;

	position = message->current - message->data;
	message->data = g_realloc(message->data, message->size);
	message->current = message->data + position;
}

/**
*
*checks whether size of the whole j_message is in the boundaries set by the maximal Message size of the used libfabric provider
*
*
*\return TRUE if Message size is in boundaries, FALSE if not
*/
static gboolean
j_message_fi_val_message_size(struct fi_info* info, JMessage* message)
{
	gboolean ret = FALSE;
	if (((size_t)(sizeof(JMessageHeader) + j_message_length(message))) <= info->ep_attr->max_msg_size)
	{
		ret = TRUE;
	}
	return ret;
}

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
JMessage*
j_message_new(JMessageType op_type, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	JMessage* message;
	guint32 rand;

	//g_return_val_if_fail(op_type != J_MESSAGE_NONE, NULL);

	length = MAX(256, length);
	rand = g_random_int();

	message = g_slice_new(JMessage);
	message->size = length;
	message->data = g_malloc(message->size);
	message->current = message->data;
	message->send_list = j_list_new(j_message_data_free);
	message->original_message = NULL;
	message->ref_count = 1;

	message->header.length = GUINT32_TO_LE(0);
	message->header.id = GUINT32_TO_LE(rand);
	message->header.semantics = GUINT32_TO_LE(0);
	message->header.op_type = GUINT32_TO_LE(op_type);
	message->header.op_count = GUINT32_TO_LE(0);

	message->header.comm_type = J_UNDEFINED;

	return message;
}

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
JMessage*
j_message_new_reply(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	JMessage* reply;

	g_return_val_if_fail(message != NULL, NULL);

	reply = g_slice_new(JMessage);
	reply->size = 256;
	reply->data = g_malloc(reply->size);
	reply->current = reply->data;
	reply->send_list = j_list_new(j_message_data_free);
	reply->original_message = j_message_ref(message);
	reply->ref_count = 1;

	reply->header.length = GUINT32_TO_LE(0);
	reply->header.id = message->header.id;
	reply->header.semantics = GUINT32_TO_LE(0);
	reply->header.op_type = message->header.op_type;
	reply->header.op_count = GUINT32_TO_LE(0);

	reply->header.comm_type = message->header.comm_type;

	return reply;
}

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
 * \return #message.
 **/
JMessage*
j_message_ref(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, NULL);

	g_atomic_int_inc(&(message->ref_count));

	return message;
}

/**
 * Decreases a message's reference count.
 * When the reference count reaches zero, frees the memory allocated for the message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 **/
void
j_message_unref(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(message != NULL);

	if (g_atomic_int_dec_and_test(&(message->ref_count)))
	{
		if (message->original_message != NULL)
		{
			j_message_unref(message->original_message);
		}

		if (message->send_list != NULL)
		{
			j_list_unref(message->send_list);
		}

		g_free(message->data);

		g_slice_free(JMessage, message);
	}
}

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
JMessageType
j_message_get_type(JMessage const* message)
{
	J_TRACE_FUNCTION(NULL);

	JMessageType op_type;

	g_return_val_if_fail(message != NULL, J_MESSAGE_NONE);

	op_type = message->header.op_type;
	op_type = GUINT32_FROM_LE(op_type);

	return op_type;
}

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
guint32
j_message_get_count(JMessage const* message)
{
	J_TRACE_FUNCTION(NULL);

	guint32 op_count;

	g_return_val_if_fail(message != NULL, 0);

	op_count = message->header.op_count;
	op_count = GUINT32_FROM_LE(op_count);

	return op_count;
}

JConnectionType
j_message_get_comm_type(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, 0);

	return message->header.comm_type;
}

/**
* returns current key, advances key_list_current if possible by one
*/
uint64_t
j_message_get_key(JMessage* message, guint key_position)
{
	gchar* ret_key;

	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, 0);

	ret_key = message->data + message->header.first_key + key_position * sizeof(uint64_t);

	return *((uint64_t*)ret_key);
}

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
gboolean
j_message_append_1(JMessage* message, gconstpointer data)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 1), FALSE);

	*(message->current) = *((gchar const*)data);
	message->current += 1;

	new_length = j_message_length(message) + 1;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

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
gboolean
j_message_append_4(JMessage* message, gconstpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gint32 new_data;
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 4), FALSE);

	new_data = GINT32_TO_LE(*((gint32 const*)data));
	memcpy(message->current, &new_data, 4);
	message->current += 4;

	new_length = j_message_length(message) + 4;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

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
gboolean
j_message_append_8(JMessage* message, gconstpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gint64 new_data;
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 8), FALSE);

	new_data = GINT64_TO_LE(*((gint64 const*)data));
	memcpy(message->current, &new_data, 8);
	message->current += 8;

	new_length = j_message_length(message) + 8;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

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
gboolean
j_message_append_n(JMessage* message, gconstpointer data, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, length), FALSE);

	memcpy(message->current, data, length);
	message->current += length;

	new_length = j_message_length(message) + length;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

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
gboolean
j_message_append_string(JMessage* message, gchar const* str)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(str != NULL, FALSE);

	return j_message_append_n(message, str, strlen(str) + 1);
}

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
gchar
j_message_get_1(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gchar ret;

	g_return_val_if_fail(message != NULL, '\0');
	g_return_val_if_fail(j_message_can_get(message, 1), '\0');

	ret = *((gchar const*)(message->current));
	message->current += 1;

	return ret;
}

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
gint32
j_message_get_4(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gint32 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(j_message_can_get(message, 4), 0);

	memcpy(&ret, message->current, 4);
	ret = GINT32_FROM_LE(ret);
	message->current += 4;

	return ret;
}

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
gint64
j_message_get_8(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gint64 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(j_message_can_get(message, 8), 0);

	memcpy(&ret, message->current, 8);
	ret = GINT64_FROM_LE(ret);
	message->current += 8;

	return ret;
}

/**
 * Gets n bytes from a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return A pointer to the data.
 **/
gpointer
j_message_get_n(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	gpointer ret;

	g_return_val_if_fail(message != NULL, NULL);
	g_return_val_if_fail(j_message_can_get(message, length), NULL);

	ret = message->current;
	message->current += length;

	return ret;
}

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
gchar const*
j_message_get_string(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gchar const* ret;

	g_return_val_if_fail(message != NULL, NULL);

	ret = message->current;
	message->current += strlen(ret) + 1;

	return ret;
}

/**
 * Reads a message from the network.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \parem stream  A network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_receive(JMessage* message, gpointer endpoint)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(endpoint != NULL, FALSE);

	return j_message_read(message, (JEndpoint*)endpoint);
}

/**
 * Writes a message to the network. Type of communication is either based on type of prior communication (in case of response) or the one set in configuration
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \parem stream  A libfabric endpoint
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_send(JMessage* message, gpointer endpoint)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(endpoint != NULL, FALSE);

	if (!(message->header.comm_type == J_MSG || message->header.comm_type == J_RDMA))
	{
		message->header.comm_type = j_configuration_fi_get_comm_type(j_configuration());
	}

	switch (message->header.comm_type)
	{
		case J_MSG:
			return j_message_write_msg(message, (JEndpoint*)endpoint);
		case J_RDMA:
			return j_message_write_rdma(message, (JEndpoint*)endpoint);
		case J_UNDEFINED:
		default:
			g_critical("\nERROR on sending message, no valid connection type requested\n");
			return FALSE;
	}
}

/**
 * Reads a message from the network.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \parem stream  An libfabric endpoint.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_read(JMessage* message, JEndpoint* jendpoint)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	ssize_t error;

	error = 0;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(jendpoint != NULL, FALSE);

	error = fi_recv(j_endpoint_get_endpoint(jendpoint, J_MSG), (void*)&(message->header), (size_t)sizeof(JMessageHeader), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while receiving Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_RECV), -1, "Receiving", "JMessage Header"))
	{
		goto end;
	}

	// TODO Dont report error, but split the Message
	if (!j_message_fi_val_message_size(j_endpoint_get_info(jendpoint, J_MSG), message))
	{
		g_critical("\nMessage bigger than provider max_msg_size. (receiving)\nMax Message Size: %zu\nMessage Size: %zu\n", j_endpoint_get_info(jendpoint, J_MSG)->ep_attr->max_msg_size, sizeof(JMessageHeader) + j_message_length(message));
		goto end;
	}

	j_message_ensure_size(message, j_message_length(message));

	error = fi_recv(j_endpoint_get_endpoint(jendpoint, J_MSG), message->data, j_message_length(message), NULL, 0, NULL);
	if ((int)error != 0)
	{
		g_critical("\nError while receiving Message.\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_RECV), -1, "Receiving", "JMessage"))
	{
		goto end;
	}

	message->current = message->data;

	if (message->original_message != NULL)
	{
		g_assert(message->header.id == message->original_message->header.id);
	}

	ret = TRUE;

end:
	return ret;
}

/**
 * Writes a message to the network based on message communication
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \parem jendpoint a connected jendpoint.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_write_msg(JMessage* message, JEndpoint* jendpoint)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	g_autoptr(JListIterator) iterator = NULL;
	ssize_t error = 0;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(jendpoint != NULL, FALSE);

	error = fi_send(j_endpoint_get_endpoint(jendpoint, J_MSG), (void*)&(message->header), sizeof(JMessageHeader), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while sending Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}

	if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_TRANSMIT), -1, "Transmit", "JMessage Header"))
	{
		goto end;
	}

	// TODO Dont report error, but split the Message
	if (!j_message_fi_val_message_size(j_endpoint_get_info(jendpoint, J_MSG), message))
	{
		g_critical("\nMessage bigger than provider max_msg_size (Sending)\nMax Message Size: %zu\nMessage Size: %zu\n", j_endpoint_get_info(jendpoint, J_MSG)->ep_attr->max_msg_size, sizeof(JMessageHeader) + j_message_length(message));
		goto end;
	}

	error = fi_send(j_endpoint_get_endpoint(jendpoint, J_MSG), message->data, (size_t)j_message_length(message), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while sending Message Data.\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_TRANSMIT), -1, "Transmit", "JMessage"))
	{
		goto end;
	}

	if (message->send_list != NULL)
	{
		iterator = j_list_iterator_new(message->send_list);

		while (j_list_iterator_next(iterator))
		{
			JMessageData* message_data = j_list_iterator_get(iterator);

			error = fi_send(j_endpoint_get_endpoint(jendpoint, J_MSG), message_data->data, message_data->length, NULL, 0, NULL);
			if ((int)error != 0)
			{
				g_critical("\nError while sending Message List Data.\nDetails:\n%s", fi_strerror(labs(error)));
				goto end;
			}
			if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_TRANSMIT), -1, "Transmit", "JMessage list"))
			{
				goto end;
			}
		}
	}

	ret = TRUE;

end:
	return ret;
}

/**
* Communicates data, JMessage and JHeader via fi_send (message base communication) and memory chunks via rdma based communication
* procedure:
*		checks whether message to send has additional data to send, if so registers those memory regions in libfabric domain and generates for each a unique key (adds those keys to message)
*		sends message and header to peer
*		waits for peer to read memory reagions if additional data present
*		removes memory regions from libfabric
* returns TRUE if successful
*/
gboolean
j_message_write_rdma(JMessage* message, JEndpoint* jendpoint)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	GSList* mr_list = NULL;

	GRand* key_generator;

	g_autoptr(JListIterator) iterator = NULL;
	ssize_t error = 0;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(jendpoint != NULL, FALSE);

	key_generator = g_rand_new();

	if (j_list_length(message->send_list) != 0)
	{
		guint counter;
		iterator = j_list_iterator_new(message->send_list);
		message->header.first_key = message->current - message->data;
		message->header.key_number = j_list_length(message->send_list);

		counter = 0;

		while (j_list_iterator_next(iterator))
		{
			struct fid_mr* memory_region;
			uint64_t key;

			JMessageData* message_data = j_list_iterator_get(iterator);

			//key = ((uint64_t) g_rand_int(key_generator)) << 32 | (uint64_t) g_rand_int(key_generator);
			key = (uint64_t)g_rand_int(key_generator);
			//key = (uint64_t) j_list_iterator_get(iterator); //TODO generate keys from list element pointer

			if (!j_message_append_n(message, &key, sizeof(uint64_t)))
			{
				g_critical("\nError while appending keys to jmessage\n");
				goto end;
			}

			printf("\nSENDING:          setting key: %lu\n", j_message_get_key(message, counter)); //debug

			error = fi_mr_reg(j_endpoint_get_domain(jendpoint, J_RDMA),
					  message_data->data,
					  message_data->length,
					  j_configuration_fi_get_mr_access(j_configuration()),
					  0,
					  j_message_get_key(message, counter),
					  j_configuration_fi_get_mr_flags(j_configuration()),
					  &memory_region,
					  NULL);
			if (error != 0)
			{
				g_critical("\nError while registering memory region\nDetails:\n%s", fi_strerror(labs(error)));
			}

			/* TODO: find why binding to endpoint not possible
			error = fi_mr_bind(memory_region, &j_endpoint_get_endpoint(jendpoint, J_RDMA)->fid, 0);
			if(error != 0)
			{
				g_critical("\nError while binding memory region to endpoint\nDeatils:\n%s", fi_strerror(labs(error)));
			}*/

			mr_list = g_slist_prepend(mr_list, (gpointer)memory_region);
			counter++;
		}
	}

	error = fi_send(j_endpoint_get_endpoint(jendpoint, J_MSG), (void*)&(message->header), sizeof(JMessageHeader), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while sending Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_TRANSMIT), -1, "Transmit", "JMessage Header"))
	{
		goto end;
	}

	// TODO Dont report error, but split the Message
	if (!j_message_fi_val_message_size(j_endpoint_get_info(jendpoint, J_MSG), message))
	{
		g_critical("\nMessage bigger than provider max_msg_size (Sending)\nMax Message Size: %zu\nMessage Size: %zu\n", j_endpoint_get_info(jendpoint, J_MSG)->ep_attr->max_msg_size, sizeof(JMessageHeader) + j_message_length(message));
		goto end;
	}

	error = fi_send(j_endpoint_get_endpoint(jendpoint, J_MSG), message->data, (size_t)j_message_length(message), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while sending Message Data.\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_TRANSMIT), -1, "Transmit", "JMessage"))
	{
		goto end;
	}

	if (j_list_length(message->send_list) != 0)
	{
		int* wakeup_buf;

		wakeup_buf = malloc(sizeof(int));

		error = fi_recv(j_endpoint_get_endpoint(jendpoint, J_MSG), (void*)wakeup_buf, sizeof(int), NULL, 0, NULL);
		if (error != 0)
		{
			g_critical("\nError receiving completion message.\nDetails:\n%s", fi_strerror(labs(error)));
			free(wakeup_buf);
			goto end;
		}
		if (!j_endpoint_read_completion_queue(j_endpoint_get_completion_queue(jendpoint, J_MSG, FI_RECV), -1, "Transmit", "JMessage list data completion message"))
		{
			free(wakeup_buf);
			goto end;
		}
		free(wakeup_buf);
	}

	ret = TRUE;

end:
	if (mr_list != NULL)
	{
		for (guint i = 0; i < g_slist_length(mr_list); i++)
		{
			error = fi_close(&((struct fid_mr*)g_slist_nth_data(mr_list, i))->fid);
			if (error != 0)
			{
				g_critical("\nERROR closing fid_mrs after sending\nDetails:\n%s", fi_strerror(labs(error)));
			}
		}
		g_slist_free(mr_list);
	}

	g_rand_free(key_generator);
	return ret;
}

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
void
j_message_add_send(JMessage* message, gconstpointer data, guint64 length)
{
	J_TRACE_FUNCTION(NULL);

	JMessageData* message_data;

	g_return_if_fail(message != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);

	message_data = g_slice_new(JMessageData);
	message_data->data = data;
	message_data->length = length;

	j_list_append(message->send_list, message_data);
}

/**
 * Adds a new operation to a message.
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  A length.
 **/
void
j_message_add_operation(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_op_count;

	g_return_if_fail(message != NULL);

	new_op_count = j_message_get_count(message) + 1;
	message->header.op_count = GUINT32_TO_LE(new_op_count);

	j_message_extend(message, length);
}

void
j_message_set_semantics(JMessage* message, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	guint32 serialized_semantics = 0;

	g_return_if_fail(message != NULL);
	g_return_if_fail(semantics != NULL);

#define SERIALIZE_SEMANTICS(type, key) \
	{ \
		gint tmp; \
		tmp = j_semantics_get(semantics, J_SEMANTICS_##type); \
		if (tmp == J_SEMANTICS_##type##_##key) \
		{ \
			serialized_semantics |= J_MESSAGE_SEMANTICS_##type##_##key; \
		} \
	}

	SERIALIZE_SEMANTICS(ATOMICITY, BATCH)
	SERIALIZE_SEMANTICS(ATOMICITY, OPERATION)
	SERIALIZE_SEMANTICS(ATOMICITY, NONE)
	SERIALIZE_SEMANTICS(CONCURRENCY, OVERLAPPING)
	SERIALIZE_SEMANTICS(CONCURRENCY, NON_OVERLAPPING)
	SERIALIZE_SEMANTICS(CONCURRENCY, NONE)
	SERIALIZE_SEMANTICS(CONSISTENCY, IMMEDIATE)
	SERIALIZE_SEMANTICS(CONSISTENCY, EVENTUAL)
	SERIALIZE_SEMANTICS(CONSISTENCY, NONE)
	SERIALIZE_SEMANTICS(ORDERING, STRICT)
	SERIALIZE_SEMANTICS(ORDERING, SEMI_RELAXED)
	SERIALIZE_SEMANTICS(ORDERING, RELAXED)
	SERIALIZE_SEMANTICS(PERSISTENCY, IMMEDIATE)
	SERIALIZE_SEMANTICS(PERSISTENCY, EVENTUAL)
	SERIALIZE_SEMANTICS(PERSISTENCY, NONE)
	SERIALIZE_SEMANTICS(SAFETY, STORAGE)
	SERIALIZE_SEMANTICS(SAFETY, NETWORK)
	SERIALIZE_SEMANTICS(SAFETY, NONE)
	SERIALIZE_SEMANTICS(SECURITY, STRICT)
	SERIALIZE_SEMANTICS(SECURITY, NONE)

#undef SERIALIZE_SEMANTICS

	message->header.semantics = GUINT32_TO_LE(serialized_semantics);
}

JSemantics*
j_message_get_semantics(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	JSemantics* semantics;

	guint32 serialized_semantics;

	g_return_val_if_fail(message != NULL, NULL);

	serialized_semantics = message->header.semantics;
	serialized_semantics = GUINT32_FROM_LE(serialized_semantics);

	// If serialized_semantics is 0, we will end up with the default semantics.
	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

#define DESERIALIZE_SEMANTICS(type, key) \
	if (serialized_semantics & J_MESSAGE_SEMANTICS_##type##_##key) \
	{ \
		j_semantics_set(semantics, J_SEMANTICS_##type, J_SEMANTICS_##type##_##key); \
	}

	DESERIALIZE_SEMANTICS(ATOMICITY, BATCH)
	DESERIALIZE_SEMANTICS(ATOMICITY, OPERATION)
	DESERIALIZE_SEMANTICS(ATOMICITY, NONE)
	DESERIALIZE_SEMANTICS(CONCURRENCY, OVERLAPPING)
	DESERIALIZE_SEMANTICS(CONCURRENCY, NON_OVERLAPPING)
	DESERIALIZE_SEMANTICS(CONCURRENCY, NONE)
	DESERIALIZE_SEMANTICS(CONSISTENCY, IMMEDIATE)
	DESERIALIZE_SEMANTICS(CONSISTENCY, EVENTUAL)
	DESERIALIZE_SEMANTICS(CONSISTENCY, NONE)
	DESERIALIZE_SEMANTICS(ORDERING, STRICT)
	DESERIALIZE_SEMANTICS(ORDERING, SEMI_RELAXED)
	DESERIALIZE_SEMANTICS(ORDERING, RELAXED)
	DESERIALIZE_SEMANTICS(PERSISTENCY, IMMEDIATE)
	DESERIALIZE_SEMANTICS(PERSISTENCY, EVENTUAL)
	DESERIALIZE_SEMANTICS(PERSISTENCY, NONE)
	DESERIALIZE_SEMANTICS(SAFETY, STORAGE)
	DESERIALIZE_SEMANTICS(SAFETY, NETWORK)
	DESERIALIZE_SEMANTICS(SAFETY, NONE)
	DESERIALIZE_SEMANTICS(SECURITY, STRICT)
	DESERIALIZE_SEMANTICS(SECURITY, NONE)

#undef DESERIALIZE_SEMANTICS

	return semantics;
}

/**
 * @}
 **/
