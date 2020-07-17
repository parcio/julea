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

#include <rdma/fabric.h>
#include <rdma/fi_domain.h> //includes cqs and
#include <rdma/fi_cm.h> //connection management
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>

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
};
#pragma pack()

typedef struct JMessageHeader JMessageHeader;

G_STATIC_ASSERT(sizeof(JMessageHeader) == 5 * sizeof(guint32));

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
j_message_fi_val_message_size(size_t max_msg_size, JMessage* message)
{
	gboolean ret = FALSE;
	if (((size_t)(sizeof(JMessageHeader) + j_message_length(message))) <= max_msg_size)
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
 * Writes a message to the network.
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

	return j_message_write(message, (JEndpoint*)endpoint);
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
j_message_read(JMessage* message, JEndpoint* j_endpoint)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	struct fid_ep* endpoint;
	ssize_t error;

	struct fi_cq_entry completion_queue_data;
	struct fi_cq_err_entry completion_queue_err_entry;

	error = 0;
	endpoint = j_endpoint->endpoint;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(j_endpoint != NULL, FALSE);
	g_return_val_if_fail(endpoint != NULL, FALSE);

	error = fi_recv(endpoint, (void*)&(message->header), (size_t)sizeof(JMessageHeader), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while receiving Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}

	error = fi_cq_sread(j_endpoint->completion_queue_receive, &completion_queue_data, 1, 0, -1);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_cq_readerr(j_endpoint->completion_queue_receive, &completion_queue_err_entry, 0);
			g_critical("\nError on completion queue while receiving Message (JMessage Header).\nDetails:\n%s", fi_cq_strerror(j_endpoint->completion_queue_receive, completion_queue_err_entry.prov_errno, completion_queue_err_entry.err_data, NULL, 0));
			goto end;
		}
		else if (error == -FI_EAGAIN || error == -FI_ECANCELED)
		{
			goto end;
		}
		else if (error > 0)
		{
			//g_printf("\nMessage Header received on Completion Queue (j_message_read)\nNumber: %ld.\n", error);
			error = 0;
		}
		else if (error < 0)
		{
			g_critical("\nError while reading completion queue after receiving Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
			goto end;
		}
	}

	//TODO splice the message, not just report an error
	if (!j_message_fi_val_message_size(j_endpoint->max_msg_size, message))
	{
		g_critical("\nMessage bigger than provider max_msg_size. (receiving)\nMax Message Size: %zu\nMessage Size: %zu\n", j_endpoint->max_msg_size, sizeof(JMessageHeader) + j_message_length(message));
		goto end;
	}

	j_message_ensure_size(message, j_message_length(message));

	//TODO descriptor (4th field) may be set, if local machine shall be used (provider dependent)
	error = fi_recv(endpoint, message->data, j_message_length(message), NULL, 0, NULL);
	if ((int)error != 0)
	{
		g_critical("\nError while receiving Message.\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	error = fi_cq_sread(j_endpoint->completion_queue_receive, &completion_queue_data, 1, NULL, -1);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_cq_readerr(j_endpoint->completion_queue_receive, &completion_queue_err_entry, 0);
			g_critical("\nError on completion queue while receiving Message Data.\nDetails:\n%s", fi_cq_strerror(j_endpoint->completion_queue_receive, completion_queue_err_entry.prov_errno, completion_queue_err_entry.err_data, NULL, 0));
			goto end;
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nNo completion data on completion Queue after receiving Message data. (SHOULD NOT HAPPEN, SINCE HEADER RECEIVED)\n");
			goto end;
		}
		else if (error == -FI_ECANCELED)
		{
			g_critical("\nShutdown initiated while receiving Data, last transfer most likely not completed.\n");
			goto end;
		}
		else if (error < 0)
		{
			g_critical("\nError while reading completion queue after receiving Message data.\nDetails:\n%s", fi_strerror(labs(error)));
			goto end;
		}
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
 * Writes a message to the network.
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
j_message_write(JMessage* message, JEndpoint* j_endpoint)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	g_autoptr(JListIterator) iterator = NULL;
	ssize_t error = 0;

	struct fi_cq_entry completion_queue_data;
	struct fi_cq_err_entry completion_queue_err_entry;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(j_endpoint != NULL, FALSE);

	//TODO descriptor (4th field) may needs to be set, if local machine shall be used (provider dependent)
	error = fi_send(j_endpoint->endpoint, (void*)&(message->header), sizeof(JMessageHeader), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while sending Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}

	error = fi_cq_sread(j_endpoint->completion_queue_transmit, &completion_queue_data, 1, NULL, -1);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_cq_readerr(j_endpoint->completion_queue_transmit, &completion_queue_err_entry, 0);
			g_critical("\nError on completion Queue after sending Message (JMessage Header)\nDetails:\n%s", fi_cq_strerror(j_endpoint->completion_queue_transmit, completion_queue_err_entry.prov_errno, completion_queue_err_entry.err_data, NULL, 0));
			goto end;
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nNo completion data on completion Queue after sending Message (JMessage Header).");
			goto end;
		}
		else if (error == -FI_ECANCELED)
		{
			g_printf("\nData transfer (JMessage Header) canceled through interrupt. Last data transfer not completed\n");
			goto end;
		}
		else if (error < 0)
		{
			g_critical("\nError reading completion Queue after sending Message (JMessage Header).\nDetails:\n%s", fi_strerror(labs(error)));
			goto end;
		}
	}

	//TODO Dont report error, but split the Message
	if (!j_message_fi_val_message_size(j_endpoint->max_msg_size, message))
	{
		g_critical("\nMessage bigger than provider max_msg_size (Sending)\nMax Message Size: %zu\nMessage Size: %zu\n", j_endpoint->max_msg_size, sizeof(JMessageHeader) + j_message_length(message));
		goto end;
	}

	error = fi_send(j_endpoint->endpoint, message->data, (size_t)j_message_length(message), NULL, 0, NULL);
	if (error != 0)
	{
		g_critical("\nError while sending Message Data.\nDetails:\n%s", fi_strerror(labs(error)));
		goto end;
	}
	else
	{
		//g_printf("JMessage Data sending succeeded.");
	}
	error = fi_cq_sread(j_endpoint->completion_queue_transmit, &completion_queue_data, 1, NULL, -1);
	if (error != 0)
	{
		if (error == -FI_EAVAIL)
		{
			error = fi_cq_readerr(j_endpoint->completion_queue_transmit, &completion_queue_err_entry, 0);
			g_critical("\nError on completion Queue after sending Message Data\nDetails:\n%s", fi_cq_strerror(j_endpoint->completion_queue_transmit, completion_queue_err_entry.prov_errno, completion_queue_err_entry.err_data, NULL, 0));
			goto end;
		}
		else if (error == -FI_EAGAIN)
		{
			g_critical("\nNo completion data on completion Queue after sending Message Data.");
			goto end;
		}
		else if (error == -FI_ECANCELED)
		{
			g_printf("\nData transfer (Message Data) canceled via interrupt. Data Transfer not completed\n");
			goto end;
		}
		else if (error < 0)
		{
			g_critical("\nError reading completion Queue after sending Message Data.\nDetails:\n%s", fi_strerror(labs(error)));
			goto end;
		}
	}

	if (message->send_list != NULL)
	{
		iterator = j_list_iterator_new(message->send_list);

		while (j_list_iterator_next(iterator))
		{
			JMessageData* message_data = j_list_iterator_get(iterator);

			//TODO descriptor (4th field) may be set, if local machine shall be used (provider dependent)
			error = fi_send(j_endpoint->endpoint, message_data->data, message_data->length, NULL, 0, NULL);
			if ((int)error != 0)
			{
				g_critical("\nError while sending Message List Data.\nDetails:\n%s", fi_strerror(labs(error)));
				goto end;
			}
			else
			{
				//printf("\nMessage List Data sent.\n"); //debug
				//fflush(stdout);
			}

			error = fi_cq_sread(j_endpoint->completion_queue_transmit, &completion_queue_data, 1, NULL, -1);
			if (error != 0)
			{
				if (error == -FI_EAVAIL)
				{
					error = fi_cq_readerr(j_endpoint->completion_queue_transmit, &completion_queue_err_entry, 0);
					g_critical("\nError on completion queue after sending Message data.\nDetails\n:%s", fi_cq_strerror(j_endpoint->completion_queue_transmit, completion_queue_err_entry.prov_errno, completion_queue_err_entry.err_data, NULL, 0));
					goto end;
				}
				else if (error == -FI_EAGAIN)
				{
					g_critical("\nNo completion data on completion Queue after sending Message List data.");
					goto end;
				}
				else if (error == -FI_ECANCELED)
				{
					g_printf("\nData transfer (data junk) canceled via interupt. Data transfer not completed.\n");
					goto end;
				}
				else if (error < 0)
				{
					g_critical("\nError while reading completion queue after sending Message List data.\nDetails:\n%s", fi_strerror(labs(error)));
					goto end;
				}
			}
		}
	}

	ret = TRUE;

end:
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
