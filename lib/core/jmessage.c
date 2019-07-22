/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
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

#define JMESSAGE_COMPILATION

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
#include <jtrace-internal.h>

#include <julea-internal.h>

/**
 * \defgroup JMessage Message
 *
 * @{
 **/

enum JMessageSemantics
{
	J_MESSAGE_SEMANTICS_ATOMICITY_BATCH =             1 << 0,
	J_MESSAGE_SEMANTICS_ATOMICITY_OPERATION =         1 << 1,
	J_MESSAGE_SEMANTICS_ATOMICITY_NONE =              1 << 2,
	J_MESSAGE_SEMANTICS_CONCURRENCY_OVERLAPPING =     1 << 3,
	J_MESSAGE_SEMANTICS_CONCURRENCY_NON_OVERLAPPING = 1 << 4,
	J_MESSAGE_SEMANTICS_CONCURRENCY_NONE =            1 << 5,
	J_MESSAGE_SEMANTICS_CONSISTENCY_IMMEDIATE =       1 << 6,
	J_MESSAGE_SEMANTICS_CONSISTENCY_EVENTUAL =        1 << 7,
	J_MESSAGE_SEMANTICS_CONSISTENCY_NONE =            1 << 8,
	J_MESSAGE_SEMANTICS_ORDERING_STRICT =             1 << 9,
	J_MESSAGE_SEMANTICS_ORDERING_SEMI_RELAXED =       1 << 10,
	J_MESSAGE_SEMANTICS_ORDERING_RELAXED =            1 << 11,
	J_MESSAGE_SEMANTICS_PERSISTENCY_IMMEDIATE =       1 << 12,
	J_MESSAGE_SEMANTICS_PERSISTENCY_EVENTUAL =        1 << 13,
	J_MESSAGE_SEMANTICS_PERSISTENCY_NONE =            1 << 14,
	J_MESSAGE_SEMANTICS_SAFETY_STORAGE =              1 << 15,
	J_MESSAGE_SEMANTICS_SAFETY_NETWORK =              1 << 16,
	J_MESSAGE_SEMANTICS_SAFETY_NONE =                 1 << 17,
	J_MESSAGE_SEMANTICS_SECURITY_STRICT =             1 << 18,
	J_MESSAGE_SEMANTICS_SECURITY_NONE =               1 << 19
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

/**
 * A message.
 **/
struct JMessage
{
	/**
	 * The current size.
	 **/
	gsize size;

	/**
	 * The data.
	 * This also contains a JMessageHeader.
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
 * Returns a message's header.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return The message's header.
 **/
static
JMessageHeader*
j_message_header (JMessage const* message)
{
	return (JMessageHeader*)message->data;
}

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
static
gsize
j_message_length (JMessage const* message)
{
	guint32 length;

	length = j_message_header(message)->length;

	return GUINT32_FROM_LE(length);
}

static
void
j_message_data_free (gpointer data)
{
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
static
gboolean
j_message_can_append (JMessage const* message, gsize length)
{
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
static
gboolean
j_message_can_get (JMessage const* message, gsize length)
{
	return (message->current + length <= message->data + sizeof(JMessageHeader) + j_message_length(message));
}

static
void
j_message_extend (JMessage* message, gsize length)
{
	gsize factor = 1;
	gsize current_length;
	gsize position;
	guint32 count;

	if (length == 0)
	{
		return;
	}

	current_length = j_message_length(message);

	if (sizeof(JMessageHeader) + current_length + length <= message->size)
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

static
void
j_message_ensure_size (JMessage* message, gsize length)
{
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
j_message_new (JMessageType op_type, gsize length)
{
	JMessage* message;
	guint32 rand;

	//g_return_val_if_fail(op_type != J_MESSAGE_NONE, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	rand = g_random_int();

	message = g_slice_new(JMessage);
	message->size = sizeof(JMessageHeader) + length;
	message->data = g_malloc(message->size);
	message->current = message->data + sizeof(JMessageHeader);
	message->send_list = j_list_new(j_message_data_free);
	message->original_message = NULL;
	message->ref_count = 1;

	j_message_header(message)->length = GUINT32_TO_LE(0);
	j_message_header(message)->id = GUINT32_TO_LE(rand);
	j_message_header(message)->semantics = GUINT32_TO_LE(0);
	j_message_header(message)->op_type = GUINT32_TO_LE(op_type);
	j_message_header(message)->op_count = GUINT32_TO_LE(0);

	j_trace_leave(G_STRFUNC);

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
j_message_new_reply (JMessage* message)
{
	JMessage* reply;

	g_return_val_if_fail(message != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	reply = g_slice_new(JMessage);
	reply->size = sizeof(JMessageHeader);
	reply->data = g_malloc(reply->size);
	reply->current = reply->data + sizeof(JMessageHeader);
	reply->send_list = j_list_new(j_message_data_free);
	reply->original_message = j_message_ref(message);
	reply->ref_count = 1;

	j_message_header(reply)->length = GUINT32_TO_LE(0);
	j_message_header(reply)->id = j_message_header(message)->id;
	j_message_header(reply)->semantics = GUINT32_TO_LE(0);
	j_message_header(reply)->op_type = j_message_header(message)->op_type;
	j_message_header(reply)->op_count = GUINT32_TO_LE(0);

	j_trace_leave(G_STRFUNC);

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
j_message_ref (JMessage* message)
{
	g_return_val_if_fail(message != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);
	g_atomic_int_inc(&(message->ref_count));
	j_trace_leave(G_STRFUNC);

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
j_message_unref (JMessage* message)
{
	g_return_if_fail(message != NULL);

	j_trace_enter(G_STRFUNC, NULL);

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

	j_trace_leave(G_STRFUNC);
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
j_message_get_type (JMessage const* message)
{
	JMessageType op_type;

	g_return_val_if_fail(message != NULL, J_MESSAGE_NONE);

	j_trace_enter(G_STRFUNC, NULL);

	op_type = j_message_header(message)->op_type;
	op_type = GUINT32_FROM_LE(op_type);

	j_trace_leave(G_STRFUNC);

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
j_message_get_count (JMessage const* message)
{
	guint32 op_count;

	g_return_val_if_fail(message != NULL, 0);

	j_trace_enter(G_STRFUNC, NULL);

	op_count = j_message_header(message)->op_count;
	op_count = GUINT32_FROM_LE(op_count);

	j_trace_leave(G_STRFUNC);

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
j_message_append_1 (JMessage* message, gconstpointer data)
{
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 1), FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	*(message->current) = *((gchar const*)data);
	message->current += 1;

	new_length = j_message_length(message) + 1;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	j_trace_leave(G_STRFUNC);

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
j_message_append_4 (JMessage* message, gconstpointer data)
{
	gint32 new_data;
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 4), FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	new_data = GINT32_TO_LE(*((gint32 const*)data));
	memcpy(message->current, &new_data, 4);
	message->current += 4;

	new_length = j_message_length(message) + 4;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	j_trace_leave(G_STRFUNC);

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
j_message_append_8 (JMessage* message, gconstpointer data)
{
	gint64 new_data;
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 8), FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	new_data = GINT64_TO_LE(*((gint64 const*)data));
	memcpy(message->current, &new_data, 8);
	message->current += 8;

	new_length = j_message_length(message) + 8;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	j_trace_leave(G_STRFUNC);

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
j_message_append_n (JMessage* message, gconstpointer data, gsize length)
{
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, length), FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	memcpy(message->current, data, length);
	message->current += length;

	new_length = j_message_length(message) + length;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	j_trace_leave(G_STRFUNC);

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
j_message_append_string (JMessage* message, gchar const* str)
{
	gboolean ret;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(str != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	ret = j_message_append_n(message, str, strlen(str) + 1);

	j_trace_leave(G_STRFUNC);

	return ret;
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
j_message_get_1 (JMessage* message)
{
	gchar ret;

	g_return_val_if_fail(message != NULL, '\0');
	g_return_val_if_fail(j_message_can_get(message, 1), '\0');

	j_trace_enter(G_STRFUNC, NULL);

	ret = *((gchar const*)(message->current));
	message->current += 1;

	j_trace_leave(G_STRFUNC);

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
j_message_get_4 (JMessage* message)
{
	gint32 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(j_message_can_get(message, 4), 0);

	j_trace_enter(G_STRFUNC, NULL);

	memcpy(&ret, message->current, 4);
	ret = GINT32_FROM_LE(ret);
	message->current += 4;

	j_trace_leave(G_STRFUNC);

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
j_message_get_8 (JMessage* message)
{
	gint64 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(j_message_can_get(message, 8), 0);

	j_trace_enter(G_STRFUNC, NULL);

	memcpy(&ret, message->current, 8);
	ret = GINT64_FROM_LE(ret);
	message->current += 8;

	j_trace_leave(G_STRFUNC);

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
j_message_get_n (JMessage* message, gsize length)
{
	gpointer ret;

	g_return_val_if_fail(message != NULL, NULL);
	g_return_val_if_fail(j_message_can_get(message, length), NULL);

	j_trace_enter(G_STRFUNC, NULL);

	ret = message->current;
	message->current += length;

	j_trace_leave(G_STRFUNC);

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
j_message_get_string (JMessage* message)
{
	gchar const* ret;

	g_return_val_if_fail(message != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	ret = message->current;
	message->current += strlen(ret) + 1;

	j_trace_leave(G_STRFUNC);

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
j_message_receive (JMessage* message, GSocketConnection* connection)
{
	gboolean ret;

	GInputStream* stream;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(connection != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	stream = g_io_stream_get_input_stream(G_IO_STREAM(connection));
	ret = j_message_read(message, stream);

	j_trace_leave(G_STRFUNC);

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
j_message_send (JMessage* message, GSocketConnection* connection)
{
	gboolean ret;

	GOutputStream* stream;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(connection != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	j_helper_set_cork(connection, TRUE);

	stream = g_io_stream_get_output_stream(G_IO_STREAM(connection));
	ret = j_message_write(message, stream);

	j_helper_set_cork(connection, FALSE);

	j_trace_leave(G_STRFUNC);

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
j_message_read (JMessage* message, GInputStream* stream)
{
	gboolean ret = FALSE;

	GError* error = NULL;
	gsize bytes_read;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	if (!g_input_stream_read_all(stream, message->data, sizeof(JMessageHeader), &bytes_read, NULL, &error))
	{
		goto end;
	}

	if (bytes_read == 0)
	{
		goto end;
	}

	j_message_ensure_size(message, sizeof(JMessageHeader) + j_message_length(message));

	if (!g_input_stream_read_all(stream, message->data + sizeof(JMessageHeader), j_message_length(message), &bytes_read, NULL, &error))
	{
		goto end;
	}

	message->current = message->data + sizeof(JMessageHeader);

	if (message->original_message != NULL)
	{
		g_assert(j_message_header(message)->id == j_message_header(message->original_message)->id);
	}

	ret = TRUE;

end:
	if (error != NULL)
	{
		J_CRITICAL("%s", error->message);
		g_error_free(error);
	}

	j_trace_leave(G_STRFUNC);

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
j_message_write (JMessage* message, GOutputStream* stream)
{
	gboolean ret = FALSE;

	g_autoptr(JListIterator) iterator = NULL;
	GError* error = NULL;
	gsize bytes_written;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	if (!g_output_stream_write_all(stream, message->data, sizeof(JMessageHeader) + j_message_length(message), &bytes_written, NULL, &error))
	{
		goto end;
	}

	if (bytes_written == 0)
	{
		goto end;
	}

	if (message->send_list != NULL)
	{
		iterator = j_list_iterator_new(message->send_list);

		while (j_list_iterator_next(iterator))
		{
			JMessageData* message_data = j_list_iterator_get(iterator);

			if (!g_output_stream_write_all(stream, message_data->data, message_data->length, &bytes_written, NULL, &error))
			{
				goto end;
			}
		}
	}

	g_output_stream_flush(stream, NULL, NULL);

	ret = TRUE;

end:
	if (error != NULL)
	{
		J_CRITICAL("%s", error->message);
		g_error_free(error);
	}

	j_trace_leave(G_STRFUNC);

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
j_message_add_send (JMessage* message, gconstpointer data, guint64 length)
{
	JMessageData* message_data;

	g_return_if_fail(message != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);

	j_trace_enter(G_STRFUNC, NULL);

	message_data = g_slice_new(JMessageData);
	message_data->data = data;
	message_data->length = length;

	j_list_append(message->send_list, message_data);

	j_trace_leave(G_STRFUNC);
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
j_message_add_operation (JMessage* message, gsize length)
{
	guint32 new_op_count;

	g_return_if_fail(message != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	new_op_count = j_message_get_count(message) + 1;
	j_message_header(message)->op_count = GUINT32_TO_LE(new_op_count);

	j_message_extend(message, length);

	j_trace_leave(G_STRFUNC);
}

void
j_message_set_semantics (JMessage* message, JSemantics* semantics)
{
	guint32 serialized_semantics = 0;

	g_return_if_fail(message != NULL);
	g_return_if_fail(semantics != NULL);

	j_trace_enter(G_STRFUNC, NULL);

#define SERIALIZE_SEMANTICS(type, key) { gint tmp; tmp = j_semantics_get(semantics, J_SEMANTICS_ ## type); if (tmp == J_SEMANTICS_ ## type ## _ ## key) { serialized_semantics |= J_MESSAGE_SEMANTICS_ ## type ## _ ##key; } }

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

	j_message_header(message)->semantics = GUINT32_TO_LE(serialized_semantics);

	j_trace_leave(G_STRFUNC);
}

JSemantics*
j_message_get_semantics (JMessage* message)
{
	JSemantics* semantics;

	guint32 serialized_semantics;

	g_return_val_if_fail(message != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	serialized_semantics = j_message_header(message)->semantics;
	serialized_semantics = GUINT32_FROM_LE(serialized_semantics);

	// If serialized_semantics is 0, we will end up with the default semantics.
	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

#define DESERIALIZE_SEMANTICS(type, key) if (serialized_semantics & J_MESSAGE_SEMANTICS_ ## type ## _ ## key) { j_semantics_set(semantics, J_SEMANTICS_ ## type, J_SEMANTICS_ ## type ## _ ## key); }

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

	j_trace_leave(G_STRFUNC);

	return semantics;
}

/**
 * @}
 **/
