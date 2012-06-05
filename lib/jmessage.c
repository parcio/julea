/*
 * Copyright (c) 2010-2012 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <string.h>

#include <jmessage.h>

#include <jlist.h>
#include <jlist-iterator.h>

/**
 * \defgroup JMessage Message
 *
 * @{
 **/

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

#pragma pack(4)
/**
 * A message header.
 **/
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
 * \author Michael Kuhn
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
 * \author Michael Kuhn
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
 * \author Michael Kuhn
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
 * \author Michael Kuhn
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
	gsize position;

	if (length == 0)
	{
		return;
	}

	message->size += length;

	position = message->current - message->data;
	message->data = g_realloc(message->data, message->size);
	message->current = message->data + position;
}

static
void
j_message_ensure_size (JMessage* message, gsize length)
{
	if (length <= message->size)
	{
		return;
	}

	j_message_extend(message, length - message->size);
}

/**
 * Creates a new message.
 *
 * \author Michael Kuhn
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
	j_message_header(message)->op_type = GUINT32_TO_LE(op_type);
	j_message_header(message)->op_count = GUINT32_TO_LE(0);

	return message;
}

/**
 * Creates a new reply message.
 *
 * \author Michael Kuhn
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

	reply = g_slice_new(JMessage);
	reply->size = sizeof(JMessageHeader);
	reply->data = g_malloc(reply->size);
	reply->current = reply->data + sizeof(JMessageHeader);
	reply->send_list = j_list_new(j_message_data_free);
	reply->original_message = j_message_ref(message);
	reply->ref_count = 1;

	j_message_header(reply)->length = GUINT32_TO_LE(0);
	j_message_header(reply)->id = j_message_header(message)->id;
	j_message_header(reply)->op_type = GUINT32_TO_LE(J_MESSAGE_REPLY);
	j_message_header(reply)->op_count = GUINT32_TO_LE(0);

	return reply;
}

/**
 * Increases a message's reference count.
 *
 * \author Michael Kuhn
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

	g_atomic_int_inc(&(message->ref_count));

	return message;
}

/**
 * Decreases a message's reference count.
 * When the reference count reaches zero, frees the memory allocated for the message.
 *
 * \author Michael Kuhn
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
 * Returns a message's ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return The message's ID.
 **/
guint32
j_message_get_id (JMessage const* message)
{
	guint32 id;

	g_return_val_if_fail(message != NULL, 0);

	id = j_message_header(message)->id;

	return GUINT32_FROM_LE(id);
}

/**
 * Returns a message's type.
 *
 * \author Michael Kuhn
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

	op_type = j_message_header(message)->op_type;

	return GUINT32_FROM_LE(op_type);
}

/**
 * Returns a message's count.
 *
 * \author Michael Kuhn
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

	op_count = j_message_header(message)->op_count;

	return GUINT32_FROM_LE(op_count);
}

/**
 * Appends 1 byte to a message.
 *
 * \author Michael Kuhn
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

	*(message->current) = *((gchar const*)data);
	message->current += 1;

	new_length = j_message_length(message) + 1;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	return TRUE;
}

/**
 * Appends 4 bytes to a message.
 * The bytes are converted to little endian automatically.
 *
 * \author Michael Kuhn
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
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 4), FALSE);

	*((gint32*)(message->current)) = GINT32_TO_LE(*((gint32 const*)data));
	message->current += 4;

	new_length = j_message_length(message) + 4;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	return TRUE;
}

/**
 * Appends 8 bytes to a message.
 * The bytes are converted to little endian automatically.
 *
 * \author Michael Kuhn
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
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 8), FALSE);

	*((gint64*)(message->current)) = GINT64_TO_LE(*((gint64 const*)data));
	message->current += 8;

	new_length = j_message_length(message) + 8;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	return TRUE;
}

/**
 * Appends a number of bytes to a message.
 *
 * \author Michael Kuhn
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

	memcpy(message->current, data, length);
	message->current += length;

	new_length = j_message_length(message) + length;
	j_message_header(message)->length = GUINT32_TO_LE(new_length);

	return TRUE;
}

/**
 * Gets 1 byte from a message.
 *
 * \author Michael Kuhn
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

	ret = *((gchar const*)(message->current));
	message->current += 1;

	return ret;
}

/**
 * Gets 4 bytes from a message.
 * The bytes are converted from little endian automatically.
 *
 * \author Michael Kuhn
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

	ret = GINT32_FROM_LE(*((gint32 const*)(message->current)));
	message->current += 4;

	return ret;
}

/**
 * Gets 8 bytes from a message.
 * The bytes are converted from little endian automatically.
 *
 * \author Michael Kuhn
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

	ret = GINT64_FROM_LE(*((gint64 const*)(message->current)));
	message->current += 8;

	return ret;
}

/**
 * Gets a string from a message.
 *
 * \author Michael Kuhn
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

	ret = message->current;
	message->current += strlen(ret) + 1;

	return ret;
}

/**
 * Reads a message from the network.
 *
 * \author Michael Kuhn
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
	gsize bytes_read;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	if (j_message_get_type(message) == J_MESSAGE_REPLY)
	{
		g_return_val_if_fail(message->original_message != NULL, FALSE);
	}

	if (!g_input_stream_read_all(stream, message->data, sizeof(JMessageHeader), &bytes_read, NULL, NULL))
	{
		return FALSE;
	}

	if (bytes_read == 0)
	{
		return FALSE;
	}

	j_message_ensure_size(message, sizeof(JMessageHeader) + j_message_length(message));

	if (!g_input_stream_read_all(stream, message->data + sizeof(JMessageHeader), j_message_length(message), &bytes_read, NULL, NULL))
	{
		return FALSE;
	}

	message->current = message->data + sizeof(JMessageHeader);

	if (j_message_get_type(message) == J_MESSAGE_REPLY)
	{
		g_assert(j_message_header(message)->id == j_message_header(message->original_message)->id);
	}

	return TRUE;
}

/**
 * Writes a message to the network.
 *
 * \author Michael Kuhn
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
	JListIterator* iterator;
	gsize bytes_written;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	if (!g_output_stream_write_all(stream, message->data, sizeof(JMessageHeader) + j_message_length(message), &bytes_written, NULL, NULL))
	{
		return FALSE;
	}

	if (bytes_written == 0)
	{
		return FALSE;
	}

	if (message->send_list != NULL)
	{
		iterator = j_list_iterator_new(message->send_list);

		while (j_list_iterator_next(iterator))
		{
			JMessageData* message_data = j_list_iterator_get(iterator);

			if (!g_output_stream_write_all(stream, message_data->data, message_data->length, &bytes_written, NULL, NULL))
			{
				return FALSE;
			}
		}

		j_list_iterator_free(iterator);
	}

	g_output_stream_flush(stream, NULL, NULL);

	return TRUE;
}

/**
 * Adds new data to send to a message.
 *
 * \author Michael Kuhn
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

	message_data = g_slice_new(JMessageData);
	message_data->data = data;
	message_data->length = length;

	j_list_append(message->send_list, message_data);
}

/**
 * Adds a new operation to a message.
 *
 * \author Michael Kuhn
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

	new_op_count = j_message_get_count(message) + 1;
	j_message_header(message)->op_count = GUINT32_TO_LE(new_op_count);

	j_message_extend(message, length);
}

/**
 * @}
 **/
