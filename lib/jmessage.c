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

struct JMessageData
{
	gconstpointer data;
	guint64 length;
};

typedef struct JMessageData JMessageData;

/**
 * A message.
 **/
#pragma pack(4)
struct JMessage
{
	/**
	 * The current position within #data.
	 **/
	gchar* current;

	JList* data_list;

	/**
	 * The message's header.
	 **/
	JMessageHeader header;

	/**
	 * The message's data.
	 **/
	gchar data[];
};
#pragma pack()

/**
 * Returns the message's length.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The message's length.
 **/
static
gsize
j_message_length (JMessage* message)
{
	return GUINT32_FROM_LE(message->header.length);
}

static
void
j_message_data_free (gpointer data)
{
	g_slice_free(JMessageData, data);
}

/**
 * Creates a new message.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param length The message's length.
 * \param op     The message's operation type.
 * \param count  The message's operation count.
 *
 * \return A new message. Should be freed with j_message_free().
 **/
JMessage*
j_message_new (gsize length, JMessageOperationType op_type, guint32 op_count)
{
	JMessage* message;
	guint32 rand;
	guint32 real_length;

	rand = g_random_int();
	real_length = sizeof(JMessageHeader) + length;

	message = g_malloc(sizeof(gchar*) + sizeof(JList*) + real_length);
	message->current = message->data;
	message->data_list = j_list_new(j_message_data_free);
	message->header.length = GUINT32_TO_LE(real_length);
	message->header.id = GUINT32_TO_LE(rand);
	message->header.op_type = GUINT32_TO_LE(op_type);
	message->header.op_count = GUINT32_TO_LE(op_count);

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
 * \return A new message. Should be freed with j_message_free().
 **/
JMessage*
j_message_new_reply (JMessage* message, gsize length)
{
	JMessage* reply;
	guint32 real_length;

	real_length = sizeof(JMessageHeader) + length;

	reply = g_malloc(sizeof(gchar*) + sizeof(JList*) + real_length);
	reply->current = reply->data;
	reply->data_list = NULL;
	reply->header.length = GUINT32_TO_LE(real_length);
	reply->header.id = message->header.id;
	reply->header.op_type = GUINT32_TO_LE(J_MESSAGE_OPERATION_REPLY);
	reply->header.op_count = message->header.op_count;

	return reply;
}

/**
 * Frees the memory allocated by the message.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 **/
void
j_message_free (JMessage* message)
{
	g_return_if_fail(message != NULL);

	if (message->data_list != NULL)
	{
		j_list_unref(message->data_list);
	}

	g_free(message);
}

/**
 * Appends 1 byte to the message.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 * \param data    The data to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_append_1 (JMessage* message, gconstpointer data)
{
	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(message->current + 1 <= (gchar const*)&(message->header) + j_message_length(message), FALSE);

	*(message->current) = *((gchar const*)data);
	message->current += 1;

	return TRUE;
}

/**
 * Appends 4 bytes to the message.
 * The bytes are converted to little endian automatically.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 * \param data    The data to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_append_4 (JMessage* message, gconstpointer data)
{
	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(message->current + 4 <= (gchar const*)&(message->header) + j_message_length(message), FALSE);

	*((gint32*)(message->current)) = GINT32_TO_LE(*((gint32 const*)data));
	message->current += 4;

	return TRUE;
}

/**
 * Appends 8 bytes to the message.
 * The bytes are converted to little endian automatically.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 * \param data    The data to append.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_append_8 (JMessage* message, gconstpointer data)
{
	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(message->current + 8 <= (gchar const*)&(message->header) + j_message_length(message), FALSE);

	*((gint64*)(message->current)) = GINT64_TO_LE(*((gint64 const*)data));
	message->current += 8;

	return TRUE;
}

/**
 * Appends a number of bytes to the message.
 *
 * \author Michael Kuhn
 *
 * \code
 * gchar* str = "Hello world!";
 * ...
 * j_message_append_n(message, str, strlen(str) + 1);
 * \endcode
 *
 * \param message The message.
 * \param data    The data to append.
 * \param length  The length of data.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_append_n (JMessage* message, gconstpointer data, gsize length)
{
	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(message->current + length <= (gchar const*)&(message->header) + j_message_length(message), FALSE);

	memcpy(message->current, data, length);
	message->current += length;

	return TRUE;
}

/**
 * Gets 1 byte from the message.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The 1 byte.
 **/
gchar
j_message_get_1 (JMessage* message)
{
	gchar ret;

	g_return_val_if_fail(message != NULL, '\0');
	g_return_val_if_fail(message->current + 1 <= (gchar const*)&(message->header) + j_message_length(message), '\0');

	ret = *((gchar const*)(message->current));
	message->current += 1;

	return ret;
}

/**
 * Gets 4 bytes from the message.
 * The bytes are converted from little endian automatically.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The 4 bytes.
 **/
gint32
j_message_get_4 (JMessage* message)
{
	gint32 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(message->current + 4 <= (gchar const*)&(message->header) + j_message_length(message), 0);

	ret = GINT32_FROM_LE(*((gint32 const*)(message->current)));
	message->current += 4;

	return ret;
}

/**
 * Gets 8 bytes from the message.
 * The bytes are converted from little endian automatically.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The 8 bytes.
 **/
gint64
j_message_get_8 (JMessage* message)
{
	gint64 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(message->current + 8 <= (gchar const*)&(message->header) + j_message_length(message), 0);

	ret = GINT64_FROM_LE(*((gint64 const*)(message->current)));
	message->current += 8;

	return ret;
}

/**
 * Gets a string from the message.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The string.
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
 * \param message The message.
 * \parem stream  The network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_read (JMessage* message, GInputStream* stream)
{
	gsize bytes_read;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	if (!g_input_stream_read_all(stream, &(message->header), sizeof(JMessageHeader), &bytes_read, NULL, NULL))
	{
		return FALSE;
	}

	g_printerr("read_header %" G_GSIZE_FORMAT "\n", bytes_read);

	if (bytes_read == 0)
	{
		return FALSE;
	}

	if (!g_input_stream_read_all(stream, message->data, j_message_length(message) - sizeof(JMessageHeader), &bytes_read, NULL, NULL))
	{
		return FALSE;
	}

	g_printerr("read_message %" G_GSIZE_FORMAT "\n", bytes_read);

	message->current = message->data;

	return TRUE;
}

/**
 * Reads a reply from the network.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 * \parem stream  The network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_read_reply (JMessage* reply, JMessage* message, GInputStream* stream)
{
	g_return_val_if_fail(reply != NULL, FALSE);
	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	g_return_val_if_fail(j_message_operation_type(reply) == J_MESSAGE_OPERATION_REPLY, FALSE);

	if (j_message_read(reply, stream))
	{
		g_return_val_if_fail(reply->header.id == message->header.id, FALSE);

		return TRUE;
	}

	return FALSE;
}

/**
 * Writes a message to the network.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 * \parem stream  The network stream.
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

	if (!g_output_stream_write_all(stream, &(message->header), j_message_length(message), &bytes_written, NULL, NULL))
	{
		return FALSE;
	}

	g_printerr("write_message %" G_GSIZE_FORMAT "\n", bytes_written);

	if (bytes_written == 0)
	{
		return FALSE;
	}

	if (message->data_list != NULL)
	{
		iterator = j_list_iterator_new(message->data_list);

		while (j_list_iterator_next(iterator))
		{
			JMessageData* message_data = j_list_iterator_get(iterator);

			if (!g_output_stream_write_all(stream, message_data->data, message_data->length, &bytes_written, NULL, NULL))
			{
				return FALSE;
			}

			g_printerr("write_message_data %" G_GSIZE_FORMAT "\n", bytes_written);
		}

		j_list_iterator_free(iterator);
	}

	g_output_stream_flush(stream, NULL, NULL);

	return TRUE;
}

/**
 * Returns the message's ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The message's ID.
 **/
guint32
j_message_id (JMessage* message)
{
	g_return_val_if_fail(message != NULL, 0);

	return GUINT32_FROM_LE(message->header.id);
}

/**
 * Returns the message's operation type.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The message's operation type.
 **/
JMessageOperationType
j_message_operation_type (JMessage* message)
{
	g_return_val_if_fail(message != NULL, J_MESSAGE_OPERATION_NONE);

	return GUINT32_FROM_LE(message->header.op_type);
}

/**
 * Returns the message's operation count.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The message.
 *
 * \return The message's operation count.
 **/
guint32
j_message_operation_count (JMessage* message)
{
	g_return_val_if_fail(message != NULL, 0);

	return GUINT32_FROM_LE(message->header.op_count);
}

void
j_message_add_data (JMessage* message, gconstpointer data, guint64 length)
{
	JMessageData* message_data;

	g_return_if_fail(message != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);

	message_data = g_slice_new(JMessageData);
	message_data->data = data;
	message_data->length = length;

	j_list_append(message->data_list, message_data);
}

/**
 * @}
 **/
