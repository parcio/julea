/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include "jmessage-reply.h"

/**
 * \defgroup JMessageReply Message Reply
 *
 * @{
 **/

/**
 * A message reply.
 **/
#pragma pack(4)
struct JMessageReply
{
	/**
	 * The message reply's header.
	 **/
	JMessageReplyHeader header;
};
#pragma pack()

/**
 * Creates a new message reply.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message The corresponding message.
 *
 * \return A new message reply. Should be freed with j_message_reply_free().
 **/
JMessageReply*
j_message_reply_new (JMessage* message, guint64 length)
{
	JMessageReply* message_reply;

	message_reply = g_slice_new(JMessageReply);
	message_reply->header.id = GUINT32_TO_LE(j_message_id(message));
	message_reply->header.length = GUINT32_TO_LE(length);

	return message_reply;
}

/**
 * Frees the memory allocated by the message reply.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message_reply The message reply.
 **/
void
j_message_reply_free (JMessageReply* message_reply)
{
	g_return_if_fail(message_reply != NULL);

	g_slice_free(JMessageReply, message_reply);
}

/**
 * Reads a message reply from the network.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message_reply The message reply.
 * \parem stream        The network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_reply_read (JMessageReply* message_reply, GInputStream* stream)
{
	JMessageReplyHeader header;
	gsize bytes_read;

	g_return_val_if_fail(message_reply != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	if (!g_input_stream_read_all(stream, &header, sizeof(JMessageReplyHeader), &bytes_read, NULL, NULL))
	{
		return FALSE;
	}

	g_printerr("read_header %" G_GSIZE_FORMAT "\n", bytes_read);

	if (bytes_read == 0)
	{
		return FALSE;
	}

	g_assert(message_reply->header.id == header.id);

	message_reply->header = header;

	return TRUE;
}

/**
 * Writes a message reply to the network.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message_reply The message reply.
 * \parem stream        The network stream.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_message_reply_write (JMessageReply* message_reply, GOutputStream* stream)
{
	gsize bytes_written;

	g_return_val_if_fail(message_reply != NULL, FALSE);
	g_return_val_if_fail(stream != NULL, FALSE);

	if (!g_output_stream_write_all(stream, &(message_reply->header), sizeof(JMessageReplyHeader), &bytes_written, NULL, NULL))
	{
		return FALSE;
	}

	g_printerr("write_header %" G_GSIZE_FORMAT "\n", bytes_written);

	if (bytes_written == 0)
	{
		return FALSE;
	}

	return TRUE;
}

/**
 * Returns the message reply's length.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param message_reply The message reply.
 *
 * \return The message reply's length.
 **/
gsize
j_message_reply_length (JMessageReply* message_reply)
{
	g_return_val_if_fail(message_reply != NULL, 0);

	return GUINT32_FROM_LE(message_reply->header.length);
}

/**
 * @}
 **/
