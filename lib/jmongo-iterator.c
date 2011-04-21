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

#include "jmongo-iterator.h"

#include "jbson.h"
#include "jmongo-connection.h"
#include "jmongo-message.h"
#include "jmongo-reply.h"

/**
 * \defgroup JMongoIterator MongoDB Iterator
 *
 * @{
 **/

/**
 * A MongoDB iterator.
 **/
struct JMongoIterator
{
	JMongoConnection* connection;
	gchar* collection;
	JMongoReply* reply;
	JBSON* bson;
};

static
gboolean
j_mongo_iterator_get_more (JMongoIterator* iterator)
{
	JMongoMessage* message;
	gint32 const zero = 0;
	gint64 cursor_id;
	gsize length;
	gsize message_length;

	cursor_id = j_mongo_reply_cursor_id(iterator->reply);

	if (cursor_id == 0)
	{
		return FALSE;
	}

	length = strlen(iterator->collection) + 1;
	message_length = 4 + length + 4 + 8;

	message = j_mongo_message_new(message_length, J_MONGO_MESSAGE_OP_GET_MORE);

	j_mongo_message_append_4(message, &zero);
	j_mongo_message_append_n(message, iterator->collection, length);
	j_mongo_message_append_4(message, &zero);
	j_mongo_message_append_8(message, &cursor_id);

	j_mongo_connection_send(iterator->connection, message);

	j_mongo_message_free(message);

	j_mongo_reply_free(iterator->reply);
	iterator->reply = j_mongo_connection_receive(iterator->connection);
	iterator->bson = NULL;

	return TRUE;
}

JMongoIterator*
j_mongo_iterator_new (JMongoConnection* connection, gchar const* collection, JMongoReply* reply)
{
	JMongoIterator* iterator;

	g_return_val_if_fail(reply != NULL, NULL);

	iterator = g_slice_new(JMongoIterator);
	iterator->connection = j_mongo_connection_ref(connection);
	iterator->collection = g_strdup(collection);
	iterator->reply = reply;
	iterator->bson = NULL;

	return iterator;
}

void
j_mongo_iterator_free (JMongoIterator* iterator)
{
	gint64 cursor_id;

	g_return_if_fail(iterator != NULL);

	cursor_id = j_mongo_reply_cursor_id(iterator->reply);

	if (cursor_id != 0)
	{
		JMongoMessage* message;
		gint32 const zero = 0;
		gint32 const one = 1;

		message = j_mongo_message_new(sizeof(gint32) + sizeof(gint32) + sizeof(gint64), J_MONGO_MESSAGE_OP_KILL_CURSORS);
		j_mongo_message_append_4(message, &zero);
		j_mongo_message_append_4(message, &one);
		j_mongo_message_append_8(message, &cursor_id);

		j_mongo_connection_send(iterator->connection, message);

		j_mongo_message_free(message);
	}

	if (iterator->bson != NULL)
	{
		j_bson_unref(iterator->bson);
	}

	j_mongo_reply_free(iterator->reply);
	j_mongo_connection_unref(iterator->connection);

	g_free(iterator->collection);

	g_slice_free(JMongoIterator, iterator);
}

gboolean
j_mongo_iterator_next (JMongoIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	if (j_mongo_reply_number(iterator->reply) == 0)
	{
		return FALSE;
	}

	if (iterator->bson == NULL)
	{
		iterator->bson = j_mongo_reply_get(iterator->reply);

		return (iterator->bson != NULL);
	}

	j_bson_unref(iterator->bson);
	iterator->bson = j_mongo_reply_get(iterator->reply);

	if (iterator->bson == NULL)
	{
		if (!j_mongo_iterator_get_more(iterator))
		{
			return FALSE;
		}

		iterator->bson = j_mongo_reply_get(iterator->reply);
	}

	return (iterator->bson != NULL);
}

JBSON*
j_mongo_iterator_get (JMongoIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return j_bson_ref(iterator->bson);
}

/**
 * @}
 **/
