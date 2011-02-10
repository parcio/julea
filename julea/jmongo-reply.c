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

#include "jmongo-reply.h"

#include "jbson.h"
#include "jmongo.h"

/**
 * \defgroup JMongoReply MongoDB Reply
 *
 * @{
 **/

/*
 * See http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol.
 */

/**
 * A MongoDB reply.
 **/
#pragma pack(4)
struct JMongoReply
{
	gchar* current;
	JMongoHeader header;
	gint32 response_flags;
	gint64 cursor_id;
	gint32 starting_from;
	gint32 number_returned;
	gchar data[];
};
#pragma pack()

JMongoReply*
j_mongo_reply_new (GInputStream* stream)
{
	JMongoHeader header;
	JMongoReply* reply;
	gsize bytes_read;

	g_return_val_if_fail(stream != NULL, NULL);

	if (!g_input_stream_read_all(stream, &header, sizeof(JMongoHeader), &bytes_read, NULL, NULL))
	{
		return NULL;
	}

	if (bytes_read == 0)
	{
		return NULL;
	}

	reply = g_malloc(sizeof(gchar*) + GINT32_FROM_LE(header.message_length));
	reply->current = reply->data;
	reply->header = header;

	if (!g_input_stream_read_all(stream, &(reply->response_flags), j_mongo_reply_length(reply) - sizeof(JMongoHeader), &bytes_read, NULL, NULL))
	{
		g_free(reply);

		return NULL;
	}

	return reply;
}

void
j_mongo_reply_free (JMongoReply* reply)
{
	g_return_if_fail(reply != NULL);

	g_free(reply);
}

JBSON*
j_mongo_reply_get (JMongoReply* reply)
{
	JBSON* bson;

	g_return_val_if_fail(reply != NULL, NULL);

	if (reply->current >= (gchar const*)&(reply->header) + j_mongo_reply_length(reply))
	{
		return NULL;
	}

	bson = j_bson_new_for_data(reply->current);
	reply->current += j_bson_size(bson);

	return bson;
}

gsize
j_mongo_reply_length (JMongoReply* reply)
{
	g_return_val_if_fail(reply != NULL, 0);

	return GINT32_FROM_LE(reply->header.message_length);
}

gint32
j_mongo_reply_number (JMongoReply* reply)
{
	g_return_val_if_fail(reply != NULL, -1);

	return GINT32_FROM_LE(reply->number_returned);
}

gint64
j_mongo_reply_cursor_id (JMongoReply* reply)
{
	g_return_val_if_fail(reply != NULL, -1);

	return GINT64_FROM_LE(reply->cursor_id);
}

/**
 * @}
 **/
