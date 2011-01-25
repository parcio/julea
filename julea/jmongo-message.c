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

#include "jmongo-message.h"

#include "jmongo.h"

/**
 * \defgroup JMongoMessage MongoDB Message
 *
 * @{
 **/

/*
 * See http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol.
 */

/**
 * A MongoDB message.
 **/
#pragma pack(4)
struct JMongoMessage
{
	JMongoHeader header;
	gchar data[];
};
#pragma pack()

JMongoMessage*
j_mongo_message_new (gsize length, JMongoMessageOp op)
{
	JMongoMessage* message;

	message = g_malloc(sizeof(JMongoHeader) + length);
	message->header.message_length = GINT32_TO_LE(sizeof(JMongoHeader) + length);
	message->header.request_id = GINT32_TO_LE(0);
	message->header.response_to = GINT32_TO_LE(0);
	message->header.op_code = GINT32_TO_LE(op);

	return message;
}

void
j_mongo_message_free (JMongoMessage* message)
{
	g_return_if_fail(message != NULL);

	g_free(message);
}

gpointer
j_mongo_message_data (JMongoMessage* message)
{
	g_return_val_if_fail(message != NULL, NULL);

	return message->data;
}

gsize
j_mongo_message_length (JMongoMessage* message)
{
	g_return_val_if_fail(message != NULL, 0);

	return GINT32_FROM_LE(message->header.message_length);
}

/**
 * @}
 **/
