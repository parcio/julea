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

/**
 * \defgroup JMongoMessage MongoDB Message
 *
 * @{
 **/

/*
 * See http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol.
 */

static const gint32 j_mongo_message_zero = 0;

enum JMongoMessageOpCode
{
	J_MONGO_MESSAGE_OP_REPLY = 1,
	J_MONGO_MESSAGE_OP_MSG = 1000,
	J_MONGO_MESSAGE_OP_UPDATE = 2001,
	J_MONGO_MESSAGE_OP_INSERT = 2002,
	J_MONGO_MESSAGE_OP_QUERY = 2004,
	J_MONGO_MESSAGE_OP_GET_MORE = 2005,
	J_MONGO_MESSAGE_OP_DELETE = 2006,
	J_MONGO_MESSAGE_OP_KILL_CURSORS = 2007
};

typedef enum JMongoMessageOpCode JMongoMessageOpCode;

#pragma pack(4)
struct JMongoMessageHeader
{
	gint32 message_length;
	gint32 request_id;
	gint32 response_to;
	gint32 op_code;
};
#pragma pack()

typedef struct JMongoMessageHeader JMongoMessageHeader;

/**
 * A MongoDB message.
 **/
#pragma pack(4)
struct JMongoMessage
{
	JMongoMessageHeader header;
	gchar data[];
};
#pragma pack()

JMongoMessage*
j_mongo_message_new (gsize length)
{
	JMongoMessage* message;

	message = g_malloc(sizeof(JMongoMessageHeader) + length);

	return message;
}

void
j_mongo_message_free (JMongoMessage* message)
{
	g_free(message);
}

/**
 * @}
 **/
