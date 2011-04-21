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

#ifndef H_MONGO_MESSAGE
#define H_MONGO_MESSAGE

struct JMongoMessage;

typedef struct JMongoMessage JMongoMessage;

enum JMongoMessageOp
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

typedef enum JMongoMessageOp JMongoMessageOp;

#include <glib.h>

JMongoMessage* j_mongo_message_new (gsize, JMongoMessageOp);
void j_mongo_message_free (JMongoMessage*);

gboolean j_mongo_message_append_1 (JMongoMessage*, gconstpointer);
gboolean j_mongo_message_append_4 (JMongoMessage*, gconstpointer);
gboolean j_mongo_message_append_8 (JMongoMessage*, gconstpointer);
gboolean j_mongo_message_append_n (JMongoMessage*, gconstpointer, gsize);

gconstpointer j_mongo_message_data (JMongoMessage*);
gsize j_mongo_message_length (JMongoMessage*);

#endif
