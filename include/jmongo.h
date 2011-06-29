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

#ifndef H_MONGO
#define H_MONGO

#include <glib.h>

#pragma pack(4)
struct JMongoHeader
{
	gint32 message_length;
	gint32 request_id;
	gint32 response_to;
	gint32 op_code;
};
#pragma pack()

typedef struct JMongoHeader JMongoHeader;

#include <jbson.h>
#include <jlist.h>
#include <jmongo-connection.h>
#include <jmongo-iterator.h>

void j_mongo_create_index(JMongoConnection*, const gchar*, JBSON*, gboolean);

void j_mongo_insert (JMongoConnection*, const gchar*, JBSON*);
void j_mongo_insert_list (JMongoConnection*, const gchar*, JList*);

JMongoIterator* j_mongo_find (JMongoConnection*, const gchar*, JBSON*, JBSON*, gint32, gint32);

gboolean j_mongo_command (JMongoConnection*, const gchar*, JBSON*);
gboolean j_mongo_command_int (JMongoConnection*, const gchar*, const gchar*, gint32);

#endif
