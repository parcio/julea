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

#ifndef H_MESSAGE
#define H_MESSAGE

#pragma pack(4)
struct JMessageHeader
{
	guint32 length;
	gint32 id;
	gint32 op;
};
#pragma pack()

struct JMessage;

typedef struct JMessage JMessage;
typedef struct JMessageHeader JMessageHeader;

enum JMessageOp
{
	J_MESSAGE_OP_READ,
	J_MESSAGE_OP_WRITE
};

typedef enum JMessageOp JMessageOp;

#include <glib.h>

JMessage* j_message_new (gsize, JMessageOp);
void j_message_free (JMessage*);

gboolean j_message_append_1 (JMessage*, gconstpointer);
gboolean j_message_append_4 (JMessage*, gconstpointer);
gboolean j_message_append_8 (JMessage*, gconstpointer);
gboolean j_message_append_n (JMessage*, gconstpointer, gsize);

gconstpointer j_message_data (JMessage*);
gsize j_message_length (JMessage*);

#endif
