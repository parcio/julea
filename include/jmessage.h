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

#ifndef H_MESSAGE
#define H_MESSAGE

#include <glib.h>

enum JMessageOperationType
{
	J_MESSAGE_OPERATION_NONE,
	J_MESSAGE_OPERATION_CREATE,
	J_MESSAGE_OPERATION_DELETE,
	J_MESSAGE_OPERATION_READ,
	J_MESSAGE_OPERATION_REPLY,
	J_MESSAGE_OPERATION_STATISTICS,
	J_MESSAGE_OPERATION_SYNC,
	J_MESSAGE_OPERATION_WRITE
};

typedef enum JMessageOperationType JMessageOperationType;

struct JMessage;

typedef struct JMessage JMessage;

#include <gio/gio.h>

JMessage* j_message_new (JMessageOperationType, gsize);
JMessage* j_message_new_reply (JMessage*);
void j_message_free (JMessage*);

gboolean j_message_append_1 (JMessage*, gconstpointer);
gboolean j_message_append_4 (JMessage*, gconstpointer);
gboolean j_message_append_8 (JMessage*, gconstpointer);
gboolean j_message_append_n (JMessage*, gconstpointer, gsize);

gchar j_message_get_1 (JMessage*);
gint32 j_message_get_4 (JMessage*);
gint64 j_message_get_8 (JMessage*);
gchar const* j_message_get_string (JMessage*);

gboolean j_message_read (JMessage*, GInputStream*);
gboolean j_message_read_reply (JMessage*, JMessage*, GInputStream*);
gboolean j_message_write (JMessage*, GOutputStream*);

guint32 j_message_id (JMessage*);
JMessageOperationType j_message_operation_type (JMessage*);
guint32 j_message_operation_count (JMessage*);

void j_message_add_send (JMessage*, gconstpointer, guint64);
void j_message_add_operation (JMessage*, gsize);

#endif
