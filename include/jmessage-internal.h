/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#ifndef H_MESSAGE_INTERNAL
#define H_MESSAGE_INTERNAL

#include <glib.h>

#include <julea-internal.h>

enum JMessageType
{
	J_MESSAGE_NONE           = 0,
	J_MESSAGE_CREATE         = 1 << 0,
	J_MESSAGE_DELETE         = 1 << 1,
	J_MESSAGE_READ           = 1 << 2,
	J_MESSAGE_STATISTICS     = 1 << 3,
	J_MESSAGE_STATUS         = 1 << 4,
	J_MESSAGE_WRITE          = 1 << 5,
	J_MESSAGE_REPLY          = 1 << 6,
	J_MESSAGE_SAFETY_NETWORK = 1 << 7,
	J_MESSAGE_SAFETY_STORAGE = 1 << 8,
	J_MESSAGE_MODIFIER_MASK  = (J_MESSAGE_REPLY | J_MESSAGE_SAFETY_NETWORK | J_MESSAGE_SAFETY_STORAGE)
};

typedef enum JMessageType JMessageType;

struct JMessage;

typedef struct JMessage JMessage;

#include <gio/gio.h>

#include <jsemantics.h>

J_GNUC_INTERNAL JMessage* j_message_new (JMessageType, gsize);
J_GNUC_INTERNAL JMessage* j_message_new_reply (JMessage*);
J_GNUC_INTERNAL JMessage* j_message_ref (JMessage*);
J_GNUC_INTERNAL void j_message_unref (JMessage*);

J_GNUC_INTERNAL guint32 j_message_get_id (JMessage const*);
J_GNUC_INTERNAL JMessageType j_message_get_type (JMessage const*);
J_GNUC_INTERNAL JMessageType j_message_get_type_modifier (JMessage const*);
J_GNUC_INTERNAL guint32 j_message_get_count (JMessage const*);

J_GNUC_INTERNAL gboolean j_message_append_1 (JMessage*, gconstpointer);
J_GNUC_INTERNAL gboolean j_message_append_4 (JMessage*, gconstpointer);
J_GNUC_INTERNAL gboolean j_message_append_8 (JMessage*, gconstpointer);
J_GNUC_INTERNAL gboolean j_message_append_n (JMessage*, gconstpointer, gsize);

J_GNUC_INTERNAL gchar j_message_get_1 (JMessage*);
J_GNUC_INTERNAL gint32 j_message_get_4 (JMessage*);
J_GNUC_INTERNAL gint64 j_message_get_8 (JMessage*);
J_GNUC_INTERNAL gchar const* j_message_get_string (JMessage*);

J_GNUC_INTERNAL gboolean j_message_read (JMessage*, GInputStream*);
J_GNUC_INTERNAL gboolean j_message_write (JMessage*, GOutputStream*);

J_GNUC_INTERNAL void j_message_add_send (JMessage*, gconstpointer, guint64);
J_GNUC_INTERNAL void j_message_add_operation (JMessage*, gsize);

J_GNUC_INTERNAL void j_message_set_safety (JMessage*, JSemantics*);

#endif
