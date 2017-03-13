/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#ifndef H_MESSAGE_INTERNAL
#define H_MESSAGE_INTERNAL

#include <glib.h>
#include <gio/gio.h>

#include <julea-internal.h>

enum JMessageType
{
	J_MESSAGE_NONE              = 0,
	J_MESSAGE_PING              = 1 << 0,
	J_MESSAGE_STATISTICS        = 1 << 1,
	J_MESSAGE_DATA_CREATE       = 1 << 2,
	J_MESSAGE_DATA_DELETE       = 1 << 3,
	J_MESSAGE_DATA_READ         = 1 << 4,
	J_MESSAGE_DATA_STATUS       = 1 << 5,
	J_MESSAGE_DATA_WRITE        = 1 << 6,
	J_MESSAGE_META_PUT          = 1 << 7,
	J_MESSAGE_META_DELETE       = 1 << 8,
	J_MESSAGE_META_GET          = 1 << 9,
	J_MESSAGE_META_GET_ALL      = 1 << 10,
	J_MESSAGE_META_GET_BY_VALUE = 1 << 11,
	J_MESSAGE_REPLY             = 1 << 12,
	J_MESSAGE_SAFETY_NETWORK    = 1 << 13,
	J_MESSAGE_SAFETY_STORAGE    = 1 << 14,
	J_MESSAGE_MODIFIER_MASK     = (J_MESSAGE_REPLY | J_MESSAGE_SAFETY_NETWORK | J_MESSAGE_SAFETY_STORAGE)
};

typedef enum JMessageType JMessageType;

struct JMessage;

typedef struct JMessage JMessage;

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
J_GNUC_INTERNAL gpointer j_message_get_n (JMessage*, gsize);
J_GNUC_INTERNAL gchar const* j_message_get_string (JMessage*);

J_GNUC_INTERNAL gboolean j_message_send (JMessage*, GSocketConnection*);
J_GNUC_INTERNAL gboolean j_message_receive (JMessage*, GSocketConnection*);

J_GNUC_INTERNAL gboolean j_message_read (JMessage*, GInputStream*);
J_GNUC_INTERNAL gboolean j_message_write (JMessage*, GOutputStream*);

J_GNUC_INTERNAL void j_message_add_send (JMessage*, gconstpointer, guint64);
J_GNUC_INTERNAL void j_message_add_operation (JMessage*, gsize);

J_GNUC_INTERNAL void j_message_set_safety (JMessage*, JSemantics*);
J_GNUC_INTERNAL void j_message_force_safety (JMessage*, gint);

#endif
