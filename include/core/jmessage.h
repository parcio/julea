/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#ifndef JULEA_MESSAGE_H
#define JULEA_MESSAGE_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gio/gio.h>

G_BEGIN_DECLS

enum JMessageType
{
	J_MESSAGE_NONE,
	J_MESSAGE_PING,
	J_MESSAGE_STATISTICS,
	J_MESSAGE_OBJECT_CREATE,
	J_MESSAGE_OBJECT_DELETE,
	J_MESSAGE_OBJECT_READ,
	J_MESSAGE_OBJECT_STATUS,
	J_MESSAGE_OBJECT_WRITE,
	J_MESSAGE_KV_PUT,
	J_MESSAGE_KV_DELETE,
	J_MESSAGE_KV_GET,
	J_MESSAGE_KV_GET_ALL,
	J_MESSAGE_KV_GET_BY_PREFIX,
	J_MESSAGE_DB_SCHEMA_CREATE,
	J_MESSAGE_DB_SCHEMA_GET,
	J_MESSAGE_DB_SCHEMA_DELETE,
	J_MESSAGE_DB_INSERT,
	J_MESSAGE_DB_UPDATE,
	J_MESSAGE_DB_DELETE,
	J_MESSAGE_DB_QUERY,
	J_MESSAGE_RESET
};

typedef enum JMessageType JMessageType;

struct JMessage;

typedef struct JMessage JMessage;

G_END_DECLS

#include <core/jsemantics.h>

G_BEGIN_DECLS

JMessage* j_message_new (JMessageType, gsize);
JMessage* j_message_new_reply (JMessage*);
JMessage* j_message_ref (JMessage*);
void j_message_unref (JMessage*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JMessage, j_message_unref)

JMessageType j_message_get_type (JMessage const*);
guint32 j_message_get_count (JMessage const*);

gboolean j_message_append_1 (JMessage*, gconstpointer);
gboolean j_message_append_4 (JMessage*, gconstpointer);
gboolean j_message_append_8 (JMessage*, gconstpointer);
gboolean j_message_append_n (JMessage*, gconstpointer, gsize);
gboolean j_message_append_string (JMessage*, gchar const*);

gchar j_message_get_1 (JMessage*);
gint32 j_message_get_4 (JMessage*);
gint64 j_message_get_8 (JMessage*);
gpointer j_message_get_n (JMessage*, gsize);
gchar const* j_message_get_string (JMessage*);

gboolean j_message_send (JMessage*, gpointer);
gboolean j_message_receive (JMessage*, gpointer);

gboolean j_message_read (JMessage*, GInputStream*);
gboolean j_message_write (JMessage*, GOutputStream*);

void j_message_add_send (JMessage*, gconstpointer, guint64);
void j_message_add_operation (JMessage*, gsize);

void j_message_set_semantics (JMessage*, JSemantics*);
JSemantics* j_message_get_semantics (JMessage*);

G_END_DECLS

#endif
