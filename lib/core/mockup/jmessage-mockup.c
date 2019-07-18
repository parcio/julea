/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
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

/* this mockup should avoid the network communication and execute the requested backend operations on the client side instead */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-decls"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <julea-config.h>
#include <stdio.h>
#include <math.h>
#include <float.h>
#include <glib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <julea.h>
#include <db/jdb-internal.h>
#include <julea-internal.h>
#include <core/jmessage.h>
#include "../../../test-afl/afl.h"
#if (JULEA_TEST_MOCKUP == 0)
#define j_message_new j_message_mockup_new
#define j_message_new_reply j_message_mockup_new_reply
#define j_message_ref j_message_mockup_ref
#define j_message_unref j_message_mockup_unref
#define j_message_get_type j_message_mockup_get_type
#define j_message_get_flags j_message_mockup_get_flags
#define j_message_get_count j_message_mockup_get_count
#define j_message_append_1 j_message_mockup_append_1
#define j_message_append_4 j_message_mockup_append_4
#define j_message_append_8 j_message_mockup_append_8
#define j_message_append_n j_message_mockup_append_n
#define j_message_get_1 j_message_mockup_get_1
#define j_message_get_4 j_message_mockup_get_4
#define j_message_get_8 j_message_mockup_get_8
#define j_message_get_n j_message_mockup_get_n
#define j_message_get_string j_message_mockup_get_string
#define j_message_send j_message_mockup_send
#define j_message_receive j_message_mockup_receive
#define j_message_read j_message_mockup_read
#define j_message_write j_message_mockup_write
#define j_message_add_send j_message_mockup_add_send
#define j_message_add_operation j_message_mockup_add_operation
#define j_message_set_safety j_message_mockup_set_safety
#define j_message_force_safety j_message_mockup_force_safety
#endif
#define myabort(val)                                         \
	do                                                   \
	{                                                    \
		if (val)                                     \
		{                                            \
			J_CRITICAL("assertion failed%d", 0); \
			abort();                             \
		}                                            \
	} while (0)

struct JMessage
{
	guint operation_count;
	GByteArray* data;
	gchar* current;
	guint size_requested;
	guint size_used;
	JMessageType type;
	gint ref_count;
	gboolean client_side;
};
JMessage*
j_message_mockup_new(JMessageType type, gsize size);
JMessage*
j_message_mockup_new_reply(JMessage* message_input);
JMessage*
j_message_mockup_ref(JMessage* message);
void
j_message_mockup_unref(JMessage* message);
gboolean
j_message_mockup_append_1(JMessage* message, gconstpointer data);
gboolean
j_message_mockup_append_4(JMessage* message, gconstpointer data);
gboolean
j_message_mockup_append_8(JMessage* message, gconstpointer data);
gboolean
j_message_mockup_append_n(JMessage* message, gconstpointer data, gsize size);
gchar
j_message_mockup_get_1(JMessage* message);
gint32
j_message_mockup_get_4(JMessage* message);
gint64
j_message_mockup_get_8(JMessage* message);
gpointer
j_message_mockup_get_n(JMessage* message, gsize size);
gchar const*
j_message_mockup_get_string(JMessage* message);
gboolean
j_message_mockup_send(JMessage* message, GSocketConnection* connection);
gboolean
j_message_mockup_receive(JMessage* message, GSocketConnection* connection);
void
j_message_mockup_add_operation(JMessage* message, gsize size);
JMessageType
j_message_mockup_get_type(JMessage const* message);
JMessageFlags
j_message_mockup_get_flags(JMessage const* message);
guint32
j_message_mockup_get_count(JMessage const* message);
gboolean
j_message_mockup_read(JMessage* message, GInputStream* stream);
gboolean
j_message_mockup_write(JMessage* message, GOutputStream* stream);
void
j_message_mockup_add_send(JMessage* message, gconstpointer data, guint64 size);
void
j_message_mockup_set_safety(JMessage* message, JSemantics* semantics);
void
j_message_mockup_force_safety(JMessage* message, gint safety);
static JMessage* server_reply_mockup;
JMessage*
_j_message_mockup_new_reply(JMessage* message);
#include "../../../server/server-db-exec.h"
JMessage*
j_message_mockup_new(JMessageType type, gsize size)
{
	JMessage* message;
	message = g_slice_new(JMessage);
	message->operation_count = 0;
	message->client_side = TRUE;
	message->type = type;
	message->data = g_byte_array_new();
	message->current = NULL;
	message->size_requested = size;
	message->size_used = 0;
	message->ref_count = 1;
	return message;
}
JMessage*
_j_message_mockup_new_reply(JMessage* message)
{
	JMessage* reply;
	myabort(!message);
	myabort(message->size_used != message->size_requested);
	reply = j_message_mockup_new(message->type, message->size_used);
	j_message_mockup_append_n(reply, message->data->data, message->data->len);
	reply->current = (gchar*)reply->data->data;
	return reply;
}
JMessage*
j_message_mockup_new_reply(JMessage* message_input)
{
	JMessage* message;
	guint operation_count;
	gpointer connection = (void*)TRUE;
	gint ret;
	JSemanticsSafety safety = J_SEMANTICS_SAFETY_NONE;
	JBackend* jd_db_backend = j_db_backend();
	myabort(!message_input);
	if (message_input->client_side)
	{
		message = _j_message_mockup_new_reply(message_input);
		message->client_side = FALSE;
		operation_count = message_input->operation_count;
		ret = db_server_message_exec(message->type, message, operation_count, jd_db_backend, safety, connection);
		if (!ret)
		{
			J_CRITICAL("mockup only implemented for db messages%d", 0);
			abort();
		}
		j_message_mockup_unref(message);
		message = _j_message_mockup_new_reply(server_reply_mockup);
		message->operation_count = server_reply_mockup->operation_count;
		j_message_mockup_unref(server_reply_mockup);
		return message;
	}
	else /*avoid infinite loop*/
	{
		return server_reply_mockup = j_message_mockup_ref(j_message_mockup_new(message_input->type, 0));
	}
}

JMessage*
j_message_mockup_ref(JMessage* message)
{
	myabort(!message);
	g_atomic_int_inc(&message->ref_count);
	return message;
}
void
j_message_mockup_unref(JMessage* message)
{
	if (message && g_atomic_int_dec_and_test(&message->ref_count))
	{
		g_byte_array_unref(message->data);
		g_slice_free(JMessage, message);
	}
}
gboolean
j_message_mockup_append_1(JMessage* message, gconstpointer data)
{

	return j_message_mockup_append_n(message, data, 1);
}
gboolean
j_message_mockup_append_4(JMessage* message, gconstpointer data)
{
	return j_message_mockup_append_n(message, data, 4);
}
gboolean
j_message_mockup_append_8(JMessage* message, gconstpointer data)
{
	return j_message_mockup_append_n(message, data, 8);
}
gboolean
j_message_mockup_append_n(JMessage* message, gconstpointer data, gsize size)
{
	myabort(!message);
	g_byte_array_append(message->data, data, size);
	message->size_used += size;
	myabort(message->size_used > message->size_requested);
	return TRUE;
}
gchar
j_message_mockup_get_1(JMessage* message)
{
	gchar result;
	myabort(!message);
	myabort(message->current - (gchar*)message->data->data + 1 > message->data->len);
	result = *(gchar*)(message->current);
	message->current++;
	return result;
}
gint32
j_message_mockup_get_4(JMessage* message)
{
	gint32 result;
	myabort(!message);
	myabort(message->current - (gchar*)message->data->data + 4 > message->data->len);
	result = *(gint32*)(message->current);
	message->current += 4;
	return result;
}
gint64
j_message_mockup_get_8(JMessage* message)
{
	gint64 result;
	myabort(!message);
	myabort(message->current - (gchar*)message->data->data + 8 > message->data->len);
	result = *(gint64*)(message->current);
	message->current += 8;
	return result;
}
gpointer
j_message_mockup_get_n(JMessage* message, gsize size)
{
	gpointer result;
	myabort(!message);
	myabort(message->current - (gchar*)message->data->data + size > message->data->len);
	result = message->current;
	message->current += size;
	return result;
}
gchar const*
j_message_mockup_get_string(JMessage* message)
{
	gchar* ptr_end;
	gchar* ptr = message->current;
	myabort(!message);
	ptr_end = (gchar*)message->data->data + message->data->len;
	while (ptr < ptr_end && *ptr)
	{
		ptr++;
	}
	myabort(ptr == ptr_end);
	return j_message_mockup_get_n(message, ptr - message->current + 1);
}
gboolean
j_message_mockup_send(JMessage* message, GSocketConnection* connection)
{
	myabort(!message);
	myabort(!connection);
	return TRUE;
}
gboolean
j_message_mockup_receive(JMessage* message, GSocketConnection* connection)
{
	myabort(!message);
	myabort(!connection);
	return TRUE;
}
void
j_message_mockup_add_operation(JMessage* message, gsize size)
{
	myabort(!message);
	message->size_requested += size;
	message->operation_count++;
}
JMessageType
j_message_mockup_get_type(JMessage const* message)
{
	myabort(!message);
	return message->type;
}
JMessageFlags
j_message_mockup_get_flags(JMessage const* message)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}
guint32
j_message_mockup_get_count(JMessage const* message)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}
gboolean
j_message_mockup_read(JMessage* message, GInputStream* stream)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}
gboolean
j_message_mockup_write(JMessage* message, GOutputStream* stream)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}

void
j_message_mockup_add_send(JMessage* message, gconstpointer data, guint64 size)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}

void
j_message_mockup_set_safety(JMessage* message, JSemantics* semantics)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}
void
j_message_mockup_force_safety(JMessage* message, gint safety)
{
	J_CRITICAL("mockup not implemented%d", 0);
	abort();
}
#pragma GCC diagnostic pop
