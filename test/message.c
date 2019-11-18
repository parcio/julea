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

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <string.h>

#include <julea.h>

#include <jmessage.h>

#include "test.h"

static
void
test_message_new_ref_unref (void)
{
	g_autoptr(JMessage) message = NULL;

	message = j_message_new(J_MESSAGE_NONE, 0);
	g_assert_true(message != NULL);
	j_message_ref(message);
	j_message_unref(message);
}

static
void
test_message_header (void)
{
	g_autoptr(JMessage) message = NULL;

	message = j_message_new(J_MESSAGE_OBJECT_READ, 42);
	g_assert_true(message != NULL);

	j_message_add_operation(message, 0);
	j_message_add_operation(message, 0);
	j_message_add_operation(message, 0);

	g_assert_true(j_message_get_type(message) == J_MESSAGE_OBJECT_READ);
	g_assert_cmpuint(j_message_get_count(message), ==, 3);
}

static
void
test_message_append (void)
{
	JMessage* message;
	gboolean ret;
	guint64 dummy = 42;

	message = j_message_new(J_MESSAGE_NONE, 20);
	g_assert_true(message != NULL);

	ret = j_message_append_1(message, &dummy);
	g_assert_true(ret);
	ret = j_message_append_4(message, &dummy);
	g_assert_true(ret);
	ret = j_message_append_8(message, &dummy);
	g_assert_true(ret);
	ret = j_message_append_n(message, &dummy, 7);
	g_assert_true(ret);

	j_message_unref(message);

	message = j_message_new(J_MESSAGE_NONE, 0);
	g_assert_true(message != NULL);

	/* Preallocation kicks in after the tenth operation */
	for (guint i = 0; i <= 10; i++)
	{
		j_message_add_operation(message, 1);
		ret = j_message_append_1(message, &dummy);
		g_assert_true(ret);
	}

	/* FIXME
	ret = j_message_append_1(message, &dummy);
	g_assert_true(!ret);
	*/

	j_message_unref(message);
}

static
void
test_message_write_read (void)
{
	g_autoptr(JMessage) message_recv = NULL;
	g_autoptr(JMessage) message_send = NULL;
	g_autoptr(GOutputStream) output = NULL;
	g_autoptr(GInputStream) input = NULL;
	gboolean ret;
	gchar dummy_1 = 23;
	guint32 dummy_4 = 42;
	guint64 dummy_8 = 2342;
	gchar const* dummy_str = "42";

	output = g_memory_output_stream_new(NULL, 0, g_realloc, g_free);
	input = g_memory_input_stream_new();

	message_send = j_message_new(J_MESSAGE_NONE, 18);
	g_assert_true(message_send != NULL);
	message_recv = j_message_new(J_MESSAGE_NONE, 0);
	g_assert_true(message_recv != NULL);

	ret = j_message_append_1(message_send, &dummy_1);
	g_assert_true(ret);
	ret = j_message_append_4(message_send, &dummy_4);
	g_assert_true(ret);
	ret = j_message_append_8(message_send, &dummy_8);
	g_assert_true(ret);
	ret = j_message_append_n(message_send, dummy_str + 1, strlen(dummy_str + 1) + 1);
	g_assert_true(ret);
	ret = j_message_append_string(message_send, dummy_str);
	g_assert_true(ret);

	ret = j_message_write(message_send, output);
	g_assert_true(ret);

	g_memory_input_stream_add_data(
		G_MEMORY_INPUT_STREAM(input),
		g_memory_output_stream_get_data(G_MEMORY_OUTPUT_STREAM(output)),
		g_memory_output_stream_get_data_size(G_MEMORY_OUTPUT_STREAM(output)),
		NULL
	);

	ret = j_message_read(message_recv, input);
	g_assert_true(ret);

	dummy_1 = j_message_get_1(message_recv);
	g_assert_cmpint(dummy_1, ==, 23);
	dummy_4 = j_message_get_4(message_recv);
	g_assert_cmpuint(dummy_4, ==, 42);
	dummy_8 = j_message_get_8(message_recv);
	g_assert_cmpuint(dummy_8, ==, 2342);
	dummy_str = j_message_get_n(message_recv, 2);
	g_assert_cmpstr(dummy_str, ==, "2");
	dummy_str = j_message_get_string(message_recv);
	g_assert_cmpstr(dummy_str, ==, "42");
}

static
void
test_message_semantics (void)
{
	g_autoptr(JMessage) message = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	g_autoptr(JSemantics) msg_semantics = NULL;

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_POSIX);
	g_assert_true(semantics != NULL);

	message = j_message_new(J_MESSAGE_NONE, 0);
	g_assert_true(message != NULL);

	j_message_set_semantics(message, semantics);
	msg_semantics = j_message_get_semantics(message);

	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_ATOMICITY), ==, j_semantics_get(msg_semantics, J_SEMANTICS_ATOMICITY));
	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_CONCURRENCY), ==, j_semantics_get(msg_semantics, J_SEMANTICS_CONCURRENCY));
	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_CONSISTENCY), ==, j_semantics_get(msg_semantics, J_SEMANTICS_CONSISTENCY));
	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_ORDERING), ==, j_semantics_get(msg_semantics, J_SEMANTICS_ORDERING));
	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY), ==, j_semantics_get(msg_semantics, J_SEMANTICS_PERSISTENCY));
	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_SAFETY), ==, j_semantics_get(msg_semantics, J_SEMANTICS_SAFETY));
	g_assert_cmpint(j_semantics_get(semantics, J_SEMANTICS_SECURITY), ==, j_semantics_get(msg_semantics, J_SEMANTICS_SECURITY));
}

void
test_message (void)
{
	g_test_add_func("/message/new_ref_unref", test_message_new_ref_unref);
	g_test_add_func("/message/header", test_message_header);
	g_test_add_func("/message/append", test_message_append);
	g_test_add_func("/message/write_read", test_message_write_read);
	g_test_add_func("/message/semantics", test_message_semantics);
}
