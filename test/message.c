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

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <string.h>

#include <julea.h>

#include <jmessage-internal.h>

#include "test.h"

static
void
test_message_new_ref_unref (void)
{
	JMessage* message;

	message = j_message_new(J_MESSAGE_NONE, 0);
	g_assert(message != NULL);
	j_message_ref(message);
	j_message_unref(message);
	j_message_unref(message);
}

static
void
test_message_header (void)
{
	JMessage* message;

	message = j_message_new(J_MESSAGE_READ, 42);
	g_assert(message != NULL);

	j_message_add_operation(message, 0);
	j_message_add_operation(message, 0);
	j_message_add_operation(message, 0);

	g_assert(j_message_get_type(message) == J_MESSAGE_READ);
	g_assert(j_message_get_type_modifier(message) == 0);
	g_assert_cmpuint(j_message_get_count(message), ==, 3);

	j_message_unref(message);
}

static
void
test_message_append (void)
{
	JMessage* message;
	gboolean ret;
	guint64 dummy = 42;

	message = j_message_new(J_MESSAGE_NONE, 20);
	g_assert(message != NULL);

	ret = j_message_append_1(message, &dummy);
	g_assert(ret);
	ret = j_message_append_4(message, &dummy);
	g_assert(ret);
	ret = j_message_append_8(message, &dummy);
	g_assert(ret);
	ret = j_message_append_n(message, &dummy, 7);
	g_assert(ret);
	/*
	 * FIXME
	ret = j_message_append_1(message, &dummy);
	g_assert(!ret);
	*/

	j_message_unref(message);
}

static
void
test_message_write_read (void)
{
	JMessage* message[2];
	GOutputStream* output;
	GInputStream* input;
	gboolean ret;
	gchar dummy_1 = 23;
	guint32 dummy_4 = 42;
	guint64 dummy_8 = 2342;
	gchar const* dummy_str = "42";

	output = g_memory_output_stream_new(NULL, 0, g_realloc, g_free);
	input = g_memory_input_stream_new();

	message[0] = j_message_new(J_MESSAGE_NONE, 16);
	g_assert(message[0] != NULL);
	message[1] = j_message_new(J_MESSAGE_NONE, 0);
	g_assert(message[1] != NULL);

	ret = j_message_append_1(message[0], &dummy_1);
	g_assert(ret);
	ret = j_message_append_4(message[0], &dummy_4);
	g_assert(ret);
	ret = j_message_append_8(message[0], &dummy_8);
	g_assert(ret);
	ret = j_message_append_n(message[0], dummy_str, strlen(dummy_str) + 1);
	g_assert(ret);

	ret = j_message_write(message[0], output);
	g_assert(ret);

	g_memory_input_stream_add_data(
		G_MEMORY_INPUT_STREAM(input),
		g_memory_output_stream_get_data(G_MEMORY_OUTPUT_STREAM(output)),
		g_memory_output_stream_get_data_size(G_MEMORY_OUTPUT_STREAM(output)),
		NULL
	);

	ret = j_message_read(message[1], input);
	g_assert(ret);

	dummy_1 = j_message_get_1(message[1]);
	g_assert_cmpint(dummy_1, ==, 23);
	dummy_4 = j_message_get_4(message[1]);
	g_assert_cmpuint(dummy_4, ==, 42);
	dummy_8 = j_message_get_8(message[1]);
	g_assert_cmpuint(dummy_8, ==, 2342);
	dummy_str = j_message_get_string(message[1]);
	g_assert_cmpstr(dummy_str, ==, "42");

	j_message_unref(message[0]);
	j_message_unref(message[1]);

	g_object_unref(input);
	g_object_unref(output);
}

void
test_message (void)
{
	g_test_add_func("/message/new_ref_unref", test_message_new_ref_unref);
	g_test_add_func("/message/header", test_message_header);
	g_test_add_func("/message/append", test_message_append);
	g_test_add_func("/message/write_read", test_message_write_read);
}
