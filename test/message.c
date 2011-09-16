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

#include <glib.h>

#include <julea.h>

#include <jmessage.h>

#include "test.h"

static
void
test_message_new_free (void)
{
	JMessage* message;

	message = j_message_new(0, J_MESSAGE_OPERATION_NONE, 0);
	g_assert(message != NULL);
	j_message_free(message);
}

static
void
test_message_header (void)
{
	JMessage* message;

	message = j_message_new(42, J_MESSAGE_OPERATION_READ, 23);
	g_assert(message != NULL);

	g_assert(j_message_data(message) != NULL);
	g_assert_cmpuint(j_message_length(message), ==, 42 + sizeof(JMessageHeader));

	g_assert(j_message_operation_type(message) == J_MESSAGE_OPERATION_READ);
	g_assert_cmpuint(j_message_operation_count(message), ==, 23);

	j_message_free(message);
}

static
void
test_message_append (void)
{
	JMessage* message;
	gboolean ret;
	guint64 dummy = 42;

	message = j_message_new(20, J_MESSAGE_OPERATION_NONE, 0);
	g_assert(message != NULL);

	ret = j_message_append_1(message, &dummy);
	g_assert(ret);
	ret = j_message_append_4(message, &dummy);
	g_assert(ret);
	ret = j_message_append_8(message, &dummy);
	g_assert(ret);
	ret = j_message_append_n(message, &dummy, 7);
	g_assert(ret);

	j_message_free(message);
}

void
test_message (void)
{
	g_test_add_func("/julea/message/new_free", test_message_new_free);
	g_test_add_func("/julea/message/header", test_message_header);
	g_test_add_func("/julea/message/append", test_message_append);
}
