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

#include "test.h"

static
void
test_list_fixture_setup (JList** list, gconstpointer data)
{
	*list = j_list_new(g_free);
}

static
void
test_list_fixture_teardown (JList** list, gconstpointer data)
{
	j_list_unref(*list);
}

static
void
test_list_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JList* list;

		list = j_list_new(NULL);
		g_assert(list != NULL);
		j_list_unref(list);
	}
}

static
void
test_list_length (JList** list, gconstpointer data)
{
	guint const n = 100000;
	guint l;

	for (guint i = 0; i < n; i++)
	{
		j_list_append(*list, g_strdup("append"));
	}

	l = j_list_length(*list);
	g_assert_cmpuint(l, ==, n);
}

static
void
test_list_append (JList** list, gconstpointer data)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		j_list_append(*list, g_strdup("append"));
	}
}

static
void
test_list_prepend (JList** list, gconstpointer data)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		j_list_prepend(*list, g_strdup("append"));
	}
}

static
void
test_list_get (JList** list, gconstpointer data)
{
	gchar const* s;

	j_list_append(*list, g_strdup("0"));
	j_list_append(*list, g_strdup("1"));
	j_list_append(*list, g_strdup("-2"));
	j_list_append(*list, g_strdup("-1"));

	s = j_list_get_first(*list);
	g_assert_cmpstr(s, ==, "0");
	s = j_list_get_last(*list);
	g_assert_cmpstr(s, ==, "-1");
}

void
test_list (void)
{
	g_test_add_func("/list/new_free", test_list_new_free);
	g_test_add("/list/length", JList*, NULL, test_list_fixture_setup, test_list_length, test_list_fixture_teardown);
	g_test_add("/list/append", JList*, NULL, test_list_fixture_setup, test_list_append, test_list_fixture_teardown);
	g_test_add("/list/prepend", JList*, NULL, test_list_fixture_setup, test_list_prepend, test_list_fixture_teardown);
	g_test_add("/list/get", JList*, NULL, test_list_fixture_setup, test_list_get, test_list_fixture_teardown);
}
