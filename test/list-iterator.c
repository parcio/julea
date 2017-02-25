/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

#include <julea.h>

#include "test.h"

static
void
test_list_iterator_fixture_setup (JListIterator** iterator, gconstpointer data)
{
	JList* list;

	(void)data;

	list = j_list_new(g_free);

	j_list_append(list, g_strdup("0"));
	j_list_append(list, g_strdup("1"));
	j_list_append(list, g_strdup("2"));

	*iterator = j_list_iterator_new(list);

	j_list_unref(list);
}

static
void
test_list_iterator_fixture_teardown (JListIterator** iterator, gconstpointer data)
{
	(void)data;

	j_list_iterator_free(*iterator);
}

static
void
test_list_iterator_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JList* list;
		JListIterator* iterator;

		list = j_list_new(NULL);
		g_assert(list != NULL);

		iterator = j_list_iterator_new(list);
		g_assert(iterator != NULL);

		j_list_unref(list);
		j_list_iterator_free(iterator);
	}
}

static
void
test_list_iterator_next_get (JListIterator** iterator, gconstpointer data)
{
	gchar const* s;
	gboolean next;

	(void)data;

	next = j_list_iterator_next(*iterator);
	g_assert(next);

	s = j_list_iterator_get(*iterator);
	g_assert_cmpstr(s, ==, "0");

	next = j_list_iterator_next(*iterator);
	g_assert(next);

	s = j_list_iterator_get(*iterator);
	g_assert_cmpstr(s, ==, "1");

	next = j_list_iterator_next(*iterator);
	g_assert(next);

	s = j_list_iterator_get(*iterator);
	g_assert_cmpstr(s, ==, "2");

	next = j_list_iterator_next(*iterator);
	g_assert(!next);
}

void
test_list_iterator (void)
{
	g_test_add_func("/list-iterator/new_free", test_list_iterator_new_free);
	g_test_add("/list-iterator/next_get", JListIterator*, NULL, test_list_iterator_fixture_setup, test_list_iterator_next_get, test_list_iterator_fixture_teardown);
}
