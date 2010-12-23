/*
 * Copyright (c) 2010 Michael Kuhn
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

#include <jbson.h>
#include <jbson-iterator.h>

static void
test_bson_iterator_fixture_setup (JBSONIterator** iterator, gconstpointer data)
{
	JBSON* jbson;

	jbson = j_bson_new();

	j_bson_append_new_id(jbson, "_id");
	j_bson_append_int(jbson, "int", 23);
	j_bson_append_str(jbson, "str", "42");

	*iterator = j_bson_iterator_new(jbson);

	j_bson_unref(jbson);
}

static void
test_bson_iterator_fixture_teardown (JBSONIterator** iterator, gconstpointer data)
{
	j_bson_iterator_free(*iterator);
}

static void
test_bson_iterator_new_free (gpointer* fixture, gconstpointer data)
{
	const guint n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JBSON* jbson;
		JBSONIterator* iterator;

		jbson = j_bson_new();
		g_assert(jbson != NULL);
		iterator = j_bson_iterator_new(jbson);
		g_assert(iterator != NULL);
		j_bson_iterator_free(iterator);
		j_bson_unref(jbson);
	}
}

static void
test_bson_iterator_next_get (JBSONIterator** iterator, gconstpointer data)
{
	const gchar* k;
	const gchar* s;
	gint i;
	gboolean next;

	next = j_bson_iterator_next(*iterator);
	g_assert(next);

	k = j_bson_iterator_get_key(*iterator);
	j_bson_iterator_get_id(*iterator);
	g_assert_cmpstr(k, ==, "_id");

	next = j_bson_iterator_next(*iterator);
	g_assert(next);

	k = j_bson_iterator_get_key(*iterator);
	i = j_bson_iterator_get_int(*iterator);
	g_assert_cmpstr(k, ==, "int");
	g_assert_cmpint(i, ==, 23);

	next = j_bson_iterator_next(*iterator);
	g_assert(next);

	k = j_bson_iterator_get_key(*iterator);
	s = j_bson_iterator_get_string(*iterator);
	g_assert_cmpstr(k, ==, "str");
	g_assert_cmpstr(s, ==, "42");

	next = j_bson_iterator_next(*iterator);
	g_assert(!next);
}

int main (int argc, char** argv)
{
	g_test_init(&argc, &argv, NULL);

	g_test_add("/julea/bson-iterator/new_free", gpointer, NULL, NULL, test_bson_iterator_new_free, NULL);

	g_test_add("/julea/bson-iterator/next_get", JBSONIterator*, NULL, test_bson_iterator_fixture_setup, test_bson_iterator_next_get, test_bson_iterator_fixture_teardown);

	return g_test_run();
}
