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

#include <jbson.h>

static void
test_bson_fixture_setup (JBSON** bson, gconstpointer data)
{
	*bson = j_bson_new();
}

static void
test_bson_fixture_teardown (JBSON** bson, gconstpointer data)
{
	j_bson_unref(*bson);
}

static void
test_bson_new_unref (gpointer* fixture, gconstpointer data)
{
	const guint n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JBSON* bson;

		bson = j_bson_new();
		g_assert(bson != NULL);
		j_bson_unref(bson);
	}
}

static void
test_bson_append (JBSON** bson, gconstpointer data)
{
	const guint n = 3;

	for (guint i = 0; i < n; i++)
	{
		JBSON* b;
		gchar* name;

		b = j_bson_new();
		name = g_strdup_printf("%u", i);

		j_bson_append_boolean(b, "_boolean", i);
		j_bson_append_int32(b, "_int32", i);
		j_bson_append_int64(b, "_int64", i);
		j_bson_append_string(b, "_string", name);

		j_bson_append_new_object_id(*bson, "_id");
		// FIXME append_id
		j_bson_append_boolean(*bson, "boolean", i);
		j_bson_append_int32(*bson, "int32", i);
		j_bson_append_int64(*bson, "int64", i);
		j_bson_append_string(*bson, "string", name);
		j_bson_append_document(*bson, name, b);

		g_free(name);
		j_bson_unref(b);
	}
}

static void
test_bson_data (JBSON** bson, gconstpointer data)
{
	gconstpointer b;

	b = j_bson_data(*bson);
	g_assert(b != NULL);
	b = j_bson_data(*bson);
	g_assert(b != NULL);
}

int main (int argc, char** argv)
{
	g_test_init(&argc, &argv, NULL);

	g_test_add("/julea/bson/new_unref", gpointer, NULL, NULL, test_bson_new_unref, NULL);

	g_test_add("/julea/bson/append", JBSON*, NULL, test_bson_fixture_setup, test_bson_append, test_bson_fixture_teardown);
	g_test_add("/julea/bson/get", JBSON*, NULL, test_bson_fixture_setup, test_bson_data, test_bson_fixture_teardown);

	return g_test_run();
}
