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

#include <julea.h>

#include <juri.h>

#include "test.h"

static
void
test_uri_new_free (void)
{
	JURI* uri;

	uri = j_uri_new("julea://JULEA");
	j_uri_free(uri);
}

static
void
test_uri_valid (void)
{
	JURI* uri;

	uri = j_uri_new("julea://");
	g_assert_cmpstr(j_uri_get_store_name(uri), ==, NULL);
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, NULL);
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA");
	g_assert_cmpstr(j_uri_get_store_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, NULL);
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA/foo");
	g_assert_cmpstr(j_uri_get_store_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "foo");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA/foo/bar");
	g_assert_cmpstr(j_uri_get_store_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "foo");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, "bar");
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA/fo.o");
	g_assert_cmpstr(j_uri_get_store_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "fo.o");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA/fo.o/ba.r");
	g_assert_cmpstr(j_uri_get_store_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "fo.o");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, "ba.r");
	j_uri_free(uri);
}

static
void
test_uri_invalid (void)
{
	JURI* uri;

	uri = j_uri_new("file://JULEA");
	g_assert(uri == NULL);

	uri = j_uri_new("julea:/JULEA");
	g_assert(uri == NULL);

	uri = j_uri_new("julea//JULEA");
	g_assert(uri == NULL);

	uri = j_uri_new("Julea://JULEA");
	g_assert(uri == NULL);

	uri = j_uri_new("JULEA://JULEA");
	g_assert(uri == NULL);

	uri = j_uri_new("julea://JULEA/foo/bar/42");
	g_assert(uri == NULL);

	uri = j_uri_new("julea://JULEA/");
	g_assert(uri == NULL);

	uri = j_uri_new("julea://JULEA/foo/");
	g_assert(uri == NULL);

	uri = j_uri_new("julea://JUL.EA");
	g_assert(uri == NULL);

	uri = j_uri_new("julea://JUL.EA/foo");
	g_assert(uri == NULL);

	uri = j_uri_new("julea://JUL.EA/foo/bar");
	g_assert(uri == NULL);
}

void
test_uri (void)
{
	g_test_add_func("/uri/new_free", test_uri_new_free);
	g_test_add_func("/uri/valid", test_uri_valid);
	g_test_add_func("/uri/invalid", test_uri_invalid);
}
