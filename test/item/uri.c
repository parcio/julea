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

#include <julea.h>
#include <julea-item.h>

#include "test.h"

static
void
test_uri_new_free (void)
{
	g_autoptr(JURI) uri = NULL;

	uri = j_uri_new("julea://JULEA");
}

static
void
test_uri_valid (void)
{
	JURI* uri;

	uri = j_uri_new("julea://");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, NULL);
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JUL.EA");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "JUL.EA");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA/foo");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, "foo");
	j_uri_free(uri);

	uri = j_uri_new("julea://JULEA/fo.o");
	g_assert_cmpstr(j_uri_get_collection_name(uri), ==, "JULEA");
	g_assert_cmpstr(j_uri_get_item_name(uri), ==, "fo.o");
	j_uri_free(uri);
}

static
void
test_uri_invalid (void)
{
	JURI* uri;

	uri = j_uri_new("file://JULEA");
	g_assert_true(uri == NULL);

	uri = j_uri_new("julea:/JULEA");
	g_assert_true(uri == NULL);

	uri = j_uri_new("julea//JULEA");
	g_assert_true(uri == NULL);

	uri = j_uri_new("Julea://JULEA");
	g_assert_true(uri == NULL);

	uri = j_uri_new("JULEA://JULEA");
	g_assert_true(uri == NULL);

	uri = j_uri_new("julea://JULEA/foo/bar");
	g_assert_true(uri == NULL);

	uri = j_uri_new("julea://JULEA/");
	g_assert_true(uri == NULL);

	uri = j_uri_new("julea://JULEA/foo/");
	g_assert_true(uri == NULL);
}

static
void
test_uri_create_delete (void)
{
	JURI* uri;
	gboolean ret;

	uri = j_uri_new("julea://uri-create-0/uri-create-0");
	ret = j_uri_create(uri, FALSE, NULL);
	g_assert_false(ret);
	j_uri_free(uri);

	uri = j_uri_new("julea://uri-create-1");
	ret = j_uri_create(uri, FALSE, NULL);
	g_assert_true(ret);
	ret = j_uri_delete(uri, NULL);
	g_assert_true(ret);
	j_uri_free(uri);

	uri = j_uri_new("julea://uri-create-2/uri-create-2");
	ret = j_uri_create(uri, TRUE, NULL);
	g_assert_true(ret);
	ret = j_uri_delete(uri, NULL);
	g_assert_true(ret);
	j_uri_free(uri);
}

static
void
test_uri_get (void)
{
	JURI* uri;
	gboolean ret;

	uri = j_uri_new("julea://uri-get-0/uri-get-0");
	j_uri_create(uri, TRUE, NULL);
	j_uri_free(uri);

	uri = j_uri_new("julea://uri-get-0/uri-get-0");
	ret = j_uri_get(uri, NULL);
	g_assert_true(ret);
	j_uri_free(uri);
}

void
test_uri (void)
{
	g_test_add_func("/item/uri/new_free", test_uri_new_free);
	g_test_add_func("/item/uri/valid", test_uri_valid);
	g_test_add_func("/item/uri/invalid", test_uri_invalid);
	g_test_add_func("/item/uri/create_delete", test_uri_create_delete);
	g_test_add_func("/item/uri/get", test_uri_get);
}
