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

#include "test.h"

static
void
test_item_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JItem* item;

		item = j_item_new("test-item");
		g_assert(item != NULL);
		j_item_unref(item);
	}
}

static
void
test_item_name (void)
{
	JItem* item;

	item = j_item_new("test-item");

	g_assert_cmpstr(j_item_get_name(item), ==, "test-item");

	j_item_unref(item);
}

static
void
test_item_size (void)
{
	JItem* item;

	item = j_item_new("test-item");

	g_assert_cmpuint(j_item_get_size(item), ==, 0);

	j_item_unref(item);
}

static
void
test_item_modification_time (void)
{
	JItem* item;

	item = j_item_new("test-item");

	g_assert_cmpuint(j_item_get_modification_time(item), >, 0);

	j_item_unref(item);
}

void
test_item (void)
{
	g_test_add_func("/item/new_free", test_item_new_free);
	g_test_add_func("/item/name", test_item_name);
	g_test_add_func("/item/size", test_item_size);
	g_test_add_func("/item/modification_time", test_item_modification_time);
}
