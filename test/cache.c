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

#include <jcache.h>

#include "test.h"

static
void
test_cache_new_free (void)
{
	JCache* cache;

	cache = j_cache_new(42);
	g_assert_true(cache != NULL);

	j_cache_free(cache);
}

static
void
test_cache_get (void)
{
	JCache* cache;
	gpointer ret;

	cache = j_cache_new(3);

	ret = j_cache_get(cache, 1);
	g_assert_true(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert_true(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert_true(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert_true(ret == NULL);

	j_cache_free(cache);
}

static
void
test_cache_release (void)
{
	JCache* cache;
	gpointer ret1;
	gpointer ret2;

	cache = j_cache_new(1);

	ret1 = j_cache_get(cache, 1);
	g_assert_true(ret1 != NULL);
	ret2 = j_cache_get(cache, 1);
	g_assert_true(ret2 == NULL);

	j_cache_release(cache, ret1);

	ret1 = j_cache_get(cache, 1);
	g_assert_true(ret1 != NULL);
	ret2 = j_cache_get(cache, 1);
	g_assert_true(ret2 == NULL);

	j_cache_free(cache);
}

void
test_cache (void)
{
	g_test_add_func("/cache/new_free", test_cache_new_free);
	g_test_add_func("/cache/get", test_cache_get);
	g_test_add_func("/cache/release", test_cache_release);
}
