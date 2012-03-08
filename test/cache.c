/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <jcache-internal.h>

#include "test.h"

static
void
test_cache_new_free (void)
{
	JCache* cache;

	cache = j_cache_new(J_MIB(50));
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
	g_assert(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert(ret == NULL);

	j_cache_free(cache);
}

static
void
test_cache_put (void)
{
	JCache* cache;
	gpointer ret;
	gchar dummy[1];

	cache = j_cache_new(3);

	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret != NULL);
	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret != NULL);
	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret != NULL);
	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret == NULL);

	j_cache_free(cache);
}

static
void
test_cache_clear (void)
{
	JCache* cache;
	gpointer ret;
	gchar dummy[1];

	cache = j_cache_new(1);

	ret = j_cache_get(cache, 1);
	g_assert(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert(ret == NULL);

	j_cache_clear(cache);

	ret = j_cache_get(cache, 1);
	g_assert(ret != NULL);
	ret = j_cache_get(cache, 1);
	g_assert(ret == NULL);

	j_cache_clear(cache);

	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret != NULL);
	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret == NULL);

	j_cache_clear(cache);

	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret != NULL);
	ret = j_cache_put(cache, dummy, 1);
	g_assert(ret == NULL);

	j_cache_free(cache);
}

void
test_cache (void)
{
	g_test_add_func("/cache/new_free", test_cache_new_free);
	g_test_add_func("/cache/get", test_cache_get);
	g_test_add_func("/cache/put", test_cache_put);
	g_test_add_func("/cache/clear", test_cache_clear);
}
