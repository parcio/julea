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

#include <jcache-internal.h>

#include "test.h"

static
void
test_cache_new_free (JCacheImplementation implementation)
{
	JCache* cache;

	cache = j_cache_new(implementation, J_MIB(50));
	j_cache_free(cache);
}

static
void
test_cache_get (JCacheImplementation implementation)
{
	JCache* cache;
	gpointer ret;

	cache = j_cache_new(implementation, 3);

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
test_cache_release (JCacheImplementation implementation)
{
	JCache* cache;
	gpointer ret1;
	gpointer ret2;

	cache = j_cache_new(implementation, 1);

	ret1 = j_cache_get(cache, 1);
	g_assert(ret1 != NULL);
	ret2 = j_cache_get(cache, 1);
	g_assert(ret2 == NULL);

	j_cache_release(cache, ret1);

	ret1 = j_cache_get(cache, 1);
	g_assert(ret1 != NULL);
	ret2 = j_cache_get(cache, 1);
	g_assert(ret2 == NULL);

	j_cache_free(cache);
}

static
void
test_cache_chunk_new_free (void)
{
	test_cache_new_free(J_CACHE_CHUNK);
}

static
void
test_cache_chunk_get (void)
{
	test_cache_get(J_CACHE_CHUNK);
}

static
void
test_cache_chunk_release (void)
{
	test_cache_release(J_CACHE_CHUNK);
}

static
void
test_cache_malloc_new_free (void)
{
	test_cache_new_free(J_CACHE_MALLOC);
}

static
void
test_cache_malloc_get (void)
{
	test_cache_get(J_CACHE_MALLOC);
}

static
void
test_cache_malloc_release (void)
{
	test_cache_release(J_CACHE_MALLOC);
}

void
test_cache (void)
{
	g_test_add_func("/cache/chunk/new_free", test_cache_chunk_new_free);
	g_test_add_func("/cache/chunk/get", test_cache_chunk_get);
	g_test_add_func("/cache/chunk/release", test_cache_chunk_release);

	g_test_add_func("/cache/malloc/new_free", test_cache_malloc_new_free);
	g_test_add_func("/cache/malloc/get", test_cache_malloc_get);
	g_test_add_func("/cache/malloc/release", test_cache_malloc_release);
}
