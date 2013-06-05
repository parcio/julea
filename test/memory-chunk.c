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

#include <jmemory-chunk-internal.h>

#include "test.h"

static
void
test_memory_chunk_new_free (void)
{
	JMemoryChunk* memory_chunk;

	memory_chunk = j_memory_chunk_new(J_MIB(50));
	j_memory_chunk_free(memory_chunk);
}

static
void
test_memory_chunk_get (void)
{
	JMemoryChunk* memory_chunk;
	gpointer ret;

	memory_chunk = j_memory_chunk_new(3);

	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret == NULL);

	j_memory_chunk_free(memory_chunk);
}

static
void
test_memory_chunk_release (void)
{
	JMemoryChunk* memory_chunk;
	gpointer ret1;
	gpointer ret2;

	memory_chunk = j_memory_chunk_new(1);

	ret1 = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret1 != NULL);
	ret2 = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret2 == NULL);

	j_memory_chunk_release(memory_chunk, ret1);

	ret1 = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret1 != NULL);
	ret2 = j_memory_chunk_get(memory_chunk, 1);
	g_assert(ret2 == NULL);

	j_memory_chunk_free(memory_chunk);
}

void
test_memory_chunk (void)
{
	g_test_add_func("/memory-chunk/new_free", test_memory_chunk_new_free);
	g_test_add_func("/memory-chunk/get", test_memory_chunk_get);
	g_test_add_func("/memory-chunk/release", test_memory_chunk_release);
}
