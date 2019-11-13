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

#include <jmemory-chunk.h>

#include "test.h"

static
void
test_memory_chunk_new_free (void)
{
	JMemoryChunk* memory_chunk;

	memory_chunk = j_memory_chunk_new(42);
	g_assert_true(memory_chunk != NULL);

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
	g_assert_true(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret == NULL);

	j_memory_chunk_free(memory_chunk);
}

static
void
test_memory_chunk_reset (void)
{
	JMemoryChunk* memory_chunk;
	gpointer ret;

	memory_chunk = j_memory_chunk_new(1);

	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret == NULL);

	j_memory_chunk_reset(memory_chunk);

	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret != NULL);
	ret = j_memory_chunk_get(memory_chunk, 1);
	g_assert_true(ret == NULL);

	j_memory_chunk_free(memory_chunk);
}

void
test_memory_chunk (void)
{
	g_test_add_func("/memory-chunk/new_free", test_memory_chunk_new_free);
	g_test_add_func("/memory-chunk/get", test_memory_chunk_get);
	g_test_add_func("/memory-chunk/reset", test_memory_chunk_reset);
}
