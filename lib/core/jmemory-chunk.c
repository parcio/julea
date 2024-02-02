/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2023 Michael Kuhn
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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <jmemory-chunk.h>

#include <jtrace.h>

/**
 * \addtogroup JMemoryChunk
 *
 * @{
 **/

/**
 * A cache.
 */
struct JMemoryChunk
{
	/**
	* The size.
	*/
	guint64 size;

	/**
	* The data.
	*/
	gchar* data;

	/**
	* The current position within #data.
	*/
	gchar* current;
};

JMemoryChunk*
j_memory_chunk_new(guint64 size)
{
	J_TRACE_FUNCTION(NULL);

	JMemoryChunk* cache;

	g_return_val_if_fail(size > 0, NULL);

	cache = g_new(JMemoryChunk, 1);
	cache->size = size;
	cache->data = g_malloc(cache->size);
	cache->current = cache->data;

	return cache;
}

void
j_memory_chunk_free(JMemoryChunk* cache)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(cache != NULL);

	if (cache->data != NULL)
	{
		g_free(cache->data);
	}

	g_free(cache);
}

gpointer
j_memory_chunk_get(JMemoryChunk* cache, guint64 length)
{
	J_TRACE_FUNCTION(NULL);

	gpointer ret = NULL;

	g_return_val_if_fail(cache != NULL, NULL);

	if (cache->current + length > cache->data + cache->size)
	{
		return NULL;
	}

	ret = cache->current;
	cache->current += length;

	return ret;
}

void
j_memory_chunk_reset(JMemoryChunk* cache)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(cache != NULL);

	cache->current = cache->data;
}

/**
 * @}
 **/
