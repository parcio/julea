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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <jmemory-chunk.h>

#include <jtrace.h>

/**
 * \defgroup JMemoryChunk Cache
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

/**
 * Creates a new cache.
 *
 * \code
 * JMemoryChunk* cache;
 *
 * cache = j_memory_chunk_new(1024);
 * \endcode
 *
 * \param size A size.
 *
 * \return A new cache. Should be freed with j_memory_chunk_free().
 **/
JMemoryChunk*
j_memory_chunk_new (guint64 size)
{
	J_TRACE_FUNCTION(NULL);

	JMemoryChunk* cache;

	g_return_val_if_fail(size > 0, NULL);

	cache = g_slice_new(JMemoryChunk);
	cache->size = size;
	cache->data = g_malloc(cache->size);
	cache->current = cache->data;

	return cache;
}

/**
 * Frees the memory allocated for the cache.
 *
 * \code
 * JMemoryChunk* cache;
 *
 * ...
 *
 * j_memory_chunk_free(cache);
 * \endcode
 *
 * \param cache A cache.
 **/
void
j_memory_chunk_free (JMemoryChunk* cache)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(cache != NULL);

	if (cache->data != NULL)
	{
		g_free(cache->data);
	}

	g_slice_free(JMemoryChunk, cache);
}

/**
 * Gets a new segment from the cache.
 *
 * \code
 * JMemoryChunk* cache;
 *
 * ...
 *
 * j_memory_chunk_get(cache, 1024);
 * \endcode
 *
 * \param cache  A cache.
 * \param length A length.
 *
 * \return A pointer to a segment of the cache, NULL if not enough space is available.
 **/
gpointer
j_memory_chunk_get (JMemoryChunk* cache, guint64 length)
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
j_memory_chunk_reset (JMemoryChunk* cache)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(cache != NULL);

	cache->current = cache->data;
}

/**
 * @}
 **/
