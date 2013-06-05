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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <jmemory-chunk-internal.h>

#include <jcommon-internal.h>
#include <jtrace-internal.h>

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

	GHashTable* buffers;

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
 * \author Michael Kuhn
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
	JMemoryChunk* cache;

	g_return_val_if_fail(size > 0, NULL);

	j_trace_enter(G_STRFUNC);

	cache = g_slice_new(JMemoryChunk);
	cache->size = size;
	cache->buffers = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
	cache->data = g_malloc(cache->size);
	cache->current = cache->data;

	j_trace_leave(G_STRFUNC);

	return cache;
}

/**
 * Frees the memory allocated for the cache.
 *
 * \author Michael Kuhn
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
	g_return_if_fail(cache != NULL);

	j_trace_enter(G_STRFUNC);

	g_hash_table_unref(cache->buffers);

	if (cache->data != NULL)
	{
		g_free(cache->data);
	}

	g_slice_free(JMemoryChunk, cache);

	j_trace_leave(G_STRFUNC);
}

/**
 * Gets a new segment from the cache.
 *
 * \author Michael Kuhn
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
	gpointer ret = NULL;

	g_return_val_if_fail(cache != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	if (cache->current + length > cache->data + cache->size)
	{
		goto end;
	}

	ret = cache->current;
	cache->current += length;

	g_hash_table_insert(cache->buffers, ret, GSIZE_TO_POINTER(length));

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

void
j_memory_chunk_release (JMemoryChunk* cache, gpointer data)
{
	gpointer size;

	g_return_if_fail(cache != NULL);
	g_return_if_fail(data != NULL);

	if ((size = g_hash_table_lookup(cache->buffers, data)) == NULL)
	{
		g_warn_if_reached();
		return;
	}

	g_hash_table_remove(cache->buffers, data);

	if (g_hash_table_size(cache->buffers) == 0)
	{
		cache->current = cache->data;
	}
}

/**
 * @}
 **/
