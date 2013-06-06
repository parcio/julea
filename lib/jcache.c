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

#include <jcache-internal.h>

#include <jcommon-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JCache Cache
 * @{
 **/

/**
 * A cache.
 */
struct JCache
{
	/**
	* The size.
	*/
	guint64 size;

	GHashTable* buffers;

	guint64 used;

	GMutex mutex[1];
};

/**
 * Creates a new cache.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCache* cache;
 *
 * cache = j_cache_new(1024);
 * \endcode
 *
 * \param size A size.
 *
 * \return A new cache. Should be freed with j_cache_free().
 **/
JCache*
j_cache_new (guint64 size)
{
	JCache* cache;

	g_return_val_if_fail(size > 0, NULL);

	j_trace_enter(G_STRFUNC);

	cache = g_slice_new(JCache);
	cache->size = size;
	cache->buffers = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
	cache->used = 0;

	g_mutex_init(cache->mutex);

	j_trace_leave(G_STRFUNC);

	return cache;
}

/**
 * Frees the memory allocated for the cache.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCache* cache;
 *
 * ...
 *
 * j_cache_free(cache);
 * \endcode
 *
 * \param cache A cache.
 **/
void
j_cache_free (JCache* cache)
{
	GHashTableIter iter[1];
	gpointer key;
	gpointer value;

	g_return_if_fail(cache != NULL);

	j_trace_enter(G_STRFUNC);

	g_hash_table_iter_init(iter, cache->buffers);

	while (g_hash_table_iter_next(iter, &key, &value))
	{
		g_free(key);
	}

	g_hash_table_unref(cache->buffers);

	g_mutex_clear(cache->mutex);

	g_slice_free(JCache, cache);

	j_trace_leave(G_STRFUNC);
}

/**
 * Gets a new segment from the cache.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCache* cache;
 *
 * ...
 *
 * j_cache_get(cache, 1024);
 * \endcode
 *
 * \param cache  A cache.
 * \param length A length.
 *
 * \return A pointer to a segment of the cache, NULL if not enough space is available.
 **/
gpointer
j_cache_get (JCache* cache, guint64 length)
{
	gpointer ret = NULL;

	g_return_val_if_fail(cache != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	g_mutex_lock(cache->mutex);

	if (cache->used + length > cache->size)
	{
		goto end;
	}

	ret = g_malloc(length);
	cache->used += length;

	g_hash_table_insert(cache->buffers, ret, GSIZE_TO_POINTER(length));

end:
	g_mutex_unlock(cache->mutex);

	j_trace_leave(G_STRFUNC);

	return ret;
}

void
j_cache_release (JCache* cache, gpointer data)
{
	gpointer size;

	g_return_if_fail(cache != NULL);
	g_return_if_fail(data != NULL);

	g_mutex_lock(cache->mutex);

	if ((size = g_hash_table_lookup(cache->buffers, data)) == NULL)
	{
		g_warn_if_reached();
		return;
	}

	g_hash_table_remove(cache->buffers, data);

	cache->used -= GPOINTER_TO_SIZE(size);
	g_free(data);

	g_mutex_unlock(cache->mutex);
}

/**
 * @}
 **/
