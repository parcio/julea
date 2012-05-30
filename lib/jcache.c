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

/**
 * \file
 **/

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	cache = g_slice_new(JCache);
	cache->size = size;
	cache->data = NULL;
	cache->current = NULL;

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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
	g_return_if_fail(cache != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (cache->data != NULL)
	{
		g_free(cache->data);
	}

	g_slice_free(JCache, cache);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (G_UNLIKELY(cache->data == NULL))
	{
		cache->data = g_malloc(cache->size);
		cache->current = cache->data;
	}

	if (cache->current + length > cache->data + cache->size)
	{
		goto end;
	}

	ret = cache->current;
	cache->current += length;

end:
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

/**
 * Puts data into a new segment from the cache.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCache* cache;
 * gpointer data;
 *
 * ...
 *
 * j_cache_put(cache, data, 1024);
 * \endcode
 *
 * \param cache  A cache.
 * \param data   Data.
 * \param length A length.
 *
 * \return A pointer to the used segment of the cache, NULL if not enough space is available.
 **/
gpointer
j_cache_put (JCache* cache, gconstpointer data, guint64 length)
{
	gpointer ret;

	g_return_val_if_fail(cache != NULL, NULL);
	g_return_val_if_fail(data != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	ret = j_cache_get(cache, length);

	if (ret != NULL)
	{
		memcpy(ret, data, length);
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

/**
 * Clears the cache.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCache* cache;
 *
 * ...
 *
 * j_cache_clear(cache);
 * \endcode
 *
 * \param cache A cache.
 **/
void
j_cache_clear (JCache* cache)
{
	g_return_if_fail(cache != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	cache->current = cache->data;

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * @}
 **/
