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

#include <jcache.h>

#include <jtrace.h>

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
	J_TRACE_FUNCTION(NULL);

	JCache* cache;

	g_return_val_if_fail(size > 0, NULL);

	cache = g_slice_new(JCache);
	cache->size = size;
	cache->buffers = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
	cache->used = 0;

	g_mutex_init(cache->mutex);

	return cache;
}

/**
 * Frees the memory allocated for the cache.
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
	J_TRACE_FUNCTION(NULL);

	GHashTableIter iter[1];
	gpointer key;
	gpointer value;

	g_return_if_fail(cache != NULL);

	g_hash_table_iter_init(iter, cache->buffers);

	while (g_hash_table_iter_next(iter, &key, &value))
	{
		g_free(key);
	}

	g_hash_table_unref(cache->buffers);

	g_mutex_clear(cache->mutex);

	g_slice_free(JCache, cache);
}

/**
 * Gets a new segment from the cache.
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
	J_TRACE_FUNCTION(NULL);

	gpointer ret = NULL;

	g_return_val_if_fail(cache != NULL, NULL);

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

	return ret;
}

void
j_cache_release (JCache* cache, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

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
