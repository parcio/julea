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

#ifndef JULEA_CACHE_H
#define JULEA_CACHE_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JCache Cache
 *
 * @{
 **/

struct JCache;

typedef struct JCache JCache;

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
JCache* j_cache_new(guint64 size);

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
void j_cache_free(JCache* cache);

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
gpointer j_cache_get(JCache* cache, guint64 length);

/**
 * Frees cache memory
 *
 * \code
 * JCache* cache;
 *
 * cache = j_cache_new(1024);
 * \endcode
 *
 * \param cache A cache.
 * \param data A pointer to cached data.
 *
 **/
void j_cache_release(JCache* cache, gpointer data);

/**
 * @}
 **/

G_END_DECLS

#endif
