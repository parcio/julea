/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
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

#ifndef JULEA_MEMORY_CHUNK_H
#define JULEA_MEMORY_CHUNK_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JMemoryChunk Memory Chunk
 *
 * A chunk of memory intended for short time cashing of larger data chunks.
 *
 * @{
 **/

struct JMemoryChunk;

typedef struct JMemoryChunk JMemoryChunk;

/**
 * Creates a new chunk.
 *
 * \code
 * JMemoryChunk* chunk;
 *
 * chunk = j_memory_chunk_new(1024);
 * \endcode
 *
 * \param size A size.
 *
 * \return A new chunk. Should be freed with j_memory_chunk_free().
 **/
JMemoryChunk* j_memory_chunk_new(guint64 size);

/**
 * Frees the memory allocated for the chunk.
 *
 * \code
 * JMemoryChunk* chunk;
 *
 * ...
 *
 * j_memory_chunk_free(chunk);
 * \endcode
 *
 * \param chunk A chunk.
 **/
void j_memory_chunk_free(JMemoryChunk* chunk);

/**
 * Gets a new segment from the chunk.
 *
 * \code
 * JMemoryChunk* chunk;
 *
 * ...
 *
 * j_memory_chunk_get(chunk, 1024);
 * \endcode
 *
 * \param chunk  A chunk.
 * \param length A length.
 *
 * \return A pointer to a segment of the chunk, NULL if not enough space is available.
 **/
gpointer j_memory_chunk_get(JMemoryChunk* chunk, guint64 length);

/**
 * Resets the given chunk. All data inside should be considered lost.
 *
 * \param chunk  A chunk.
 *
 **/
void j_memory_chunk_reset(JMemoryChunk* chunk);

/**
 * @}
 **/

G_END_DECLS

#endif
