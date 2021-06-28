/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021 Michael Kuhn
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

#ifndef JULEA_DIR_ITERATOR_H
#define JULEA_DIR_ITERATOR_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JDirIterator Directory Iterator
 * @{
 **/

struct JDirIterator;

typedef struct JDirIterator JDirIterator;

G_END_DECLS

#include <core/jlist.h>

G_BEGIN_DECLS

/**
 * Creates a new directory iterator.
 *
 * \code
 * \endcode
 *
 * \param path A directory path.
 *
 * \return A new directory iterator.
 **/
JDirIterator* j_dir_iterator_new(gchar const* path);

/**
 * Frees the memory allocated by a directory iterator.
 *
 * \code
 * \endcode
 *
 * \param iterator A directory iterator.
 **/
void j_dir_iterator_free(JDirIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDirIterator, j_dir_iterator_free)

/**
 * Checks whether another file is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A directory iterator.
 *
 * \return TRUE on success, FALSE if the end of the iterator is reached.
 **/
gboolean j_dir_iterator_next(JDirIterator* iterator);

/**
 * Returns the current file.
 *
 * \code
 * \endcode
 *
 * \param iterator A directory iterator.
 *
 * \return A list element.
 **/
gchar const* j_dir_iterator_get(JDirIterator* iterator);

/**
 * @}
 **/

G_END_DECLS

#endif
