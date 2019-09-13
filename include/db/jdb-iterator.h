/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
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

#ifndef JULEA_DB_ITERATOR_H
#define JULEA_DB_ITERATOR_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JDBIterator;

typedef struct JDBIterator JDBIterator;

G_END_DECLS

#include <db/jdb-schema.h>
#include <db/jdb-selector.h>

G_BEGIN_DECLS

/**
 * Allocates a new iterator.
 *
 * \param[in] schema The schema defines the structure of the iterator
 * \param[in] selector The selector defines which entrys to select
 * \pre schema != NULL
 * \pre schema is initialized
 * \pre selector != NULL
 * \pre selector fetches at least 1 element in the backend
 *
 * \return the new iterator or NULL on failure
 **/

JDBIterator* j_db_iterator_new (JDBSchema* schema, JDBSelector* selector, GError** error);

/**
 * Increase the ref_count of the given iterator.
 *
 * \param[in] iterator the iterator to increase the ref_count
 * \pre iterator != NULL
 *
 * \return the iterator or NULL on failure
 **/

JDBIterator* j_db_iterator_ref (JDBIterator* iterator);

/**
 * Decrease the ref_count of the given iterator - and automatically call free if ref_count is 0. This is a noop if iterator == NULL.
 *
 * \param[in] iterator the iterator to decrease the ref_count
 **/

void j_db_iterator_unref (JDBIterator* iterator);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDBIterator, j_db_iterator_unref)

/**
 * The iterator moves to the next element.
 *
 * \param[inout] iterator to update
 * \pre iterator != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/

gboolean j_db_iterator_next (JDBIterator* iterator, GError** error);

/**
 * Get a single value from the current entry of the iterator.
 *
 * \param[in] iterator to query
 * \param[in] name the name of the value to retrieve
 * \param[out] type the type of the retrieved value
 * \param[out] value the retieved value
 * \param[out] length the length of the retrieved value
 * \pre iterator != NULL
 * \pre name != NULL
 * \pre type != NULL
 * \pre value != NULL
 * \pre *value should not be initialized
 * \pre length != NULL
 * \post *value points to a new allocated memory region. The caller must free this later using g_free.
 * \post *length contains the length of the allocated memory region
 *
 * \return TRUE on success, FALSE otherwise
 **/

gboolean j_db_iterator_get_field (JDBIterator* iterator, gchar const* name, JDBType* type, gpointer* value, guint64* length, GError** error);

G_END_DECLS

#endif
