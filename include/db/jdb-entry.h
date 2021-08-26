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

#ifndef JULEA_DB_ENTRY_H
#define JULEA_DB_ENTRY_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

struct JDBEntry;

typedef struct JDBEntry JDBEntry;

G_END_DECLS

#include <db/jdb-schema.h>
#include <db/jdb-selector.h>

G_BEGIN_DECLS

/**
 * Allocates a new entry.
 *
 * \param[in] schema The schema defines the structure of the entity
 * \param[out] error A GError pointer. Will point to a GError object in case of failure.
 * \pre schema != NULL
 * \pre schema is initialized
 *
 * \return the new entry or NULL on failure
 **/
JDBEntry* j_db_entry_new(JDBSchema* schema, GError** error);

/**
 * Increase the ref_count of the given entry.
 *
 * \param[in] entry the entry to increase the ref_count
 * \pre entry != NULL
 *
 * \return the entry or NULL on failure
 **/
JDBEntry* j_db_entry_ref(JDBEntry* entry);

/**
 * Decrease the ref_count of the given entry - and automatically call free if ref_count is 0.  This is a noop if entry == NULL.
 *
 * \param[in] entry the entry to decrease the ref_count
 **/
void j_db_entry_unref(JDBEntry* entry);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDBEntry, j_db_entry_unref)

/**
 * returns the id of the entry, after j_db_entry_insert has been called
 *
 * \param[in] entry   A JDBEntry.
 * \param[out] value  The id of \p entry. Should be freed using g_free().
 * \param[out] length The length of the id in byte.
 * \param[out] error  A GError pointer. Will point to a GError object in case of failure.
 *
 * \return TRUE on success, FALSE otherwise
 */
gboolean j_db_entry_get_id(JDBEntry* entry, gpointer* value, guint64* length, GError** error);

/**
 * Set a field in the given entry
 *
 * \param[in] entry the entry to set a value
 * \param[in] name the name to set a value
 * \param[in] value the value to set
 * \param[in] length the length of the value. Only used if the value-type defined in the Schema is binary.
 * \param[out] error A GError pointer. Will point to a GError object in case of failure.
 * \pre entry != NULL
 * \pre name != NULL
 * \pre value != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_entry_set_field(JDBEntry* entry, gchar const* name, gconstpointer value, guint64 length, GError** error);

/**
 * Save the entry in the backend.
 * All variables defined in the schema, which are not explicitily set, are initialized to NULL.
 *
 * The entry must not be modified until the batch is executed.
 *
 * \param[in] entry the entry to save
 * \param[in] batch the batch to append this operation to
 * \param[out] error  A GError pointer. Will point to a GError object in case of failure.
 * \pre entry != NULL
 * \pre entry has a least 1 value set to not NULL
 * \pre batch != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_entry_insert(JDBEntry* entry, JBatch* batch, GError** error);

/**
 * Replayes all entrys attributes with the given entrys attributes in the backend where the selector matches.
 *
 * The entry must not be modified until the batch is executed.
 * The selector must not be modified until the batch is executed.
 *
 * \param[in] entry the entry defining the final values of all matched entrys
 * \param[in] selector the selector defines which entrys should be modifies
 * \param[in] batch the batch to append this operation to
 * \param[out] error  A GError pointer. Will point to a GError object in case of failure.
 * \pre entry != NULL
 * \pre entry has a least 1 value set to not NULL
 * \pre selector != NULL
 * \pre selector matches at least 1 entry
 * \pre batch != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_entry_update(JDBEntry* entry, JDBSelector* selector, JBatch* batch, GError** error);

/**
 * Delete the entry from the backend.
 *
 * The entry must not be modified until the batch is executed.
 * The selector must not be modified until the batch is executed.
 *
 * \param[in] entry specifies the schema to use
 * \param[in] selector the selector defines what should be deleted
 * \param[in] batch the batch to append this operation to
 * \param[out] error  A GError pointer. Will point to a GError object in case of failure.
 * \pre entry != NULL
 * \pre batch != NULL
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_entry_delete(JDBEntry* entry, JDBSelector* selector, JBatch* batch, GError** error);

G_END_DECLS

#endif
