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

#ifndef JULEA_DB_SELECTOR_H
#define JULEA_DB_SELECTOR_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

enum JDBSelectorMode
{
	J_DB_SELECTOR_MODE_AND,
	J_DB_SELECTOR_MODE_OR,
	_J_DB_SELECTOR_MODE_COUNT
};

typedef enum JDBSelectorMode JDBSelectorMode;

enum JDBSelectorOperator
{
	// <
	J_DB_SELECTOR_OPERATOR_LT,
	// <=
	J_DB_SELECTOR_OPERATOR_LE,
	// >
	J_DB_SELECTOR_OPERATOR_GT,
	// >=
	J_DB_SELECTOR_OPERATOR_GE,
	// =
	J_DB_SELECTOR_OPERATOR_EQ,
	// !=
	J_DB_SELECTOR_OPERATOR_NE,
	_J_DB_SELECTOR_OPERATOR_COUNT
};

typedef enum JDBSelectorOperator JDBSelectorOperator;

struct JDBSelector;

typedef struct JDBSelector JDBSelector;

G_END_DECLS

#include <db/jdb-schema.h>
#include <db/jdb-type.h>

G_BEGIN_DECLS

/**
 * Allocates a new selector.
 *
 * \param[in] schema    The schema defines the structure of the selector
 * \param[in] mode the more of the selector
 * \pre schema != NULL
 * \pre schema is initialized
 *
 * \return the new selector or NULL on failure
 **/

JDBSelector* j_db_selector_new (JDBSchema* schema, JDBSelectorMode mode, GError** error);

/**
 * Increase the ref_count of the given selector.
 *
 * \param[in] selector the selector to increase the ref_count
 * \pre selector != NULL
 *
 * \return the selector or NULL on failure
 **/

JDBSelector* j_db_selector_ref (JDBSelector* selector);

/**
 * Decrease the ref_count of the given selector - and automatically call free if ref_count is 0.  This is a noop if selector == NULL.
 *
 * \param[in] selector the selector to decrease the ref_count
 **/
void j_db_selector_unref (JDBSelector* selector);

G_DEFINE_AUTOPTR_CLEANUP_FUNC (JDBSelector, j_db_selector_unref)

/**
 * add a search field to the selector.
 *
 * \param[in] selector to add a search field to
 * \param[in] name the name of the field to compare
 * \param[in] operator the operator to use to compare the stored value with the given value
 * \param[in] value the value to compare with
 * \param[in] length the length of the value. Only used if value is binary
 *
 * \pre selector != NULL
 * \pre name != NULL
 * \pre name must exist in the schema
 * \pre operator must be applyable to the defined type of the variable
 * \pre selector including all previously added sub_selectors must not contain more than 500 search fields after applying this operation
 * \post the value may be freed or modified by the caller immediately after calling this function
 *
 * \return TRUE on success, FALSE otherwise
 **/

gboolean j_db_selector_add_field (JDBSelector* selector, gchar const* name, JDBSelectorOperator operator, gconstpointer value, guint64 length, GError** error);

/**
 * add a search field to the selector.
 *
 * \param[in] selector to add a sub_selector to
 * \param[in] sub_selector to add to the selector
 *
 * \pre selector != NULL
 * \pre sub_selector != NULL
 * \pre selector != sub_selector
 * \pre selector including all previously added sub_selectors must not contain more than 500 search fields after applying this operation
 *
 * \return TRUE on success, FALSE otherwise
 **/

gboolean j_db_selector_add_selector (JDBSelector* selector, JDBSelector* sub_selector, GError** error);

G_END_DECLS

#endif
