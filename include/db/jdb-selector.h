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
	J_DB_SELECTOR_MODE_OR
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
	J_DB_SELECTOR_OPERATOR_NE
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
 * \param[out] error A GError pointer. Will point to a GError object in case of failure.
 * \pre schema != NULL
 * \pre schema is initialized
 *
 * \return the new selector or NULL on failure
 **/
JDBSelector* j_db_selector_new(JDBSchema* schema, JDBSelectorMode mode, GError** error);

/**
 * Increase the ref_count of the given selector.
 *
 * \param[in] selector the selector to increase the ref_count
 * \pre selector != NULL
 *
 * \return the selector or NULL on failure
 **/
JDBSelector* j_db_selector_ref(JDBSelector* selector);

/**
 * Decrease the ref_count of the given selector - and automatically call free if ref_count is 0.  This is a noop if selector == NULL.
 *
 * \param[in] selector the selector to decrease the ref_count
 **/
void j_db_selector_unref(JDBSelector* selector);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDBSelector, j_db_selector_unref)

/**
 * add a search field to the selector.
 *
 * \param[in] selector to add a search field to
 * \param[in] name the name of the field to compare
 * \param[in] operator_ the operator to use to compare the stored value with the given value
 * \param[in] value the value to compare with
 * \param[in] length the length of the value. Only used if value is binary
 * \param[out] error A GError pointer. Will point to a GError object in case of failure.
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
gboolean j_db_selector_add_field(JDBSelector* selector, gchar const* name, JDBSelectorOperator operator_, gconstpointer value, guint64 length, GError** error);

/**
 * \brief Add a second selector as sub selector.
 * 
 * Using this function it is possible to build nested expressions (e.g. "A and B and (C or D)" where "C or D" is given by a sub selector).
 * 
 * \param[in] selector primary selector
 * \param[in] sub_selector sub selector
 * \param[out] error A GError pointer. Will point to a GError object in case of failure.
 *
 * \pre selector != NULL
 * \pre sub_selector != NULL
 * \pre selector != sub_selector
 * \pre selector including all previously added sub_selectors must not contain more than 500 search fields after applying this operation
 *
 * \return TRUE on success, FALSE otherwise
 **/
gboolean j_db_selector_add_selector(JDBSelector* selector, JDBSelector* sub_selector, GError** error);

/**
 * Add a JOIN to the primary selector using the schema from the sub selector.
 * 
 * This function will join both schemas using the given fields and also copy previous add_join and add_selector data from the secondary selector to the primary selector.
 * 
 * \param[in] selector Primary selector.
 * \param[in] selector_field Name of a field in the primary schema of the selector.
 * \param[in] sub_selector The second selector to be used in the join.
 * \param[in] sub_selector_field Name of a field in the primary schema of the sub_selector.
 * 
 * \pre selector != NULL
 * \pre sub_selector != NULL
 * \pre selector != sub_selector
 * \pre selector including all previously added sub_selectors must not contain more than 500 search fields after applying this operation
 *
 * \return TRUE on success, FALSE otherwise
 **/

gboolean j_db_selector_add_join(JDBSelector* selector, gchar const* selector_field, JDBSelector* sub_selector, gchar const* sub_selector_field, GError** error);

G_END_DECLS

#endif
