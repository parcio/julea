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

#ifndef JULEA_DB_ERROR_H
#define JULEA_DB_ERROR_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

#define J_DB_ERROR j_db_error_quark()

enum JDBError
{
	J_DB_ERROR_DUPLICATE_INDEX,
	J_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS,
	J_DB_ERROR_MODE_INVALID,
	J_DB_ERROR_OPERATOR_INVALID,
	J_DB_ERROR_SCHEMA_INITIALIZED,
	J_DB_ERROR_SCHEMA_NOT_INITIALIZED,
	J_DB_ERROR_SCHEMA_SERVER,
	J_DB_ERROR_SELECTOR_EMPTY,
	J_DB_ERROR_SELECTOR_MUST_NOT_EQUAL,
	J_DB_ERROR_SELECTOR_TOO_COMPLEX,
	J_DB_ERROR_TYPE_INVALID,
	J_DB_ERROR_VARIABLE_ALREADY_SET,
	J_DB_ERROR_VARIABLE_NOT_FOUND
};

typedef enum JDBError JDBError;

GQuark j_db_error_quark (void);

G_END_DECLS

#endif
