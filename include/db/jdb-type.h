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

#ifndef JULEA_DB_TYPE_H
#define JULEA_DB_TYPE_H

#if !defined(JULEA_DB_H) && !defined(JULEA_DB_COMPILATION)
#error "Only <julea-db.h> can be included directly."
#endif

enum JDBType
{
	J_DB_TYPE_SINT32 = 0,
	J_DB_TYPE_UINT32,
	J_DB_TYPE_FLOAT32,
	J_DB_TYPE_SINT64,
	J_DB_TYPE_UINT64,
	J_DB_TYPE_FLOAT64,
	J_DB_TYPE_STRING,
	J_DB_TYPE_BLOB,
	_J_DB_TYPE_COUNT
};

typedef enum JDBType JDBType;

#endif
