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

#ifndef JULEA_DB_SELECTOR_H
#define JULEA_DB_SELECTOR_H

#include <glib.h>
#include <bson.h>
#include <julea.h>
#include <db/jdb-type.h>
#include <db/jdb-schema.h>

struct JDBSelector
{
	JDBSchema* schema;
	gint ref_count;
	bson_t bson;
};
typedef struct JDBSelector JDBSelector;

JDBSelector* j_db_selector_new(JDBSchema* schema, GError** error);

JDBSelector* j_db_selector_ref(JDBSelector* selector, GError** error);
void j_db_selector_unref(JDBSelector* selector);

//TODO the functions here

#endif
