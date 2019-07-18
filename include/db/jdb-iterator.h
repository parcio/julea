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

#ifndef JULEA_DB_ITERATOR_H
#define JULEA_DB_ITERATOR_H

#include <glib.h>
#include <bson.h>
#include <julea.h>
#include <db/jdb-schema.h>
#include <db/jdb-selector.h>

struct JDBIterator
{
	JDBSchema* schema;
	JDBSelector* selector;
	gpointer iterator;
	gint ref_count;
	gboolean valid;
	gboolean bson_valid;
	bson_t bson;
};
typedef struct JDBIterator JDBIterator;

JDBIterator* j_db_iterator_new(JDBSchema* schema, JDBSelector* selector, JBatch* batch, GError** error);
JDBIterator* j_db_iterator_ref(JDBIterator* iterator, GError** error);
void j_db_iterator_unref(JDBIterator* iterator);

gboolean j_db_iterator_next(JDBIterator* iterator, GError** error);
gboolean j_db_iterator_get_field(JDBIterator* iterator, gchar const* name, JDBType* type, gpointer* value, guint64* length, GError** error);

#endif
