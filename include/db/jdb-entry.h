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

#ifndef JULEA_DB_ENTRY_H
#define JULEA_DB_ENTRY_H

#include <glib.h>
#include <bson.h>
#include <julea.h>
#include <db/jdb-type.h>
#include <db/jdb-selector.h>
#include <db/jdb-schema.h>

struct JDBEntry
{
	gint ref_count;
	JDBSchema* schema;
	JDBSelector* selector;
	bson_t bson;
};
typedef struct JDBEntry JDBEntry;

JDBEntry* j_db_entry_new(JDBSchema* schema, GError** error);

JDBEntry* j_db_entry_ref(JDBEntry* entry, GError** error);
void j_db_entry_unref(JDBEntry* entry);

gboolean j_db_entry_set_field(JDBEntry* entry, gchar const* name, gconstpointer value, guint64 length, GError** error);

gboolean j_db_entry_set_selector(JDBEntry* entry, JDBSelector* selector, GError** error);

gboolean j_db_entry_insert(JDBEntry* entry, JBatch* batch, GError** error);
gboolean j_db_entry_update(JDBEntry* entry, JBatch* batch, GError** error);
gboolean j_db_entry_delete(JDBEntry* entry, JBatch* batch, GError** error);

#endif
