/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2022 Timm Leon Erxleben
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

#ifndef JULEA_BSON_H
#define JULEA_BSON_H

#include <glib.h>

#include <julea-db.h>

gboolean j_bson_init(bson_t* bson, GError** error);
void j_bson_destroy(bson_t* bson);

gboolean j_bson_has_field(bson_t* bson, gchar const* name, gboolean* has_field, GError** error);
gboolean j_bson_has_enough_keys(const bson_t* bson, guint32 min_keys, GError** error);
gboolean j_bson_count_keys(bson_t* bson, guint32* count, GError** error);
gboolean j_bson_array_generate_key(guint32 index, const char** key, char* buf, guint buf_length, GError** error);
gboolean j_bson_append_array(bson_t* bson, const char* key, bson_t* bson_child, GError** error);
gboolean j_bson_append_array_begin(bson_t* bson, const char* key, bson_t* bson_child, GError** error);
gboolean j_bson_append_array_end(bson_t* bson, bson_t* bson_child, GError** error);
gboolean j_bson_append_document(bson_t* bson, const char* key, bson_t* bson_child, GError** error);
gboolean j_bson_append_document_begin(bson_t* bson, const char* key, bson_t* bson_child, GError** error);
gboolean j_bson_append_document_end(bson_t* bson, bson_t* bson_child, GError** error);
gboolean j_bson_append_value(bson_t* bson, const char* name, JDBType type, JDBTypeValue* value, GError** error);

gboolean j_bson_iter_init(bson_iter_t* iter, const bson_t* bson, GError** error);
gboolean j_bson_iter_next(bson_iter_t* iter, gboolean* has_next, GError** error);
gboolean j_bson_iter_key_equals(bson_iter_t* iter, const char* key, gboolean* equals, GError** error);
const char* j_bson_iter_key(bson_iter_t* iter, GError** error);
gboolean j_bson_iter_value(bson_iter_t* iter, JDBType type, JDBTypeValue* value, GError** error);
gboolean j_bson_iter_find(bson_iter_t* iter, const char* key, GError** error);
gboolean j_bson_iter_not_find(bson_iter_t* iter, const char* key, GError** error);
gboolean j_bson_iter_recurse_array(bson_iter_t* iter, bson_iter_t* iter_child, GError** error);
gboolean j_bson_iter_recurse_document(bson_iter_t* iter, bson_iter_t* iter_child, GError** error);
gboolean j_bson_iter_copy_document(bson_iter_t* iter, bson_t* bson, GError** error);

#endif
