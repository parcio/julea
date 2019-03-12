/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
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

#ifndef JULEA_KV_KV_ITERATOR_H
#define JULEA_KV_KV_ITERATOR_H

#if !defined(JULEA_KV_H) && !defined(JULEA_KV_COMPILATION)
#error "Only <julea-kv.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JKVIterator;

typedef struct JKVIterator JKVIterator;

G_END_DECLS

#include <kv/jkv.h>

G_BEGIN_DECLS

JKVIterator* j_kv_iterator_new (guint32, gchar const*, gchar const*);
void j_kv_iterator_free (JKVIterator*);

gboolean j_kv_iterator_next (JKVIterator*);
bson_t const* j_kv_iterator_get (JKVIterator*);

G_END_DECLS

#endif
