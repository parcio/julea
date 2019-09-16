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

#ifndef JULEA_KV_KV_H
#define JULEA_KV_KV_H

#if !defined(JULEA_KV_H) && !defined(JULEA_KV_COMPILATION)
#error "Only <julea-kv.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

struct JKV;

typedef struct JKV JKV;

/**
 * A callback for j_kv_get_callback.
 *
 * The callback will receive a pointer to the data (must be freed by the caller), the data's length and a pointer to optional user-provided data.
 */
typedef void (*JKVGetFunc) (gpointer, guint32, gpointer);

JKV* j_kv_new (gchar const*, gchar const*);
JKV* j_kv_new_for_index (guint32, gchar const*, gchar const*);
JKV* j_kv_ref (JKV*);
void j_kv_unref (JKV*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JKV, j_kv_unref)

void j_kv_put (JKV*, gpointer, guint32, GDestroyNotify, JBatch*);
void j_kv_delete (JKV*, JBatch*);

void j_kv_get (JKV*, gpointer*, guint32*, JBatch*);
void j_kv_get_callback (JKV*, JKVGetFunc, gpointer, JBatch*);

G_END_DECLS

#endif
