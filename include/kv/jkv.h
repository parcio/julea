/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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

#ifndef H_KV_KV
#define H_KV_KV

#include <glib.h>

struct JKV;

typedef struct JKV JKV;

#include <jbatch.h>

JKV* j_kv_new (guint32, gchar const*, gchar const*);
JKV* j_kv_ref (JKV*);
void j_kv_unref (JKV*);

gchar const* j_kv_get_name (JKV*);

void j_kv_create (JKV*, JBatch*);
void j_kv_delete (JKV*, JBatch*);

void j_kv_read (JKV*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_kv_write (JKV*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_kv_get_status (JKV*, gint64*, guint64*, JBatch*);

#endif
