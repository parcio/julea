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

#ifndef H_OBJECT_OBJECT
#define H_OBJECT_OBJECT

#include <glib.h>

struct JObject;

typedef struct JObject JObject;

#include <jbatch.h>

JObject* j_object_new (guint32, gchar const*, gchar const*);
JObject* j_object_ref (JObject*);
void j_object_unref (JObject*);

gchar const* j_object_get_name (JObject*);

void j_object_create (JObject*, JBatch*);
void j_object_delete (JObject*, JBatch*);

void j_object_read (JObject*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_object_write (JObject*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_object_get_status (JObject*, gint64*, guint64*, JBatch*);

#endif
