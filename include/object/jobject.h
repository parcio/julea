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

JObject* j_object_ref (JObject*);
void j_object_unref (JObject*);

gchar const* j_object_get_name (JObject*);

JObject* j_object_create (gchar const*, gchar const*, guint32, JBatch*);
void j_object_delete (JObject*, JBatch*);
void j_object_get (JObject**, gchar const*, JBatch*);

void j_object_read (JObject*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_object_write (JObject*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_object_get_status (JObject*, JBatch*);

guint64 j_object_get_size (JObject*);
gint64 j_object_get_modification_time (JObject*);

guint64 j_object_get_optimal_access_size (JObject*);

#endif
