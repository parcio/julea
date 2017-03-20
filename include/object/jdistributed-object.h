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

#ifndef H_OBJECT_DISTRIBUTED_OBJECT
#define H_OBJECT_DISTRIBUTED_OBJECT

#include <glib.h>

struct JDistributedObject;

typedef struct JDistributedObject JDistributedObject;

#include <jbatch.h>

JDistributedObject* j_distributed_object_new (guint32, gchar const*, gchar const*);
JDistributedObject* j_distributed_object_ref (JDistributedObject*);
void j_distributed_object_unref (JDistributedObject*);

gchar const* j_distributed_object_get_name (JDistributedObject*);

void j_distributed_object_create (JDistributedObject*, JBatch*);
void j_distributed_object_delete (JDistributedObject*, JBatch*);

void j_distributed_object_read (JDistributedObject*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_distributed_object_write (JDistributedObject*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_distributed_object_get_status (JDistributedObject*, gint64*, guint64*, JBatch*);

#endif
