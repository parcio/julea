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

#ifndef JULEA_OBJECT_DISTRIBUTED_OBJECT_H
#define JULEA_OBJECT_DISTRIBUTED_OBJECT_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

struct JDistributedObject;

typedef struct JDistributedObject JDistributedObject;

JDistributedObject* j_distributed_object_new(gchar const*, gchar const*, JDistribution*);
JDistributedObject* j_distributed_object_ref(JDistributedObject*);
void j_distributed_object_unref(JDistributedObject*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDistributedObject, j_distributed_object_unref)

void j_distributed_object_create(JDistributedObject*, JBatch*);
void j_distributed_object_delete(JDistributedObject*, JBatch*);

void j_distributed_object_read(JDistributedObject*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_distributed_object_write(JDistributedObject*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_distributed_object_status(JDistributedObject*, gint64*, guint64*, JBatch*);

G_END_DECLS

#endif
