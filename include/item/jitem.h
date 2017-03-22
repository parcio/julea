/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#ifndef H_ITEM_ITEM
#define H_ITEM_ITEM

#include <glib.h>

struct JItem;

typedef struct JItem JItem;

#include <item/jcollection.h>

#include <jbatch.h>
#include <jdistribution.h>
#include <jsemantics.h>

JItem* j_item_ref (JItem*);
void j_item_unref (JItem*);

gchar const* j_item_get_name (JItem*);

JItem* j_item_create (JCollection*, gchar const*, JDistribution*, JBatch*);
void j_item_delete (JItem*, JBatch*);
void j_item_get (JCollection*, JItem**, gchar const*, JBatch*);

void j_item_read (JItem*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_item_write (JItem*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_item_get_status (JItem*, JBatch*);

guint64 j_item_get_size (JItem*);
gint64 j_item_get_modification_time (JItem*);

guint64 j_item_get_optimal_access_size (JItem*);

#endif
