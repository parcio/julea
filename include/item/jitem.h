/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#ifndef JULEA_ITEM_ITEM_H
#define JULEA_ITEM_ITEM_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

struct JItem;

typedef struct JItem JItem;

G_END_DECLS

#include <item/jcollection.h>

G_BEGIN_DECLS

JItem* j_item_ref (JItem*);
void j_item_unref (JItem*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JItem, j_item_unref)

gchar const* j_item_get_name (JItem*);
JCredentials* j_item_get_credentials (JItem*);

JItem* j_item_create (JCollection*, gchar const*, JDistribution*, JBatch*);
void j_item_delete (JItem*, JBatch*);
void j_item_get (JCollection*, JItem**, gchar const*, JBatch*);

void j_item_read (JItem*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_item_write (JItem*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_item_get_status (JItem*, JBatch*);

guint64 j_item_get_size (JItem*);
gint64 j_item_get_modification_time (JItem*);

G_END_DECLS

#endif
