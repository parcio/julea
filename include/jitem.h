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

#ifndef H_ITEM
#define H_ITEM

#include <glib.h>

enum JItemStatusFlags
{
	J_ITEM_STATUS_NONE              = 0,
	J_ITEM_STATUS_MODIFICATION_TIME = 1 << 0,
	J_ITEM_STATUS_SIZE              = 1 << 1,
	J_ITEM_STATUS_ALL               = (J_ITEM_STATUS_MODIFICATION_TIME | J_ITEM_STATUS_SIZE)
};

typedef enum JItemStatusFlags JItemStatusFlags;

struct JItem;

typedef struct JItem JItem;

#include <jbatch.h>
#include <jcollection.h>
#include <jdistribution.h>
#include <jsemantics.h>

JItem* j_item_ref (JItem*);
void j_item_unref (JItem*);

gchar const* j_item_get_name (JItem*);

JItem* j_item_create (JCollection*, gchar const*, JDistribution*, JBatch*);
void j_item_delete (JCollection*, JItem*, JBatch*);
void j_item_get (JCollection*, JItem**, gchar const*, JBatch*);

void j_item_read (JItem*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_item_write (JItem*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_item_get_status (JItem*, JItemStatusFlags, JBatch*);

guint64 j_item_get_size (JItem*);
gint64 j_item_get_modification_time (JItem*);

guint64 j_item_get_optimal_access_size (JItem*);

#endif
