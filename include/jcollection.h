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

#ifndef H_COLLECTION
#define H_COLLECTION

#include <glib.h>

struct JCollection;

typedef struct JCollection JCollection;

#include <jitem.h>
#include <jdistribution.h>
#include <jlist.h>
#include <jbatch.h>
#include <jsemantics.h>

JCollection* j_collection_ref (JCollection*);
void j_collection_unref (JCollection*);

gchar const* j_collection_get_name (JCollection*);

JCollection* j_collection_create (gchar const*, JBatch*);
void j_collection_get (JCollection**, gchar const*, JBatch*);
void j_collection_delete (JCollection*, JBatch*);

#endif
