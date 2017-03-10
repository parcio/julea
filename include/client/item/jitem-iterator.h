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

#ifndef H_ITEM_ITERATOR
#define H_ITEM_ITERATOR

#include <glib.h>

struct JItemIterator;

typedef struct JItemIterator JItemIterator;

#include <client/item/jcollection.h>
#include <client/item/jitem.h>

JItemIterator* j_item_iterator_new (JCollection*);
void j_item_iterator_free (JItemIterator*);

gboolean j_item_iterator_next (JItemIterator*);
JItem* j_item_iterator_get (JItemIterator*);

#endif
