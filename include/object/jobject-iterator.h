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

#ifndef JULEA_OBJECT_OBJECT_ITERATOR_H
#define JULEA_OBJECT_OBJECT_ITERATOR_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JObjectIterator;

typedef struct JObjectIterator JObjectIterator;

JObjectIterator* j_object_iterator_new(gchar const*);
void j_object_iterator_free(JObjectIterator*);

gboolean j_object_iterator_next(JObjectIterator*);
gchar const* j_object_iterator_get(JObjectIterator*, guint64*);

G_END_DECLS

#endif
