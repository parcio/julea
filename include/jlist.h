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

#ifndef H_LIST
#define H_LIST

#include <glib.h>

struct JList;

typedef struct JList JList;

typedef void (*JListFreeFunc) (gpointer);

JList* j_list_new (JListFreeFunc);
JList* j_list_ref (JList*);
void j_list_unref (JList*);

guint j_list_length (JList*);

void j_list_append (JList*, gpointer);
void j_list_prepend (JList*, gpointer);

gpointer j_list_get_first (JList*);
gpointer j_list_get_last (JList*);

void j_list_delete_all (JList*);

#endif
