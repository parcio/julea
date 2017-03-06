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

#ifndef H_LOCK_INTERNAL
#define H_LOCK_INTERNAL

#include <glib.h>

#include <julea-internal.h>

struct JLock;

typedef struct JLock JLock;

#include <jitem.h>

J_GNUC_INTERNAL JLock* j_lock_new (JItem*);
J_GNUC_INTERNAL void j_lock_free (JLock*);

J_GNUC_INTERNAL gboolean j_lock_acquire (JLock*);
J_GNUC_INTERNAL gboolean j_lock_release (JLock*);

J_GNUC_INTERNAL void j_lock_add (JLock*, guint64);

#endif
