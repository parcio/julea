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

#ifndef JULEA_HELPER_H
#define JULEA_HELPER_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gio/gio.h>

#include <core/jbackground-operation.h>

G_BEGIN_DECLS

guint64 j_helper_atomic_add (guint64 volatile*, guint64);
gboolean j_helper_execute_parallel (JBackgroundOperationFunc, gpointer*, guint);
guint32 j_helper_hash (gchar const*);
// FIXME get rid of GSocketConnection
void j_helper_set_nodelay (GSocketConnection*, gboolean);
gchar* j_helper_str_replace (gchar const*, gchar const*, gchar const*);

G_END_DECLS

#endif
