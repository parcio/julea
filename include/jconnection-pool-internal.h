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

#ifndef H_CONNECTION_POOL_INTERNAL
#define H_CONNECTION_POOL_INTERNAL

#include <glib.h>
#include <gio/gio.h>

#include <julea-internal.h>

#include <jconfiguration-internal.h>

J_GNUC_INTERNAL void j_connection_pool_init (JConfiguration*);
J_GNUC_INTERNAL void j_connection_pool_fini (void);

J_GNUC_INTERNAL GSocketConnection* j_connection_pool_pop_data (guint);
J_GNUC_INTERNAL void j_connection_pool_push_data (guint, GSocketConnection*);

J_GNUC_INTERNAL GSocketConnection* j_connection_pool_pop_meta (guint);
J_GNUC_INTERNAL void j_connection_pool_push_meta (guint, GSocketConnection*);

#endif
