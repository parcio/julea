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

#ifndef H_BACKGROUND_OPERATION_INTERNAL
#define H_BACKGROUND_OPERATION_INTERNAL

#include <glib.h>

#include <julea-internal.h>

struct JBackgroundOperation;

typedef struct JBackgroundOperation JBackgroundOperation;

typedef gpointer (*JBackgroundOperationFunc) (gpointer);

J_GNUC_INTERNAL void j_background_operation_init (guint count);
J_GNUC_INTERNAL void j_background_operation_fini (void);

J_GNUC_INTERNAL guint j_background_operation_get_num_threads (void);

J_GNUC_INTERNAL JBackgroundOperation* j_background_operation_new (JBackgroundOperationFunc, gpointer);
J_GNUC_INTERNAL JBackgroundOperation* j_background_operation_ref (JBackgroundOperation*);
J_GNUC_INTERNAL void j_background_operation_unref (JBackgroundOperation*);

J_GNUC_INTERNAL gpointer j_background_operation_wait (JBackgroundOperation*);

#endif
