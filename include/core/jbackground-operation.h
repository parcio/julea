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

#ifndef JULEA_BACKGROUND_OPERATION_H
#define JULEA_BACKGROUND_OPERATION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JBackgroundOperation;

typedef struct JBackgroundOperation JBackgroundOperation;

typedef gpointer (*JBackgroundOperationFunc)(gpointer);

JBackgroundOperation* j_background_operation_new(JBackgroundOperationFunc, gpointer);
JBackgroundOperation* j_background_operation_ref(JBackgroundOperation*);
void j_background_operation_unref(JBackgroundOperation*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JBackgroundOperation, j_background_operation_unref)

gpointer j_background_operation_wait(JBackgroundOperation*);

G_END_DECLS

#endif
