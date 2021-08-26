/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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

#ifndef JULEA_BACKGROUND_OPERATION_INTERNAL_H
#define JULEA_BACKGROUND_OPERATION_INTERNAL_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * Initializes the background operation framework.
 *
 * \code
 * j_background_operation_init();
 * \endcode
 **/
G_GNUC_INTERNAL void j_background_operation_init(guint count);

/**
 * Shuts down the background operation framework.
 *
 * \code
 * j_background_operation_fini();
 * \endcode
 **/
G_GNUC_INTERNAL void j_background_operation_fini(void);

/**
 * Gets the number of threads used.
 **/
G_GNUC_INTERNAL guint j_background_operation_get_num_threads(void);

G_END_DECLS

#endif
