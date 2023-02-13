/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
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

#ifndef JULEA_OPERATION_CACHE_H
#define JULEA_OPERATION_CACHE_H

#include <glib.h>

#include <core/jbatch.h>

G_BEGIN_DECLS

/**
 * Flush the current cache of in-flight eventually consistency batches.
 *
 */
gboolean j_operation_cache_flush(void);

G_END_DECLS

#endif
