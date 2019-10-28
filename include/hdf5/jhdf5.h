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

#ifndef JULEA_HDF5_HDF5_H
#define JULEA_HDF5_HDF5_H

#if !defined(JULEA_HDF5_H) && !defined(JULEA_HDF5_COMPILATION)
#error "Only <julea-hdf5.h> can be included directly."
#endif

#include <glib.h>

#include <hdf5.h>

#include <julea.h>

G_BEGIN_DECLS

hid_t j_hdf5_get_fapl (void);
void j_hdf5_set_semantics (JSemantics*);

G_END_DECLS

#endif
