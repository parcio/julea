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

#ifndef H_COMMON_INTERNAL
#define H_COMMON_INTERNAL

#include <glib.h>

#include <julea-internal.h>

#include <jcommon.h>

#include <jbackend.h>
#include <jbackend-internal.h>
#include <jconfiguration-internal.h>
#include <jlist.h>
#include <jtrace-internal.h>

J_GNUC_INTERNAL JConfiguration* j_configuration (void);

J_GNUC_INTERNAL JBackend* j_data_backend (void);
J_GNUC_INTERNAL JBackend* j_metadata_backend (void);

#endif
