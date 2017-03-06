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

#ifndef H_JULEA_INTERNAL
#define H_JULEA_INTERNAL

#ifdef J_ENABLE_INTERNAL
#define J_GNUC_INTERNAL
#else
#define J_GNUC_INTERNAL G_GNUC_INTERNAL
#endif

#define J_KIB(n) ((n) * 1024)
#define J_MIB(n) ((n) * J_KIB(1024))
#define J_GIB(n) ((n) * J_MIB(1024))

#define J_STRIPE_SIZE J_MIB(4)

#define J_CRITICAL(format, ...) g_critical("%s: " format, G_STRFUNC, __VA_ARGS__);

/* FIXME j_sync() for benchmarks */

#endif
