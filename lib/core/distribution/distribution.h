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

#ifndef JULEA_DISTRIBUTION_DISTRIBUTION_H
#define JULEA_DISTRIBUTION_DISTRIBUTION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

#include <jconfiguration.h>

struct JDistributionVTable
{
	gpointer (*distribution_new)(guint, guint64);
	void (*distribution_free)(gpointer);

	void (*distribution_set)(gpointer, gchar const*, guint64);
	void (*distribution_set2)(gpointer, gchar const*, guint64, guint64);

	void (*distribution_serialize)(gpointer, bson_t*);
	void (*distribution_deserialize)(gpointer, bson_t const*);

	void (*distribution_reset)(gpointer, guint64, guint64);
	gboolean (*distribution_distribute)(gpointer, guint*, guint64*, guint64*, guint64*);
};

typedef struct JDistributionVTable JDistributionVTable;

void j_distribution_round_robin_get_vtable(JDistributionVTable*);
void j_distribution_single_server_get_vtable(JDistributionVTable*);
void j_distribution_weighted_get_vtable(JDistributionVTable*);

#endif
