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

#ifndef H_DISTRIBUTION_INTERNAL
#define H_DISTRIBUTION_INTERNAL

#include <glib.h>

#include <julea-internal.h>
#include <jconfiguration-internal.h>
#include <jdistribution.h>

#include <bson.h>

J_GNUC_INTERNAL void j_distribution_init (void);

J_GNUC_INTERNAL JDistribution* j_distribution_new_for_configuration (JDistributionType, JConfiguration*);
J_GNUC_INTERNAL JDistribution* j_distribution_new_from_bson (bson_t const*);

J_GNUC_INTERNAL bson_t* j_distribution_serialize (JDistribution*);
J_GNUC_INTERNAL void j_distribution_deserialize (JDistribution*, bson_t const*);

J_GNUC_INTERNAL void j_distribution_reset (JDistribution*, guint64, guint64);
J_GNUC_INTERNAL gboolean j_distribution_distribute (JDistribution*, guint*, guint64*, guint64*, guint64*);

#endif
