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

#ifndef JULEA_DISTRIBUTION_H
#define JULEA_DISTRIBUTION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

G_BEGIN_DECLS

enum JDistributionType
{
	J_DISTRIBUTION_ROUND_ROBIN,
	J_DISTRIBUTION_SINGLE_SERVER,
	J_DISTRIBUTION_WEIGHTED
};

typedef enum JDistributionType JDistributionType;

struct JDistribution;

typedef struct JDistribution JDistribution;

G_END_DECLS

#include <core/jconfiguration.h>

G_BEGIN_DECLS

JDistribution* j_distribution_new(JDistributionType);
JDistribution* j_distribution_new_for_configuration(JDistributionType, JConfiguration*);
JDistribution* j_distribution_new_from_bson(bson_t const*);

JDistribution* j_distribution_ref(JDistribution*);
void j_distribution_unref(JDistribution*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDistribution, j_distribution_unref)

bson_t* j_distribution_serialize(JDistribution*);

void j_distribution_set_block_size(JDistribution*, guint64);
void j_distribution_set(JDistribution*, gchar const*, guint64);
void j_distribution_set2(JDistribution*, gchar const*, guint64, guint64);

void j_distribution_reset(JDistribution*, guint64, guint64);
gboolean j_distribution_distribute(JDistribution*, guint*, guint64*, guint64*, guint64*);

G_END_DECLS

#endif
