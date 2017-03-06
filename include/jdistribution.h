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

#ifndef H_DISTRIBUTION
#define H_DISTRIBUTION

#include <glib.h>

enum JDistributionType
{
	J_DISTRIBUTION_ROUND_ROBIN,
	J_DISTRIBUTION_SINGLE_SERVER,
	J_DISTRIBUTION_WEIGHTED
};

typedef enum JDistributionType JDistributionType;

struct JDistribution;

typedef struct JDistribution JDistribution;

JDistribution* j_distribution_new (JDistributionType);
JDistribution* j_distribution_ref (JDistribution*);
void j_distribution_unref (JDistribution*);

void j_distribution_set_block_size (JDistribution*, guint64);
void j_distribution_set (JDistribution*, gchar const*, guint64);
void j_distribution_set2 (JDistribution*, gchar const*, guint64, guint64);

#endif
