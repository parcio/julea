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

#ifndef BENCHMARK_STATISTICS_H
#define BENCHMARK_STATISTICS_H

/**
 * \file
 **/

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JBenchmarkStatistics Statistics 
 * module to track round-times and report detailed statistics about them
 * @{
 **/

void* j_benchmark_statistics_init(guint compression);
void j_benchmark_statistics_fini(void*);

/**
 * Adds a new round time for to statistics.
 * round_time[0] = current_time[0]
 * round_time[n] = current_time[n] - current_time[n-1]
 *
 * \attention for long runtime looses precision due to subtraction of large floating point numbers
 * \todo check if this is a problem
 *
 * \param[inout] this
 * \param[in] current_time elapsed time spending since benchmark start.
 */
void j_benchmark_statistics_add(void*, double current_time);

double j_benchmark_statistics_min(void*);
double j_benchmark_statistics_mean(void*);
double j_benchmark_statistics_max(void*);
guint j_benchmark_statistics_num_entries(void*);
double j_benchmark_statistics_derivation(void*);
double j_benchmark_statistics_quantiles(void*, double q);

/**
 * @}
 **/

G_END_DECLS

#endif
