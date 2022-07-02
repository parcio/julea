/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021 Julian Benda
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

#ifndef TDIGEST_H
#define TDIGEST_H

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JTDigest TDigest
 *
 * T-Digest implementation based on https://github.com/tdunning/t-digest
 * approximates quantiles with buckest to handle abetrarie sizes.
 * Buckets are filled uequaly, therefore quantiles at the extreme
 * are more precise then in the center.
 *
 * @{
 **/

struct TDigest;
typedef struct TDigest TDigest;

/**
 * Instantiates a new TDigest to store double values.
 * Space complexity is O(compression)
 *
 * \param[in] compression number of cells to use for approximation, 100 is a good start value.
 */
TDigest* julea_t_digest_init(int compression);

void julea_t_digest_fini(TDigest*);

/** adds a value to the TDigest **/
void julea_t_digest_add(TDigest*, double value);

/**
 * query the q quantil(approximation), high and low quantils have a much higher precision
 * \retval NAN on error, eg no values provided so far
 */
double julea_t_digest_quantiles(TDigest*, double q);

/** query the max value (precise) */
double julea_t_digest_min(TDigest*);

/** query the min value (precise) */
double julea_t_digest_max(TDigest*);

/**
 * @}
 **/

G_END_DECLS

#endif
