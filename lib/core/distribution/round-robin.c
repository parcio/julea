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

#include <julea-config.h>

#include <glib.h>

#include <bson.h>

#include <jconfiguration.h>
#include <jtrace-internal.h>

#include "distribution.h"

/**
 * \defgroup JDistribution Distribution
 *
 * Data structures and functions for managing distributions.
 *
 * @{
 **/

/**
 * A distribution.
 **/
struct JDistributionRoundRobin
{
	/**
	 * The server count.
	 **/
	guint server_count;

	/**
	 * The length.
	 **/
	guint64 length;

	/**
	 * The offset.
	 **/
	guint64 offset;

	/**
	 * The block size.
	 */
	guint64 block_size;

	guint start_index;
};

typedef struct JDistributionRoundRobin JDistributionRoundRobin;

/**
 * Distributes data in a round robin fashion.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param index        A server index.
 * \param new_length   A new length.
 * \param new_offset   A new offset.
 *
 * \return TRUE on success, FALSE if the distribution is finished.
 **/
static
gboolean
distribution_distribute (gpointer data, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id)
{
	JDistributionRoundRobin* distribution = data;

	gboolean ret = TRUE;
	guint64 block;
	guint64 displacement;
	guint64 round;

	j_trace_enter(G_STRFUNC, NULL);

	if (distribution->length == 0)
	{
		ret = FALSE;
		goto end;
	}

	block = distribution->offset / distribution->block_size;
	round = block / distribution->server_count;
	displacement = distribution->offset % distribution->block_size;

	*index = (distribution->start_index + block) % distribution->server_count;
	*new_length = MIN(distribution->length, distribution->block_size - displacement);
	*new_offset = (round * distribution->block_size) + displacement;
	*block_id = block;

	distribution->length -= *new_length;
	distribution->offset += *new_length;

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gpointer
distribution_new (guint server_count, guint64 stripe_size)
{
	JDistributionRoundRobin* distribution;

	j_trace_enter(G_STRFUNC, NULL);

	distribution = g_slice_new(JDistributionRoundRobin);
	distribution->server_count = server_count;
	distribution->length = 0;
	distribution->offset = 0;
	distribution->block_size = stripe_size;

	distribution->start_index = g_random_int_range(0, distribution->server_count);

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Decreases a distribution's reference count.
 * When the reference count reaches zero, frees the memory allocated for the distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 **/
static
void
distribution_free (gpointer data)
{
	JDistributionRoundRobin* distribution = data;

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_slice_free(JDistributionRoundRobin, distribution);

	j_trace_leave(G_STRFUNC);
}

/**
 * Sets the start index for the round robin distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param start_index  An index.
 */
static
void
distribution_set (gpointer data, gchar const* key, guint64 value)
{
	JDistributionRoundRobin* distribution = data;

	g_return_if_fail(distribution != NULL);

	if (g_strcmp0(key, "block-size") == 0)
	{
		distribution->block_size = value;
	}
	else if (g_strcmp0(key, "start-index") == 0)
	{
		g_return_if_fail(value < distribution->server_count);

		distribution->start_index = value;
	}
}

/**
 * Serializes distribution.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param distribution Credentials.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
static
void
distribution_serialize (gpointer data, bson_t* b)
{
	JDistributionRoundRobin* distribution = data;

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_append_int64(b, "block_size", -1, distribution->block_size);
	bson_append_int32(b, "start_index", -1, distribution->start_index);

	j_trace_leave(G_STRFUNC);
}

/**
 * Deserializes distribution.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param distribution distribution.
 * \param b           A BSON object.
 **/
static
void
distribution_deserialize (gpointer data, bson_t const* b)
{
	JDistributionRoundRobin* distribution = data;

	bson_iter_t iterator;

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "block_size") == 0)
		{
			distribution->block_size = bson_iter_int64(&iterator);
		}
		else if (g_strcmp0(key, "start_index") == 0)
		{
			distribution->start_index = bson_iter_int32(&iterator);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Initializes a distribution.
 *
 * \code
 * JDistribution* d;
 *
 * j_distribution_init(d, 0, 0);
 * \endcode
 *
 * \param length A length.
 * \param offset An offset.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
static
void
distribution_reset (gpointer data, guint64 length, guint64 offset)
{
	JDistributionRoundRobin* distribution = data;

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	distribution->length = length;
	distribution->offset = offset;

	j_trace_leave(G_STRFUNC);
}

void
j_distribution_round_robin_get_vtable (JDistributionVTable* vtable)
{
	vtable->distribution_new = distribution_new;
	vtable->distribution_free = distribution_free;
	vtable->distribution_set = distribution_set;
	vtable->distribution_set2 = NULL;
	vtable->distribution_serialize = distribution_serialize;
	vtable->distribution_deserialize = distribution_deserialize;
	vtable->distribution_reset = distribution_reset;
	vtable->distribution_distribute = distribution_distribute;
}

/**
 * @}
 **/
