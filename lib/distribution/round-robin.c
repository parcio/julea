/*
 * Copyright (c) 2010-2013 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <bson.h>

#include <jconfiguration-internal.h>
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
	 * The configuration.
	 **/
	JConfiguration* configuration;

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
 * \author Michael Kuhn
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

	j_trace_enter(G_STRFUNC);

	if (distribution->length == 0)
	{
		ret = FALSE;
		goto end;
	}

	block = distribution->offset / distribution->block_size;
	round = block / j_configuration_get_data_server_count(distribution->configuration);
	displacement = distribution->offset % distribution->block_size;

	*index = (distribution->start_index + block) % j_configuration_get_data_server_count(distribution->configuration);
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
distribution_new (JConfiguration* configuration)
{
	JDistributionRoundRobin* distribution;

	j_trace_enter(G_STRFUNC);

	distribution = g_slice_new(JDistributionRoundRobin);
	distribution->configuration = j_configuration_ref(configuration);
	distribution->length = 0;
	distribution->offset = 0;
	distribution->block_size = J_STRIPE_SIZE;

	distribution->start_index = g_random_int_range(0, j_configuration_get_data_server_count(distribution->configuration));

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Decreases a distribution's reference count.
 * When the reference count reaches zero, frees the memory allocated for the distribution.
 *
 * \author Michael Kuhn
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

	j_trace_enter(G_STRFUNC);

	j_configuration_unref(distribution->configuration);

	j_trace_leave(G_STRFUNC);
}

/**
 * Sets the start index for the round robin distribution.
 *
 * \author Michael Kuhn
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

	if (g_strcmp0(key, "start-index") == 0)
	{
		g_return_if_fail(value < j_configuration_get_data_server_count(distribution->configuration));

		distribution->start_index = value;
	}
}

/**
 * Serializes distribution.
 *
 * \private
 *
 * \author Michael Kuhn
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
distribution_serialize (gpointer data, bson* b)
{
	JDistributionRoundRobin* distribution = data;

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC);

	bson_append_long(b, "BlockSize", distribution->block_size);
	bson_append_int(b, "StartIndex", distribution->start_index);

	j_trace_leave(G_STRFUNC);
}

/**
 * Deserializes distribution.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution distribution.
 * \param b           A BSON object.
 **/
static
void
distribution_deserialize (gpointer data, bson const* b)
{
	JDistributionRoundRobin* distribution = data;

	bson_iterator iterator;

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC);

	bson_iterator_init(&iterator, b);

	while (bson_iterator_next(&iterator))
	{
		gchar const* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "BlockSize") == 0)
		{
			distribution->block_size = bson_iterator_int(&iterator);
		}
		else if (g_strcmp0(key, "StartIndex") == 0)
		{
			distribution->start_index = bson_iterator_int(&iterator);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Initializes a distribution.
 *
 * \author Michael Kuhn
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

	j_trace_enter(G_STRFUNC);

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
