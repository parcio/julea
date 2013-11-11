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

#include <jdistribution.h>
#include <jdistribution-internal.h>

#include <julea-internal.h>

#include <jcommon-internal.h>
#include <jconfiguration-internal.h>
#include <jtrace-internal.h>

#include "distribution/distribution.h"

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
struct JDistribution
{
	/**
	 * The type.
	 **/
	JDistributionType type;

	/**
	 * The actual distribution.
	 */
	gpointer distribution;

	/**
	 * The reference count.
	 **/
	guint ref_count;
};

static JDistributionVTable j_distribution_vtables[3];

static
JDistribution*
j_distribution_new_common (JDistributionType type, JConfiguration* configuration)
{
	JDistribution* distribution;

	j_trace_enter(G_STRFUNC);

	distribution = g_slice_new(JDistribution);
	distribution->type = type;
	distribution->distribution = j_distribution_vtables[type].distribution_new(configuration);
	distribution->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Creates a new distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * JDistribution* d;
 *
 * d = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
 * \endcode
 *
 * \param type   A distribution type.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
JDistribution*
j_distribution_new (JDistributionType type)
{
	JDistribution* distribution;

	j_trace_enter(G_STRFUNC);

	distribution = j_distribution_new_common(type, j_configuration());

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Increases a distribution's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 **/
JDistribution*
j_distribution_ref (JDistribution* distribution)
{
	g_return_val_if_fail(distribution != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	g_atomic_int_inc(&(distribution->ref_count));

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
void
j_distribution_unref (JDistribution* distribution)
{
	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(distribution->ref_count)))
	{
		j_distribution_vtables[distribution->type].distribution_free(distribution->distribution);

		g_slice_free(JDistribution, distribution);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Sets the block size for the distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param block_size   A block size.
 */
void
j_distribution_set_block_size (JDistribution* distribution, guint64 block_size)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(block_size > 0);

	if (j_distribution_vtables[distribution->type].distribution_set != NULL)
	{
		j_distribution_vtables[distribution->type].distribution_set(distribution->distribution, "block-size", MIN(block_size, J_STRIPE_SIZE));
	}
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
void
j_distribution_set (JDistribution* distribution, gchar const* key, guint64 value)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(key != NULL);

	if (j_distribution_vtables[distribution->type].distribution_set != NULL)
	{
		j_distribution_vtables[distribution->type].distribution_set(distribution->distribution, key, value);
	}
}

void
j_distribution_set2 (JDistribution* distribution, gchar const* key, guint64 value1, guint64 value2)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(key != NULL);

	if (j_distribution_vtables[distribution->type].distribution_set2 != NULL)
	{
		j_distribution_vtables[distribution->type].distribution_set2(distribution->distribution, key, value1, value2);
	}
}

/* Internal */

void
j_distribution_init (void)
{
	j_distribution_round_robin_get_vtable(&(j_distribution_vtables[J_DISTRIBUTION_ROUND_ROBIN]));
	j_distribution_single_server_get_vtable(&(j_distribution_vtables[J_DISTRIBUTION_SINGLE_SERVER]));
	j_distribution_weighted_get_vtable(&(j_distribution_vtables[J_DISTRIBUTION_WEIGHTED]));
}

/**
 * Creates a new distribution from a BSON object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param b A BSON object.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
JDistribution*
j_distribution_new_from_bson (bson const* b)
{
	JDistribution* distribution;

	j_trace_enter(G_STRFUNC);

	distribution = j_distribution_new_common(J_DISTRIBUTION_ROUND_ROBIN, j_configuration());

	j_distribution_deserialize(distribution, b);

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Creates a new distribution for a given configuration.
 *
 * \author Michael Kuhn
 *
 * \code
 * JDistribution* d;
 *
 * d = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
 * \endcode
 *
 * \param type   A distribution type.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
JDistribution*
j_distribution_new_for_configuration (JDistributionType type, JConfiguration* configuration)
{
	JDistribution* distribution;

	j_trace_enter(G_STRFUNC);

	distribution = j_distribution_new_common(type, configuration);

	j_trace_leave(G_STRFUNC);

	return distribution;
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
bson*
j_distribution_serialize (JDistribution* distribution)
{
	bson* b;

	g_return_val_if_fail(distribution != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	b = g_slice_new(bson);
	bson_init(b);

	bson_append_int(b, "Type", distribution->type);
	//bson_append_long(b, "BlockSize", distribution->block_size);

	j_distribution_vtables[distribution->type].distribution_serialize(distribution->distribution, b);

	bson_finish(b);

	j_trace_leave(G_STRFUNC);

	return b;
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
void
j_distribution_deserialize (JDistribution* distribution, bson const* b)
{
	bson_iterator iterator;

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC);

	bson_iterator_init(&iterator, b);

	while (bson_iterator_next(&iterator))
	{
		gchar const* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "Type") == 0)
		{
			distribution->type = bson_iterator_int(&iterator);
		}
	}

	j_distribution_vtables[distribution->type].distribution_deserialize(distribution->distribution, b);

	j_trace_leave(G_STRFUNC);
}

/**
 * Resets a distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * JDistribution* d;
 *
 * j_distribution_reset(d, 0, 0);
 * \endcode
 *
 * \param length A length.
 * \param offset An offset.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
void
j_distribution_reset (JDistribution* distribution, guint64 length, guint64 offset)
{
	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC);

	j_distribution_vtables[distribution->type].distribution_reset(distribution->distribution, length, offset);

	j_trace_leave(G_STRFUNC);
}

/**
 * Calculates a new length and a new offset based on a distribution.
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
gboolean
j_distribution_distribute (JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id)
{
	gboolean ret = FALSE;

	g_return_val_if_fail(distribution != NULL, FALSE);
	g_return_val_if_fail(index != NULL, FALSE);
	g_return_val_if_fail(new_length != NULL, FALSE);
	g_return_val_if_fail(new_offset != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	ret = j_distribution_vtables[distribution->type].distribution_distribute(distribution->distribution, index, new_length, new_offset, block_id);

	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * @}
 **/
