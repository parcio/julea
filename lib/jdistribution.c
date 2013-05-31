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

#include <julea-internal.h>

#include <jcommon-internal.h>
#include <jconfiguration.h>
#include <jtrace-internal.h>

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
	 * The configuration.
	 **/
	JConfiguration* configuration;

	/**
	 * The type.
	 **/
	JDistributionType type;

	/**
	 * The length.
	 **/
	guint64 length;

	/**
	 * The offset.
	 **/
	guint64 offset;

	union
	{
		struct
		{
			guint64 block_size;
			guint start_index;
		}
		round_robin;

		struct
		{
			guint64 block_size;
			guint index;
		}
		single_server;
	}
	u;
};

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
j_distribution_distribute_round_robin (JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id)
{
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

	block = distribution->offset / distribution->u.round_robin.block_size;
	round = block / j_configuration_get_data_server_count(distribution->configuration);
	displacement = distribution->offset % distribution->u.round_robin.block_size;

	*index = block % j_configuration_get_data_server_count(distribution->configuration);
	*new_length = MIN(distribution->length, distribution->u.round_robin.block_size - displacement);
	*new_offset = (round * distribution->u.round_robin.block_size) + displacement;
	*block_id = block;

	distribution->length -= *new_length;
	distribution->offset += *new_length;

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Distributes data to a single server.
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
j_distribution_distribute_single_server (JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id)
{
	gboolean ret = TRUE;
	guint64 block;
	guint64 displacement;

	j_trace_enter(G_STRFUNC);

	if (distribution->length == 0)
	{
		ret = FALSE;
		goto end;
	}

	block = distribution->offset / distribution->u.single_server.block_size;
	displacement = distribution->offset % distribution->u.single_server.block_size;

	*index = distribution->u.single_server.index;
	*new_length = MIN(distribution->length, distribution->u.single_server.block_size - displacement);
	*new_offset = distribution->offset;
	*block_id = block;

	distribution->length -= *new_length;
	distribution->offset += *new_length;

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Creates a new distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * JDistribution* d;
 *
 * d = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, 0, 0);
 * \endcode
 *
 * \param type   A distribution type.
 *
 * \return A new distribution. Should be freed with j_distribution_free().
 **/
JDistribution*
j_distribution_new (JConfiguration* configuration, JDistributionType type)
{
	JDistribution* distribution;

	j_trace_enter(G_STRFUNC);

	distribution = g_slice_new(JDistribution);
	distribution->configuration = j_configuration_ref(configuration);
	distribution->type = type;
	distribution->length = 0;
	distribution->offset = 0;

	switch (distribution->type)
	{
		case J_DISTRIBUTION_ROUND_ROBIN:
			distribution->u.round_robin.block_size = J_STRIPE_SIZE;
			distribution->u.round_robin.start_index = g_random_int_range(0, j_configuration_get_data_server_count(distribution->configuration));
			break;
		case J_DISTRIBUTION_SINGLE_SERVER:
			distribution->u.single_server.block_size = J_STRIPE_SIZE;
			distribution->u.single_server.index = 0;
			break;
		case J_DISTRIBUTION_NONE:
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Frees the memory allocated by a distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 **/
void
j_distribution_free (JDistribution* distribution)
{
	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC);

	j_configuration_unref(distribution->configuration);

	g_slice_free(JDistribution, distribution);

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
 * \return A new distribution. Should be freed with j_distribution_free().
 **/
void
j_distribution_init (JDistribution* distribution, guint64 length, guint64 offset)
{
	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC);

	distribution->length = length;
	distribution->offset = offset;

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

	switch (distribution->type)
	{
		case J_DISTRIBUTION_ROUND_ROBIN:
			ret = j_distribution_distribute_round_robin(distribution, index, new_length, new_offset, block_id);
			break;
		case J_DISTRIBUTION_SINGLE_SERVER:
			ret = j_distribution_distribute_single_server(distribution, index, new_length, new_offset, block_id);
			break;
		case J_DISTRIBUTION_NONE:
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Sets the block size for the round robin distribution.
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
j_distribution_set_round_robin_block_size (JDistribution* distribution, guint64 block_size)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(block_size > 0);
	g_return_if_fail(distribution->type == J_DISTRIBUTION_ROUND_ROBIN);

	distribution->u.round_robin.block_size = block_size;
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
j_distribution_set_round_robin_start_index (JDistribution* distribution, guint start_index)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(start_index < j_configuration_get_data_server_count(distribution->configuration));
	g_return_if_fail(distribution->type == J_DISTRIBUTION_ROUND_ROBIN);

	distribution->u.round_robin.start_index = start_index;
}

/**
 * Sets the block size for the single server distribution.
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
j_distribution_set_single_server_block_size (JDistribution* distribution, guint64 block_size)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(block_size > 0);
	g_return_if_fail(distribution->type == J_DISTRIBUTION_SINGLE_SERVER);

	distribution->u.single_server.block_size = block_size;
}

/**
 * Sets the index for the single server distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param index        An index.
 */
void
j_distribution_set_single_server_index (JDistribution* distribution, guint index)
{
	g_return_if_fail(distribution != NULL);
	g_return_if_fail(index < j_configuration_get_data_server_count(distribution->configuration));
	g_return_if_fail(distribution->type == J_DISTRIBUTION_SINGLE_SERVER);

	distribution->u.single_server.index = index;
}

/* Internal */

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

	switch (distribution->type)
	{
		case J_DISTRIBUTION_ROUND_ROBIN:
			bson_append_string(b, "Type", "RoundRobin");
			bson_append_long(b, "BlockSize", distribution->u.round_robin.block_size);
			bson_append_int(b, "StartIndex", distribution->u.round_robin.start_index);
			break;
		case J_DISTRIBUTION_SINGLE_SERVER:
			bson_append_string(b, "Type", "SingleServer");
			bson_append_long(b, "BlockSize", distribution->u.single_server.block_size);
			bson_append_int(b, "Index", distribution->u.single_server.index);
			break;
		case J_DISTRIBUTION_NONE:
		default:
			g_warn_if_reached();
	}

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
			gchar const* type = bson_iterator_string(&iterator);

			if (g_strcmp0(type, "RoundRobin") == 0)
			{
				distribution->type = J_DISTRIBUTION_ROUND_ROBIN;
			}
			else if (g_strcmp0(type, "SingleServer") == 0)
			{
				distribution->type = J_DISTRIBUTION_SINGLE_SERVER;
			}
		}
		else if (g_strcmp0(key, "BlockSize") == 0)
		{
			switch (distribution->type)
			{
				case J_DISTRIBUTION_ROUND_ROBIN:
					distribution->u.round_robin.block_size = bson_iterator_int(&iterator);
					break;
				case J_DISTRIBUTION_SINGLE_SERVER:
					distribution->u.single_server.block_size = bson_iterator_int(&iterator);
					break;
				case J_DISTRIBUTION_NONE:
				default:
					g_warn_if_reached();
			}
		}
		else if (g_strcmp0(key, "StartIndex") == 0)
		{
			g_assert(distribution->type == J_DISTRIBUTION_ROUND_ROBIN);

			distribution->u.round_robin.start_index = bson_iterator_int(&iterator);
		}
		else if (g_strcmp0(key, "Index") == 0)
		{
			g_assert(distribution->type == J_DISTRIBUTION_SINGLE_SERVER);

			distribution->u.single_server.index = bson_iterator_int(&iterator);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
