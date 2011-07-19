/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <glib.h>

#include "jdistribution.h"

#include "jcommon.h"

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
	 * The length.
	 **/
	guint64 length;

	/**
	 * The offset.
	 **/
	guint64 offset;
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
j_distribution_round_robin (JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset)
{
	guint64 const block_size = 512 * 1024;

	guint64 block;
	guint64 displacement;
	guint64 round;

	if (distribution->length == 0)
	{
		return FALSE;
	}

	block = distribution->offset / block_size;
	round = block / j_configuration()->data_len;
	displacement = distribution->offset % block_size;

	*index = block % j_configuration()->data_len;
	*new_length = MIN(distribution->length, block_size - displacement);
	*new_offset = (round * block_size) + displacement;

	distribution->length -= *new_length;
	distribution->offset += *new_length;

	return TRUE;
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
 * \param length A length.
 * \param offset An offset.
 *
 * \return A new distribution. Should be freed with j_distribution_free().
 **/
JDistribution*
j_distribution_new (JDistributionType type, guint64 length, guint64 offset)
{
	JDistribution* distribution;

	distribution = g_slice_new(JDistribution);
	distribution->type = type;
	distribution->length = length;
	distribution->offset = offset;

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

	g_slice_free(JDistribution, distribution);
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
j_distribution_distribute (JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset)
{
	g_return_val_if_fail(distribution != NULL, FALSE);
	g_return_val_if_fail(index != NULL, FALSE);
	g_return_val_if_fail(new_length != NULL, FALSE);
	g_return_val_if_fail(new_offset != NULL, FALSE);

	switch (distribution->type)
	{
		case J_DISTRIBUTION_ROUND_ROBIN:
			return j_distribution_round_robin(distribution, index, new_length, new_offset);
		default:
			g_return_val_if_reached(FALSE);
	}
}

/**
 * @}
 **/
