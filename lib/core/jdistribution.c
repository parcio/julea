/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2023 Michael Kuhn
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

#include <jdistribution.h>
#include <jdistribution-internal.h>

#include <jbackend.h>
#include <jconfiguration.h>
#include <jtrace.h>

#include "distribution/distribution.h"

/**
 * \addtogroup JDistribution
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

static JDistribution*
j_distribution_new_common(JDistributionType type, JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	JDistribution* distribution;
	guint server_count;
	guint64 stripe_size;

	server_count = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT);
	stripe_size = j_configuration_get_stripe_size(configuration);

	distribution = g_new(JDistribution, 1);
	distribution->type = type;
	distribution->distribution = j_distribution_vtables[type].distribution_new(server_count, stripe_size);
	distribution->ref_count = 1;

	return distribution;
}

JDistribution*
j_distribution_new(JDistributionType type)
{
	J_TRACE_FUNCTION(NULL);

	JDistribution* distribution;

	distribution = j_distribution_new_common(type, j_configuration());

	return distribution;
}

JDistribution*
j_distribution_ref(JDistribution* distribution)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(distribution != NULL, NULL);

	g_atomic_int_inc(&(distribution->ref_count));

	return distribution;
}

void
j_distribution_unref(JDistribution* distribution)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(distribution != NULL);

	if (g_atomic_int_dec_and_test(&(distribution->ref_count)))
	{
		j_distribution_vtables[distribution->type].distribution_free(distribution->distribution);

		g_free(distribution);
	}
}

void
j_distribution_set_block_size(JDistribution* distribution, guint64 block_size)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(block_size > 0);

	if (j_distribution_vtables[distribution->type].distribution_set != NULL)
	{
		j_distribution_vtables[distribution->type].distribution_set(distribution->distribution, "block-size", block_size);
	}
}

void
j_distribution_set(JDistribution* distribution, gchar const* key, guint64 value)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(key != NULL);

	if (j_distribution_vtables[distribution->type].distribution_set != NULL)
	{
		j_distribution_vtables[distribution->type].distribution_set(distribution->distribution, key, value);
	}
}

void
j_distribution_set2(JDistribution* distribution, gchar const* key, guint64 value1, guint64 value2)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(key != NULL);

	if (j_distribution_vtables[distribution->type].distribution_set2 != NULL)
	{
		j_distribution_vtables[distribution->type].distribution_set2(distribution->distribution, key, value1, value2);
	}
}

void
j_distribution_reset(JDistribution* distribution, guint64 length, guint64 offset)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(distribution != NULL);

	j_distribution_vtables[distribution->type].distribution_reset(distribution->distribution, length, offset);
}

gboolean
j_distribution_distribute(JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(distribution != NULL, FALSE);
	g_return_val_if_fail(index != NULL, FALSE);
	g_return_val_if_fail(new_length != NULL, FALSE);
	g_return_val_if_fail(new_offset != NULL, FALSE);

	return j_distribution_vtables[distribution->type].distribution_distribute(distribution->distribution, index, new_length, new_offset, block_id);
}

bson_t*
j_distribution_serialize(JDistribution* distribution)
{
	J_TRACE_FUNCTION(NULL);

	bson_t* b;

	g_return_val_if_fail(distribution != NULL, NULL);

	b = bson_new();

	bson_append_int32(b, "type", -1, distribution->type);
	//bson_append_int64(b, "BlockSize", -1, distribution->block_size);

	j_distribution_vtables[distribution->type].distribution_serialize(distribution->distribution, b);

	//bson_finish(b);

	return b;
}

void
j_distribution_deserialize(JDistribution* distribution, bson_t const* b)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(b != NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "type") == 0)
		{
			distribution->type = bson_iter_int32(&iterator);
		}
	}

	j_distribution_vtables[distribution->type].distribution_deserialize(distribution->distribution, b);
}

/* Internal */

static void
j_distribution_check_vtables(void)
{
	J_TRACE_FUNCTION(NULL);

	for (guint i = 0; i < G_N_ELEMENTS(j_distribution_vtables); i++)
	{
		g_return_if_fail(j_distribution_vtables[i].distribution_new != NULL);
		g_return_if_fail(j_distribution_vtables[i].distribution_free != NULL);

		g_return_if_fail(j_distribution_vtables[i].distribution_serialize != NULL);
		g_return_if_fail(j_distribution_vtables[i].distribution_deserialize != NULL);

		g_return_if_fail(j_distribution_vtables[i].distribution_reset != NULL);
		g_return_if_fail(j_distribution_vtables[i].distribution_distribute != NULL);
	}
}

void
j_distribution_init(void)
{
	J_TRACE_FUNCTION(NULL);

	j_distribution_round_robin_get_vtable(&(j_distribution_vtables[J_DISTRIBUTION_ROUND_ROBIN]));
	j_distribution_single_server_get_vtable(&(j_distribution_vtables[J_DISTRIBUTION_SINGLE_SERVER]));
	j_distribution_weighted_get_vtable(&(j_distribution_vtables[J_DISTRIBUTION_WEIGHTED]));

	j_distribution_check_vtables();
}

JDistribution*
j_distribution_new_from_bson(bson_t const* b)
{
	J_TRACE_FUNCTION(NULL);

	JDistribution* distribution;

	distribution = j_distribution_new_common(J_DISTRIBUTION_ROUND_ROBIN, j_configuration());

	j_distribution_deserialize(distribution, b);

	return distribution;
}

JDistribution*
j_distribution_new_for_configuration(JDistributionType type, JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	JDistribution* distribution;

	distribution = j_distribution_new_common(type, configuration);

	return distribution;
}

/**
 * @}
 **/
