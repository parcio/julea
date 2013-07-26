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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <julea-internal.h>

#include <jcommon.h>
#include <jconfiguration-internal.h>
#include <jdistribution-internal.h>

#include "test.h"

static
void
test_distribution_fixture_setup (JConfiguration** configuration, gconstpointer data)
{
	GKeyFile* key_file;
	gchar const* servers[] = { "localhost", "localhost", NULL };

	(void)data;

	key_file = g_key_file_new();
	g_key_file_set_string_list(key_file, "servers", "data", servers, 2);
	g_key_file_set_string_list(key_file, "servers", "metadata", servers, 2);
	g_key_file_set_string(key_file, "storage", "backend", "null");
	g_key_file_set_string(key_file, "storage", "path", "");

	*configuration = j_configuration_new_for_data(key_file);

	g_key_file_free(key_file);
}

static
void
test_distribution_fixture_teardown (JConfiguration** configuration, gconstpointer data)
{
	(void)data;

	j_configuration_unref(*configuration);
}

static
void
test_distribution_distribute (JDistributionType type, JConfiguration** configuration, gconstpointer data)
{
	JDistribution* distribution;
	gboolean ret;
	guint64 block_size;
	guint64 length;
	guint64 offset;
	guint64 block_id;
	guint index;

	(void)data;

	block_size = J_STRIPE_SIZE;

	distribution = j_distribution_new_for_configuration(type, *configuration);
	j_distribution_init(distribution, 4 * block_size, 42);

	j_distribution_set_block_size(distribution, block_size);

	switch (type)
	{
		case J_DISTRIBUTION_ROUND_ROBIN:
			j_distribution_set_round_robin_start_index(distribution, 1);
			break;
		case J_DISTRIBUTION_SINGLE_SERVER:
			j_distribution_set_single_server_index(distribution, 1);
			break;
		case J_DISTRIBUTION_WEIGHTED:
			j_distribution_set_weight(distribution, 0, 1);
			j_distribution_set_weight(distribution, 1, 2);
			break;
		default:
			g_warn_if_reached();
	}

	ret = j_distribution_distribute(distribution, &index, &length, &offset, &block_id);
	g_assert(ret);

	if (type == J_DISTRIBUTION_WEIGHTED)
	{
		g_assert_cmpuint(index, ==, 0);
	}
	else
	{
		g_assert_cmpuint(index, ==, 1);
	}

	g_assert_cmpuint(length, ==, block_size - 42);
	g_assert_cmpuint(offset, ==, 42);
	g_assert_cmpuint(block_id, ==, 0);

	ret = j_distribution_distribute(distribution, &index, &length, &offset, &block_id);
	g_assert(ret);
	g_assert_cmpuint(length, ==, block_size);
	g_assert_cmpuint(block_id, ==, 1);

	if (type == J_DISTRIBUTION_ROUND_ROBIN)
	{
		g_assert_cmpuint(index, ==, 0);
		g_assert_cmpuint(offset, ==, 0);
	}
	else if (type == J_DISTRIBUTION_SINGLE_SERVER)
	{
		g_assert_cmpuint(index, ==, 1);
		g_assert_cmpuint(offset, ==, block_size);
	}
	else if (type == J_DISTRIBUTION_WEIGHTED)
	{
		g_assert_cmpuint(index, ==, 1);
		g_assert_cmpuint(offset, ==, 0);
	}

	ret = j_distribution_distribute(distribution, &index, &length, &offset, &block_id);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 1);
	g_assert_cmpuint(length, ==, block_size);
	g_assert_cmpuint(block_id, ==, 2);

	if (type == J_DISTRIBUTION_ROUND_ROBIN)
	{
		g_assert_cmpuint(offset, ==, block_size);
	}
	else if (type == J_DISTRIBUTION_SINGLE_SERVER)
	{
		g_assert_cmpuint(offset, ==, 2 * block_size);
	}
	else if (type == J_DISTRIBUTION_WEIGHTED)
	{
		g_assert_cmpuint(offset, ==, block_size);
	}

	ret = j_distribution_distribute(distribution, &index, &length, &offset, &block_id);
	g_assert(ret);
	g_assert_cmpuint(length, ==, block_size);
	g_assert_cmpuint(block_id, ==, 3);

	if (type == J_DISTRIBUTION_ROUND_ROBIN)
	{
		g_assert_cmpuint(index, ==, 0);
		g_assert_cmpuint(offset, ==, block_size);
	}
	else if (type == J_DISTRIBUTION_SINGLE_SERVER)
	{
		g_assert_cmpuint(index, ==, 1);
		g_assert_cmpuint(offset, ==, 3 * block_size);
	}
	else if (type == J_DISTRIBUTION_WEIGHTED)
	{
		g_assert_cmpuint(index, ==, 0);
		g_assert_cmpuint(offset, ==, block_size);
	}

	ret = j_distribution_distribute(distribution, &index, &length, &offset, &block_id);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 1);
	g_assert_cmpuint(length, ==, 42);
	g_assert_cmpuint(block_id, ==, 4);

	if (type == J_DISTRIBUTION_ROUND_ROBIN)
	{
		g_assert_cmpuint(offset, ==, 2 * block_size);
	}
	else if (type == J_DISTRIBUTION_SINGLE_SERVER)
	{
		g_assert_cmpuint(offset, ==, 4 * block_size);
	}
	else if (type == J_DISTRIBUTION_WEIGHTED)
	{
		g_assert_cmpuint(offset, ==, 2 * block_size);
	}

	ret = j_distribution_distribute(distribution, &index, &length, &offset, &block_id);
	g_assert(!ret);

	j_distribution_unref(distribution);
}

static
void
test_distribution_round_robin (JConfiguration** configuration, gconstpointer data)
{
	test_distribution_distribute(J_DISTRIBUTION_ROUND_ROBIN, configuration, data);
}

static
void
test_distribution_single_server (JConfiguration** configuration, gconstpointer data)
{
	test_distribution_distribute(J_DISTRIBUTION_SINGLE_SERVER, configuration, data);
}

static
void
test_distribution_weighted (JConfiguration** configuration, gconstpointer data)
{
	test_distribution_distribute(J_DISTRIBUTION_WEIGHTED, configuration, data);
}

void
test_distribution (void)
{
	g_test_add("/distribution/round_robin", JConfiguration*, NULL, test_distribution_fixture_setup, test_distribution_round_robin, test_distribution_fixture_teardown);
	g_test_add("/distribution/single_server", JConfiguration*, NULL, test_distribution_fixture_setup, test_distribution_single_server, test_distribution_fixture_teardown);
	g_test_add("/distribution/weighted", JConfiguration*, NULL, test_distribution_fixture_setup, test_distribution_weighted, test_distribution_fixture_teardown);
}
