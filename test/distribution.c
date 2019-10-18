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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include <jconfiguration.h>
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
	g_key_file_set_string_list(key_file, "servers", "object", servers, 2);
	g_key_file_set_string_list(key_file, "servers", "kv", servers, 2);
	g_key_file_set_string_list(key_file, "servers", "db", servers, 2);
	g_key_file_set_string(key_file, "object", "backend", "null");
	g_key_file_set_string(key_file, "object", "component", "server");
	g_key_file_set_string(key_file, "object", "path", "");
	g_key_file_set_string(key_file, "kv", "backend", "null");
	g_key_file_set_string(key_file, "kv", "component", "server");
	g_key_file_set_string(key_file, "kv", "path", "");
	g_key_file_set_string(key_file, "db", "backend", "null");
	g_key_file_set_string(key_file, "db", "component", "server");
	g_key_file_set_string(key_file, "db", "path", "");

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
	g_autoptr(JDistribution) distribution = NULL;
	gboolean ret;
	guint64 block_size;
	guint64 length;
	guint64 offset;
	guint64 block_id;
	guint index;

	(void)data;

	block_size = j_configuration_get_stripe_size(*configuration) - 1;

	distribution = j_distribution_new_for_configuration(type, *configuration);
	j_distribution_reset(distribution, 4 * block_size, 42);

	j_distribution_set_block_size(distribution, block_size);

	switch (type)
	{
		case J_DISTRIBUTION_ROUND_ROBIN:
			j_distribution_set(distribution, "start-index", 1);
			break;
		case J_DISTRIBUTION_SINGLE_SERVER:
			j_distribution_set(distribution, "index", 1);
			break;
		case J_DISTRIBUTION_WEIGHTED:
			j_distribution_set2(distribution, "weight", 0, 1);
			j_distribution_set2(distribution, "weight", 1, 2);
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
