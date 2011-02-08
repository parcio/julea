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

#include <glib.h>

#include <julea.h>

#include <jcommon-internal.h>
#include <jdistribution.h>

static
void
test_distribution_fixture_setup (gpointer* fixture, gconstpointer data)
{
	GKeyFile* key_file;
	gchar const* servers[3] = { "localhost", "localhost", NULL };

	key_file = g_key_file_new();
	g_key_file_set_string_list(key_file, "servers", "data", servers, 2);
	g_key_file_set_string_list(key_file, "servers", "metadata", servers, 2);

	j_init_for_data(key_file);

	g_key_file_free(key_file);
}

static
void
test_distribution_fixture_teardown (gpointer* fixture, gconstpointer data)
{
	j_deinit();
}

static
void
test_distribution_round_robin (gpointer* fixture, gconstpointer data)
{
	JDistribution* distribution;
	gboolean ret;
	guint64 length;
	guint64 offset;
	guint index;

	distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, 4 * 512 * 1024, 42);

	ret = j_distribution_distribute(distribution, &index, &length, &offset);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 0);
	g_assert_cmpuint(length, ==, (512 * 1024) - 42);
	g_assert_cmpuint(offset, ==, 42);

	ret = j_distribution_distribute(distribution, &index, &length, &offset);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 1);
	g_assert_cmpuint(length, ==, 512 * 1024);
	g_assert_cmpuint(offset, ==, 0);

	ret = j_distribution_distribute(distribution, &index, &length, &offset);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 0);
	g_assert_cmpuint(length, ==, 512 * 1024);
	g_assert_cmpuint(offset, ==, 512 * 1024);

	ret = j_distribution_distribute(distribution, &index, &length, &offset);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 1);
	g_assert_cmpuint(length, ==, 512 * 1024);
	g_assert_cmpuint(offset, ==, 512 * 1024);

	ret = j_distribution_distribute(distribution, &index, &length, &offset);
	g_assert(ret);
	g_assert_cmpuint(index, ==, 0);
	g_assert_cmpuint(length, ==, 42);
	g_assert_cmpuint(offset, ==, 2 * 512 * 1024);

	ret = j_distribution_distribute(distribution, &index, &length, &offset);
	g_assert(!ret);

	j_distribution_free(distribution);
}

int main (int argc, char** argv)
{
	g_test_init(&argc, &argv, NULL);

	g_test_add("/julea/bson/round_robin", gpointer, NULL, test_distribution_fixture_setup, test_distribution_round_robin, test_distribution_fixture_teardown);

	return g_test_run();
}
