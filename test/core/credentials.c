/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2020 Michael Kuhn
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

#include <sys/types.h>
#include <unistd.h>

#include <julea.h>

#include "test.h"

static void
test_credentials_new_ref_unref(void)
{
	guint const n = 1000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JCredentials) credentials = NULL;

		credentials = j_credentials_new();
		g_assert_nonnull(credentials);
		credentials = j_credentials_ref(credentials);
		g_assert_nonnull(credentials);
		j_credentials_unref(credentials);
	}
}

static void
test_credentials_get(void)
{
	g_autoptr(JCredentials) credentials = NULL;

	guint32 user;
	guint32 group;

	credentials = j_credentials_new();
	user = j_credentials_get_user(credentials);
	group = j_credentials_get_group(credentials);

	g_assert_cmpuint(user, ==, getuid());
	g_assert_cmpuint(group, ==, getgid());
}

static void
test_credentials_serialize(void)
{
	g_autoptr(JCredentials) credentials = NULL;
	g_autoptr(JCredentials) credentials2 = NULL;

	bson_t* bson = NULL;

	credentials = j_credentials_new();
	credentials2 = j_credentials_new();

	bson = j_credentials_serialize(credentials);
	g_assert_nonnull(bson);

	j_credentials_deserialize(credentials2, bson);

	bson_destroy(bson);
}

void
test_core_credentials(void)
{
	g_test_add_func("/core/credentials/new_ref_unref", test_credentials_new_ref_unref);
	g_test_add_func("/core/credentials/get", test_credentials_get);
	g_test_add_func("/core/credentials/serialize", test_credentials_serialize);
}
