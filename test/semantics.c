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

#include "test.h"

static
void
test_semantics_fixture_setup (JSemantics** semantics, G_GNUC_UNUSED gconstpointer data)
{
	*semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
}

static
void
test_semantics_fixture_teardown (JSemantics** semantics, G_GNUC_UNUSED gconstpointer data)
{
	j_semantics_unref(*semantics);
}

static
void
test_semantics_new_ref_unref (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JSemantics) semantics = NULL;

		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
		g_assert_true(semantics != NULL);
		semantics = j_semantics_ref(semantics);
		g_assert_true(semantics != NULL);
		j_semantics_unref(semantics);
	}
}

static
void
test_semantics_set_get (JSemantics** semantics, G_GNUC_UNUSED gconstpointer data)
{
	gint s;

	j_semantics_set(*semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_OPERATION);
	s = j_semantics_get(*semantics, J_SEMANTICS_ATOMICITY);
	g_assert_cmpint(s, ==, J_SEMANTICS_ATOMICITY_OPERATION);

	j_semantics_set(*semantics, J_SEMANTICS_CONCURRENCY, J_SEMANTICS_CONCURRENCY_OVERLAPPING);
	s = j_semantics_get(*semantics, J_SEMANTICS_CONCURRENCY);
	g_assert_cmpint(s, ==, J_SEMANTICS_CONCURRENCY_OVERLAPPING);

	j_semantics_set(*semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_IMMEDIATE);
	s = j_semantics_get(*semantics, J_SEMANTICS_CONSISTENCY);
	g_assert_cmpint(s, ==, J_SEMANTICS_CONSISTENCY_IMMEDIATE);

	j_semantics_set(*semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_EVENTUAL);
	s = j_semantics_get(*semantics, J_SEMANTICS_PERSISTENCY);
	g_assert_cmpint(s, ==, J_SEMANTICS_PERSISTENCY_EVENTUAL);

	j_semantics_set(*semantics, J_SEMANTICS_SAFETY, J_SEMANTICS_SAFETY_STORAGE);
	s = j_semantics_get(*semantics, J_SEMANTICS_SAFETY);
	g_assert_cmpint(s, ==, J_SEMANTICS_SAFETY_STORAGE);

	j_semantics_set(*semantics, J_SEMANTICS_SECURITY, J_SEMANTICS_SECURITY_STRICT);
	s = j_semantics_get(*semantics, J_SEMANTICS_SECURITY);
	g_assert_cmpint(s, ==, J_SEMANTICS_SECURITY_STRICT);
}

void
test_semantics (void)
{
	g_test_add_func("/semantics/new_ref_unref", test_semantics_new_ref_unref);
	g_test_add("/semantics/set_get", JSemantics*, NULL, test_semantics_fixture_setup, test_semantics_set_get, test_semantics_fixture_teardown);
}
