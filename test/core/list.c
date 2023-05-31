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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include "test.h"

static void
test_list_fixture_setup(JList** list, gconstpointer data)
{
	(void)data;

	*list = j_list_new(g_free);
}

static void
test_list_fixture_teardown(JList** list, gconstpointer data)
{
	(void)data;

	j_list_unref(*list);
}

static void
test_list_new_free(void)
{
	guint const n = 100000;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JList) list = NULL;

		list = j_list_new(NULL);
		g_assert_true(list != NULL);
	}
	J_TEST_TRAP_END;
}

static void
test_list_length(JList** list, gconstpointer data)
{
	guint const n = 100000;
	guint l;

	(void)data;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		j_list_append(*list, g_strdup("append"));
	}

	l = j_list_length(*list);
	g_assert_cmpuint(l, ==, n);
	J_TEST_TRAP_END;
}

static void
test_list_append(JList** list, gconstpointer data)
{
	guint const n = 100000;

	(void)data;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		j_list_append(*list, g_strdup("append"));
	}
	J_TEST_TRAP_END;
}

static void
test_list_prepend(JList** list, gconstpointer data)
{
	guint const n = 100000;

	(void)data;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		j_list_prepend(*list, g_strdup("append"));
	}
	J_TEST_TRAP_END;
}

static void
test_list_get(JList** list, gconstpointer data)
{
	gchar const* s;

	(void)data;

	J_TEST_TRAP_START;
	j_list_append(*list, g_strdup("0"));
	j_list_append(*list, g_strdup("1"));
	j_list_append(*list, g_strdup("-2"));
	j_list_append(*list, g_strdup("-1"));

	s = j_list_get_first(*list);
	g_assert_cmpstr(s, ==, "0");
	s = j_list_get_last(*list);
	g_assert_cmpstr(s, ==, "-1");
	J_TEST_TRAP_END;
}

void
test_core_list(void)
{
	g_test_add_func("/core/list/new_free", test_list_new_free);
	g_test_add("/core/list/length", JList*, NULL, test_list_fixture_setup, test_list_length, test_list_fixture_teardown);
	g_test_add("/core/list/append", JList*, NULL, test_list_fixture_setup, test_list_append, test_list_fixture_teardown);
	g_test_add("/core/list/prepend", JList*, NULL, test_list_fixture_setup, test_list_prepend, test_list_fixture_teardown);
	g_test_add("/core/list/get", JList*, NULL, test_list_fixture_setup, test_list_get, test_list_fixture_teardown);
}
