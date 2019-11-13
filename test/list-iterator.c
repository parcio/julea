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
test_list_iterator_fixture_setup (JListIterator** iterator, gconstpointer data)
{
	g_autoptr(JList) list = NULL;

	(void)data;

	list = j_list_new(g_free);

	j_list_append(list, g_strdup("0"));
	j_list_append(list, g_strdup("1"));
	j_list_append(list, g_strdup("2"));

	*iterator = j_list_iterator_new(list);
}

static
void
test_list_iterator_fixture_teardown (JListIterator** iterator, gconstpointer data)
{
	(void)data;

	j_list_iterator_free(*iterator);
}

static
void
test_list_iterator_new_free (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JList) list = NULL;
		g_autoptr(JListIterator) iterator = NULL;

		list = j_list_new(NULL);
		g_assert_true(list != NULL);

		iterator = j_list_iterator_new(list);
		g_assert_true(iterator != NULL);
	}
}

static
void
test_list_iterator_next_get (JListIterator** iterator, gconstpointer data)
{
	gchar const* s;
	gboolean next;

	(void)data;

	next = j_list_iterator_next(*iterator);
	g_assert_true(next);

	s = j_list_iterator_get(*iterator);
	g_assert_cmpstr(s, ==, "0");

	next = j_list_iterator_next(*iterator);
	g_assert_true(next);

	s = j_list_iterator_get(*iterator);
	g_assert_cmpstr(s, ==, "1");

	next = j_list_iterator_next(*iterator);
	g_assert_true(next);

	s = j_list_iterator_get(*iterator);
	g_assert_cmpstr(s, ==, "2");

	next = j_list_iterator_next(*iterator);
	g_assert_true(!next);
}

void
test_list_iterator (void)
{
	g_test_add_func("/list-iterator/new_free", test_list_iterator_new_free);
	g_test_add("/list-iterator/next_get", JListIterator*, NULL, test_list_iterator_fixture_setup, test_list_iterator_next_get, test_list_iterator_fixture_teardown);
}
