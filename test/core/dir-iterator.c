/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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
#include <glib/gstdio.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <julea.h>

#include "test.h"

static gchar const* test_dir_dirs[] = {
	"sub",
	"sub/sub"
};

static gchar const* test_dir_files[] = {
	"foo",
	"sub/bar",
	"sub/sub/baz",
	"42"
};

static void
test_dir_iterator_fixture_setup(gchar** path, gconstpointer data)
{
	(void)data;

	*path = g_dir_make_tmp(NULL, NULL);

	for (guint i = 0; i < G_N_ELEMENTS(test_dir_dirs); i++)
	{
		g_autofree gchar* dir;

		dir = g_build_filename(*path, test_dir_dirs[i], NULL);
		g_mkdir_with_parents(dir, 0700);
	}

	for (guint i = 0; i < G_N_ELEMENTS(test_dir_files); i++)
	{
		g_autofree gchar* file;
		int fd;

		file = g_build_filename(*path, test_dir_files[i], NULL);
		fd = g_open(file, O_CREAT | O_RDWR, 0600);
		g_close(fd, NULL);
	}
}

static void
test_dir_iterator_fixture_teardown(gchar** path, gconstpointer data)
{
	(void)data;

	for (gint i = G_N_ELEMENTS(test_dir_files) - 1; i >= 0; i--)
	{
		g_autofree gchar* file;

		file = g_build_filename(*path, test_dir_files[i], NULL);
		g_remove(file);
	}

	for (gint i = G_N_ELEMENTS(test_dir_dirs) - 1; i >= 0; i--)
	{
		g_autofree gchar* dir;

		dir = g_build_filename(*path, test_dir_dirs[i], NULL);
		g_rmdir(dir);
	}

	g_rmdir(*path);
	g_free(*path);
}

static void
test_dir_iterator_new_free(void)
{
	guint const n = 1000;

	J_TEST_TRAP_START;
	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JDirIterator) iterator = NULL;

		iterator = j_dir_iterator_new("/tmp");
		g_assert_true(iterator != NULL);
	}
	J_TEST_TRAP_END;
}

static void
test_dir_iterator_next_get(gchar** path, gconstpointer data)
{
	g_autoptr(JDirIterator) iterator = NULL;
	g_autoptr(GHashTable) hash_table = NULL;
	gboolean ret;

	(void)data;

	J_TEST_TRAP_START;
	hash_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

	for (guint i = 0; i < G_N_ELEMENTS(test_dir_files); i++)
	{
		ret = g_hash_table_insert(hash_table, g_strdup(test_dir_files[i]), NULL);
		g_assert_true(ret);
	}

	g_assert_cmpuint(g_hash_table_size(hash_table), ==, G_N_ELEMENTS(test_dir_files));

	iterator = j_dir_iterator_new(*path);
	g_assert_true(iterator != NULL);

	while (j_dir_iterator_next(iterator))
	{
		gchar const* name;

		name = j_dir_iterator_get(iterator);
		g_assert_true(name != NULL);

		ret = g_hash_table_remove(hash_table, name);
		g_assert_true(ret);
	}

	g_assert_cmpuint(g_hash_table_size(hash_table), ==, 0);
	J_TEST_TRAP_END;
}

void
test_core_dir_iterator(void)
{
	g_test_add_func("/core/dir-iterator/new_free", test_dir_iterator_new_free);
	g_test_add("/core/dir-iterator/next_get", gchar*, NULL, test_dir_iterator_fixture_setup, test_dir_iterator_next_get, test_dir_iterator_fixture_teardown);
}
