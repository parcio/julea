/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021-2024 Michael Kuhn
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

#include <jdir-iterator.h>

#include <jtrace.h>

/**
 * \addtogroup JDirIterator Directory Iterator
 *
 * @{
 **/

/**
 *
 **/
struct JDirIterator
{
	/**
	 * The directories.
	 **/
	GArray* dirs;

	/**
	 * The paths below the root directory.
	 **/
	GArray* paths;

	/**
	 * The current file.
	 **/
	gchar* current;

	/**
	 * The root directory.
	 **/
	gchar* root;

	/**
	 * The current depth.
	 **/
	guint depth;
};

JDirIterator*
j_dir_iterator_new(gchar const* path)
{
	J_TRACE_FUNCTION(NULL);

	JDirIterator* iterator;

	GDir* dir;
	gchar* path_tmp;

	g_return_val_if_fail(path != NULL, NULL);

	dir = g_dir_open(path, 0, NULL);

	if (dir == NULL)
	{
		return NULL;
	}

	iterator = g_new(JDirIterator, 1);
	iterator->dirs = g_array_new(FALSE, FALSE, sizeof(GDir*));
	iterator->paths = g_array_new(FALSE, FALSE, sizeof(gchar*));
	iterator->current = NULL;
	iterator->root = g_strdup(path);
	iterator->depth = 0;

	path_tmp = g_strdup("");

	g_array_append_val(iterator->dirs, dir);
	g_array_append_val(iterator->paths, path_tmp);

	return iterator;
}

void
j_dir_iterator_free(JDirIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(iterator != NULL);

	for (guint i = 0; i < iterator->paths->len; i++)
	{
		gchar* path;

		path = g_array_index(iterator->paths, gchar*, i);
		g_free(path);
	}

	for (guint i = 0; i < iterator->dirs->len; i++)
	{
		GDir* dir;

		dir = g_array_index(iterator->dirs, GDir*, i);
		g_dir_close(dir);
	}

	g_array_unref(iterator->paths);
	g_array_unref(iterator->dirs);

	g_free(iterator->root);

	g_free(iterator);
}

gboolean
j_dir_iterator_next(JDirIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, FALSE);

	while (TRUE)
	{
		GDir* dir;
		gchar* path;

		gchar const* name;

		dir = g_array_index(iterator->dirs, GDir*, iterator->depth);
		path = g_array_index(iterator->paths, gchar*, iterator->depth);

		if ((name = g_dir_read_name(dir)) != NULL)
		{
			g_free(iterator->current);
			iterator->current = g_build_filename(path, name, NULL);
		}
		else
		{
			g_free(iterator->current);
			iterator->current = NULL;
		}

		if (iterator->current == NULL)
		{
			if (iterator->depth > 0)
			{
				g_dir_close(dir);
				g_array_remove_index(iterator->dirs, iterator->depth);

				g_free(path);
				g_array_remove_index(iterator->paths, iterator->depth);

				iterator->depth--;

				continue;
			}
		}
		else
		{
			g_autofree gchar* root_path = NULL;

			root_path = g_build_filename(iterator->root, iterator->current, NULL);

			if (g_file_test(root_path, G_FILE_TEST_IS_DIR))
			{
				dir = g_dir_open(root_path, 0, NULL);

				if (dir != NULL)
				{
					path = g_strdup(iterator->current);

					g_array_append_val(iterator->dirs, dir);
					g_array_append_val(iterator->paths, path);

					iterator->depth++;
				}

				continue;
			}
		}

		break;
	}

	return (iterator->current != NULL);
}

gchar const*
j_dir_iterator_get(JDirIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, NULL);
	g_return_val_if_fail(iterator->current != NULL, NULL);

	return iterator->current;
}

/**
 * @}
 **/
