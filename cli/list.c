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

#include "cli.h"

gboolean
j_cmd_list(gchar const** arguments)
{
	gboolean ret = TRUE;
	g_autoptr(JURI) uri = NULL;
	GError* error = NULL;

	if (j_cmd_arguments_length(arguments) != 1)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	if ((uri = j_uri_new(arguments[0])) == NULL)
	{
		ret = FALSE;
		g_print("Error: Invalid argument “%s”.\n", arguments[0]);
		goto end;
	}

	if (!j_uri_get(uri, &error))
	{
		ret = FALSE;
		g_print("Error: %s\n", error->message);
		g_error_free(error);
		goto end;
	}

	if (j_uri_get_item(uri) != NULL)
	{
		ret = FALSE;
		j_cmd_usage();
	}
	else if (j_uri_get_collection(uri) != NULL)
	{
		JItemIterator* iterator;

		iterator = j_item_iterator_new(j_uri_get_collection(uri));

		while (j_item_iterator_next(iterator))
		{
			JItem* item_ = j_item_iterator_get(iterator);

			g_print("%s\n", j_item_get_name(item_));

			j_item_unref(item_);
		}

		j_item_iterator_free(iterator);
	}
	else
	{
		JCollectionIterator* iterator;

		iterator = j_collection_iterator_new();

		while (j_collection_iterator_next(iterator))
		{
			JCollection* collection_ = j_collection_iterator_get(iterator);

			g_print("%s\n", j_collection_get_name(collection_));

			j_collection_unref(collection_);
		}

		j_collection_iterator_free(iterator);
	}

end:
	return ret;
}
