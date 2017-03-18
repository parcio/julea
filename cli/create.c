/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include <string.h>

gboolean
j_cmd_create (gchar const** arguments, gboolean with_parents)
{
	gboolean ret = TRUE;
	JURI* uri = NULL;
	GError* error = NULL;

	if (j_cmd_arguments_length(arguments) != 1)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	if (g_str_has_prefix(arguments[0], "object://"))
	{
		JBatch* batch;
		gchar** parts = NULL;
		guint parts_len;
		guint32 index;

		parts = g_strsplit(arguments[0] + strlen("object://"), "/", 3);
		parts_len = g_strv_length(parts);

		if (parts_len != 3)
		{
			ret = FALSE;
			j_cmd_usage();
			goto end;
		}

		index = g_ascii_strtoull(parts[0], NULL, 10);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		j_object_create(parts[1], parts[2], index, batch);

		j_batch_execute(batch);
		j_batch_unref(batch);

		g_strfreev(parts);
	}
	else
	{
		if ((uri = j_uri_new(arguments[0])) == NULL)
		{
			ret = FALSE;
			g_print("Error: Invalid argument “%s”.\n", arguments[0]);
			goto end;
		}

		if (!j_uri_create(uri, with_parents, &error))
		{
			ret = FALSE;
			g_print("Error: %s\n", error->message);
			g_error_free(error);
		}
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
