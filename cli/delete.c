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

gboolean
j_cmd_delete (gchar const** arguments)
{
	gboolean ret = TRUE;
	JBatch* batch;
	JURI* uri = NULL;
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

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (j_uri_get_item(uri) != NULL)
	{
		j_item_delete(j_uri_get_collection(uri), j_uri_get_item(uri), batch);
		j_batch_execute(batch);
	}
	else if (j_uri_get_collection(uri) != NULL)
	{
		j_collection_delete(j_uri_get_collection(uri), batch);
		j_batch_execute(batch);
	}
	else
	{
		ret = FALSE;
		j_cmd_usage();
	}

	j_batch_unref(batch);

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
