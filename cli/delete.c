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
j_cmd_delete (gchar const** arguments)
{
	gboolean ret = TRUE;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JObjectURI) duri = NULL;
	g_autoptr(JObjectURI) ouri = NULL;
	g_autoptr(JURI) uri = NULL;
	GError* error = NULL;

	if (j_cmd_arguments_length(arguments) != 1)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	ouri = j_object_uri_new(arguments[0], J_OBJECT_URI_SCHEME_OBJECT);

	if (ouri != NULL)
	{
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_object_delete(j_object_uri_get_object(ouri), batch);
		ret = j_batch_execute(batch);

		goto end;
	}

	duri = j_object_uri_new(arguments[0], J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT);

	if (duri != NULL)
	{
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_distributed_object_delete(j_object_uri_get_distributed_object(duri), batch);
		ret = j_batch_execute(batch);

		goto end;
	}

	uri = j_uri_new(arguments[0]);

	if (uri != NULL)
	{
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
			j_item_delete(j_uri_get_item(uri), batch);
			ret = j_batch_execute(batch);
		}
		else if (j_uri_get_collection(uri) != NULL)
		{
			j_collection_delete(j_uri_get_collection(uri), batch);
			ret = j_batch_execute(batch);
		}
		else
		{
			ret = FALSE;
			j_cmd_usage();
		}

		goto end;
	}

	ret = FALSE;
	g_print("Error: Invalid argument “%s”.\n", arguments[0]);

end:
	return ret;
}
