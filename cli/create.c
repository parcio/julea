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

#include <string.h>

gboolean
j_cmd_create (gchar const** arguments, gboolean with_parents)
{
	gboolean ret = TRUE;
	g_autoptr(JObjectURI) duri = NULL;
	g_autoptr(JObjectURI) ouri = NULL;
	g_autoptr(JKVURI) kuri = NULL;
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
		g_autoptr(JBatch) batch = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_object_create(j_object_uri_get_object(ouri), batch);
		ret = j_batch_execute(batch);

		goto end;
	}

	duri = j_object_uri_new(arguments[0], J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT);

	if (duri != NULL)
	{
		g_autoptr(JBatch) batch = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_distributed_object_create(j_object_uri_get_distributed_object(duri), batch);
		ret = j_batch_execute(batch);

		goto end;
	}

	kuri = j_kv_uri_new(arguments[0], J_KV_URI_SCHEME_KV);

	if (kuri != NULL)
	{
		g_autoptr(JBatch) batch = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_put(j_kv_uri_get_kv(kuri), g_strdup("empty"), 6, g_free, batch);
		ret = j_batch_execute(batch);

		goto end;
	}

	uri = j_uri_new(arguments[0]);

	if (uri != NULL)
	{
		if (!j_uri_create(uri, with_parents, &error))
		{
			ret = FALSE;
			g_print("Error: %s\n", error->message);
			g_error_free(error);
		}

		goto end;
	}

	ret = FALSE;
	g_print("Error: Invalid argument “%s”.\n", arguments[0]);

end:
	return ret;
}
