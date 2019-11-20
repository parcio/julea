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
j_cmd_status (gchar const** arguments)
{
	gboolean ret = TRUE;
	g_autoptr(JBatch) batch = NULL;
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
		g_autoptr(GDateTime) date_time = NULL;
		g_autofree gchar* modification_time_string = NULL;
		g_autofree gchar* size_string = NULL;
		gint64 modification_time;
		guint64 size;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_object_status(j_object_uri_get_object(ouri), &modification_time, &size, batch);
		ret = j_batch_execute(batch);

		if (ret)
		{
			date_time = g_date_time_new_from_unix_local(modification_time / G_USEC_PER_SEC);
			modification_time_string = g_date_time_format(date_time, "%Y-%m-%d %H:%M:%S");
			size_string = g_format_size(size);

			g_print("Modification time: %s.%06" G_GUINT64_FORMAT "\n", modification_time_string, modification_time % G_USEC_PER_SEC);
			g_print("Size:              %s\n", size_string);
		}

		goto end;
	}

	duri = j_object_uri_new(arguments[0], J_OBJECT_URI_SCHEME_DISTRIBUTED_OBJECT);

	if (duri != NULL)
	{
		g_autoptr(GDateTime) date_time = NULL;
		g_autofree gchar* modification_time_string = NULL;
		g_autofree gchar* size_string = NULL;
		gint64 modification_time;
		guint64 size;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_distributed_object_status(j_object_uri_get_distributed_object(duri), &modification_time, &size, batch);
		ret = j_batch_execute(batch);

		if (ret)
		{
			date_time = g_date_time_new_from_unix_local(modification_time / G_USEC_PER_SEC);
			modification_time_string = g_date_time_format(date_time, "%Y-%m-%d %H:%M:%S");
			size_string = g_format_size(size);

			g_print("Modification time: %s.%06" G_GUINT64_FORMAT "\n", modification_time_string, modification_time % G_USEC_PER_SEC);
			g_print("Size:              %s\n", size_string);
		}

		goto end;
	}

	kuri = j_kv_uri_new(arguments[0], J_KV_URI_SCHEME_KV);

	if (kuri != NULL)
	{
		g_autofree gchar* size_string = NULL;
		gpointer value;
		guint32 len;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(j_kv_uri_get_kv(kuri), &value, &len, batch);
		ret = j_batch_execute(batch);

		if (ret)
		{
			size_string = g_format_size(len);

			g_print("Size:              %s\n", size_string);

			g_free(value);
		}

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
			JCredentials* credentials;
			g_autoptr(GDateTime) date_time = NULL;
			g_autofree gchar* modification_time_string = NULL;
			g_autofree gchar* size_string = NULL;
			guint64 modification_time;
			guint64 size;

			j_item_get_status(j_uri_get_item(uri), batch);
			ret = j_batch_execute(batch);

			if (ret)
			{
				credentials = j_item_get_credentials(j_uri_get_item(uri));
				modification_time = j_item_get_modification_time(j_uri_get_item(uri));
				size = j_item_get_size(j_uri_get_item(uri));

				date_time = g_date_time_new_from_unix_local(modification_time / G_USEC_PER_SEC);
				modification_time_string = g_date_time_format(date_time, "%Y-%m-%d %H:%M:%S");
				size_string = g_format_size(size);

				g_print("User:              %" G_GUINT32_FORMAT "\n", j_credentials_get_user(credentials));
				g_print("Group:             %" G_GUINT32_FORMAT "\n", j_credentials_get_group(credentials));
				g_print("Modification time: %s.%06" G_GUINT64_FORMAT "\n", modification_time_string, modification_time % G_USEC_PER_SEC);
				g_print("Size:              %s\n", size_string);

				j_credentials_unref(credentials);
			}
		}
		else if (j_uri_get_collection(uri) != NULL)
		{

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
