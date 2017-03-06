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

#include "julea.h"

#include <juri.h>

#include <gio/gio.h>

gboolean
j_cmd_copy (gchar const** arguments)
{
	gboolean ret = TRUE;
	JBatch* batch;
	JURI* uri[2] = { NULL, NULL };
	GError* error;
	GFile* file;
	GFileIOStream* stream[2] = { NULL, NULL };
	gchar* buffer;
	guint64 offset;
	guint i;

	if (j_cmd_arguments_length(arguments) != 2)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	for (i = 0; i <= 1; i++)
	{
		if ((uri[i] = j_uri_new(arguments[i])) != NULL)
		{
			error = NULL;

			if (j_uri_get_item_name(uri[i]) == NULL)
			{
				ret = FALSE;
				j_cmd_usage();
				goto end;
			}

			if (i == 0)
			{
				if (!j_uri_get(uri[i], &error))
				{
					ret = FALSE;
					g_print("Error: %s\n", error->message);
					g_error_free(error);
					goto end;
				}
			}
			else if (i == 1)
			{
				JItem* item;

				if (j_uri_get(uri[i], &error))
				{
					ret = FALSE;

					if (j_uri_get_item(uri[i]) != NULL)
					{
						g_print("Error: Item “%s” already exists.\n", j_item_get_name(j_uri_get_item(uri[i])));
					}

					goto end;
				}
				else
				{
					if (!j_cmd_error_last(uri[i]))
					{
						ret = FALSE;
						g_print("Error: %s\n", error->message);
						g_error_free(error);
						goto end;
					}

					g_error_free(error);
				}

				batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

				item = j_item_create(j_uri_get_collection(uri[i]), j_uri_get_item_name(uri[i]), NULL, batch);
				j_item_unref(item);

				j_batch_execute(batch);

				j_batch_unref(batch);

				j_uri_get(uri[i], NULL);
			}
		}
		else
		{
			error = NULL;

			file = g_file_new_for_commandline_arg(arguments[i]);

			if (i == 0)
			{
				stream[i] = g_file_open_readwrite(file, NULL, &error);
			}
			else if (i == 1)
			{
				stream[i] = g_file_create_readwrite(file, G_FILE_CREATE_NONE, NULL, &error);
			}

			g_object_unref(file);

			if (stream[i] == NULL)
			{
				ret = FALSE;

				if (error != NULL)
				{
					g_print("Error: %s\n", error->message);
					g_error_free(error);
				}

				goto end;
			}

		}
	}

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	offset = 0;
	buffer = g_new(gchar, 1024 * 1024);

	while (TRUE)
	{
		guint64 bytes_read = 0;

		if (uri[0] != NULL)
		{
			guint64 nbytes;

			j_item_read(j_uri_get_item(uri[0]), buffer, 1024 * 1024, offset, &nbytes, batch);
			j_batch_execute(batch);

			bytes_read = nbytes;
		}
		else if (stream[0] != NULL)
		{
			GInputStream* input;
			gsize nbytes;

			input = g_io_stream_get_input_stream(G_IO_STREAM(stream[0]));

			g_input_stream_read_all(input, buffer, 1024 * 1024, &nbytes, NULL, NULL);
			bytes_read = nbytes;
		}

		if (uri[1] != NULL)
		{
			guint64 dummy;

			j_item_write(j_uri_get_item(uri[1]), buffer, bytes_read, offset, &dummy, batch);
			j_batch_execute(batch);
		}
		else if (stream[1] != NULL)
		{
			GOutputStream* output;

			output = g_io_stream_get_output_stream(G_IO_STREAM(stream[1]));

			g_output_stream_write_all(output, buffer, bytes_read, NULL, NULL, NULL);
		}

		offset += bytes_read;

		if (bytes_read < 1024 * 1024)
		{
			break;
		}
	}

	j_batch_unref(batch);

	g_free(buffer);

end:
	for (i = 0; i <= 1; i++)
	{
		if (stream[i] != NULL)
		{
			g_object_unref(stream[i]);
		}

		if (uri[i] != NULL)
		{
			j_uri_free(uri[i]);
		}
	}

	return ret;
}
