/*
 * Copyright (c) 2010-2011 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "julea.h"

#include <juri.h>

#include <gio/gio.h>

gboolean
j_cmd_copy (gchar const** arguments)
{
	gboolean ret = TRUE;
	JOperation* operation;
	JURI* uri;
	GError* error = NULL;
	GFile* file;
	GFileOutputStream* stream;

	if (j_cmd_arguments_length(arguments) != 2)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	/* FIXME */
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

	file = g_file_new_for_commandline_arg(arguments[1]);
	stream = g_file_replace(file, NULL, FALSE, G_FILE_CREATE_NONE, NULL, NULL);
	g_object_unref(file);

	if (stream == NULL)
	{
		ret = FALSE;
		g_print("Error: Can not open file “%s”.\n", arguments[1]);
		goto end;
	}

	operation = j_operation_new();

	if (j_uri_get_item(uri) != NULL)
	{
		/* FIXME */
	}
	else
	{
		ret = FALSE;
		j_cmd_usage();
	}

	j_operation_free(operation);

	g_object_unref(stream);

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
