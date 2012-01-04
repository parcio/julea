/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <locale.h>
#include <stdlib.h>

guint
j_cmd_arguments_length (gchar const** arguments)
{
	guint i = 0;

	for (; *arguments != NULL; arguments++)
	{
		i++;
	}

	return i;
}

void
j_cmd_usage (void)
{
	g_print("Usage:\n");
	g_print("  %s COMMAND\n", g_get_prgname());
	g_print("\n");
	g_print("Commands:\n");
	g_print("  create julea://store/[collection/[item]]\n");
	g_print("  copy   julea://store/collection/item julea://store/collection/item\n");
	g_print("         julea://store/collection/item file://file\n");
	g_print("         file://file julea://store/collection/item\n");
	g_print("  delete julea://store/[collection/[item]]\n");
	g_print("  list   julea://[store/[collection]]\n");
	g_print("  status julea://store/collection/item\n");
	g_print("\n");
}

gboolean
j_cmd_error_last (JURI* uri)
{
	gboolean ret = FALSE;

	if (j_uri_get_store(uri) == NULL && j_uri_get_store_name(uri) != NULL)
	{
		ret = (j_uri_get_collection_name(uri) == NULL && j_uri_get_item_name(uri) == NULL);
	}
	else if (j_uri_get_collection(uri) == NULL && j_uri_get_collection_name(uri) != NULL)
	{
		ret = (j_uri_get_item_name(uri) == NULL);
	}
	else if (j_uri_get_item(uri) == NULL && j_uri_get_item_name(uri) != NULL)
	{
		ret = TRUE;
	}

	return ret;
}

int
main (int argc, char** argv)
{
	gboolean success;
	gchar* basename;
	gchar const* command = NULL;
	gchar const** arguments = NULL;
	gint i;

	setlocale(LC_ALL, "");

	basename = g_path_get_basename(argv[0]);
	g_set_prgname(basename);
	g_free(basename);

	if (argc <= 2)
	{
		j_cmd_usage();
		return 1;
	}

	command = argv[1];

	if (!j_init(&argc, &argv))
	{
		g_printerr("Could not initialize.\n");
		return 1;
	}

	arguments = g_new(gchar const*, argc - 1);

	for (i = 2; i < argc; i++)
	{
		arguments[i - 2] = argv[i];
	}

	arguments[argc - 2] = NULL;

	if (g_strcmp0(command, "copy") == 0)
	{
		success = j_cmd_copy(arguments);
	}
	else if (g_strcmp0(command, "create") == 0)
	{
		success = j_cmd_create(arguments);
	}
	else if (g_strcmp0(command, "delete") == 0)
	{
		success = j_cmd_delete(arguments);
	}
	else if (g_strcmp0(command, "list") == 0)
	{
		success = j_cmd_list(arguments);
	}
	else if (g_strcmp0(command, "status") == 0)
	{
		success = j_cmd_status(arguments);
	}
	else
	{
		success = FALSE;
		j_cmd_usage();
	}

	g_free(arguments);

	j_fini();

	return (success) ? 0 : 1;
}
