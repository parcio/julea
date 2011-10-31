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

#include <locale.h>
#include <stdlib.h>

guint
j_cmd_remaining_length (gchar const** remaining)
{
	guint i = 0;

	for (; *remaining != NULL; remaining++)
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
	g_print("  create  STORE  [COLLECTION] [ITEM]\n");
	g_print("  copy    STORE   COLLECTION   ITEM   FILE\n");
	g_print("  list   [STORE] [COLLECTION]\n");
	g_print("  remove  STORE  [COLLECTION] [ITEM]\n");
	g_print("  status  STORE   COLLECTION   ITEM\n");
	g_print("\n");
}

gboolean
j_cmd_parse (gchar const* store_name, gchar const* collection_name, gchar const* item_name, JStore** store, JCollection** collection, JItem** item)
{
	JOperation* operation;

	g_return_val_if_fail(store != NULL && *store == NULL, FALSE);
	g_return_val_if_fail(collection != NULL && *collection == NULL, FALSE);
	g_return_val_if_fail(item != NULL && *item == NULL, FALSE);

	operation = j_operation_new();

	if (store_name != NULL)
	{
		j_get_store(store, store_name, operation);
		j_operation_execute(operation);

		if (*store == NULL)
		{
			return FALSE;
		}
	}

	if (collection_name != NULL)
	{
		j_store_get_collection(*store, collection, collection_name, operation);
		j_operation_execute(operation);

		if (*collection == NULL)
		{
			return FALSE;
		}
	}

	if (item_name != NULL)
	{
		j_collection_get_item(*collection, item, item_name, J_ITEM_STATUS_NONE, operation);
		j_operation_execute(operation);

		if (*item == NULL)
		{
			return FALSE;
		}
	}

	return TRUE;
}

gchar*
j_cmd_error_exists (gchar const* store_name, gchar const* collection_name, gchar const* item_name, JStore* store, JCollection* collection, JItem* item)
{
	gchar* error = NULL;

	if (store == NULL && store_name != NULL)
	{
		error = g_strdup_printf("Store “%s” does not exist.", store_name);
	}
	else if (collection == NULL && collection_name != NULL)
	{
		error = g_strdup_printf("Collection “%s” does not exist.", collection_name);
	}
	else if (item == NULL && item_name != NULL)
	{
		error = g_strdup_printf("Item “%s” does not exist.", item_name);
	}

	if (error == NULL)
	{
		error = g_strdup("Unknown error.");
	}

	return error;
}

gboolean
j_cmd_error_last (gchar const* store_name, gchar const* collection_name, gchar const* item_name, JStore* store, JCollection* collection, JItem* item)
{
	gboolean ret = FALSE;

	if (store == NULL && store_name != NULL)
	{
		ret = (collection_name == NULL && item_name == NULL);
	}
	else if (collection == NULL && collection_name != NULL)
	{
		ret = (item_name == NULL);
	}
	else if (item == NULL && item_name != NULL)
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
	gchar const* store_name = NULL;
	gchar const* collection_name = NULL;
	gchar const* item_name = NULL;
	gchar const** remaining = NULL;
	gint i;

	setlocale(LC_ALL, "");

	basename = g_path_get_basename(argv[0]);
	g_set_prgname(basename);
	g_free(basename);

	switch (argc)
	{
		default:
		case 5:
			item_name = argv[4];
		case 4:
			collection_name = argv[3];
		case 3:
			store_name = argv[2];
		case 2:
			command = argv[1];
			break;
		case 1:
			j_cmd_usage();
			return 1;
	}

	if (!j_init(&argc, &argv))
	{
		g_printerr("Could not initialize.\n");
		return 1;
	}

	remaining = g_new(gchar const*, MAX(argc - 4, 1));

	for (i = 0; i + 5 < argc; i++)
	{
		remaining[i] = argv[i + 5];
	}

	remaining[MAX(argc - 5, 0)] = NULL;

	if (g_strcmp0(command, "copy") == 0)
	{
		success = j_cmd_copy(store_name, collection_name, item_name, remaining);
	}
	else if (g_strcmp0(command, "create") == 0)
	{
		success = j_cmd_create(store_name, collection_name, item_name, remaining);
	}
	else if (g_strcmp0(command, "list") == 0)
	{
		success = j_cmd_list(store_name, collection_name, item_name, remaining);
	}
	else if (g_strcmp0(command, "remove") == 0)
	{
		success = j_cmd_remove(store_name, collection_name, item_name, remaining);
	}
	else if (g_strcmp0(command, "status") == 0)
	{
		success = j_cmd_status(store_name, collection_name, item_name, remaining);
	}
	else
	{
		success = FALSE;
		j_cmd_usage();
	}

	g_free(remaining);

	j_fini();

	return (success) ? 0 : 1;
}
