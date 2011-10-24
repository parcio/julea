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

void
j_cmd_parse (gchar const* store_name, gchar const* collection_name, gchar const* item_name, JStore** store, JCollection** collection, JItem** item)
{
	JOperation* operation;

	operation = j_operation_new();

	if (store_name != NULL)
	{
		j_get_store(store, store_name, operation);
		j_operation_execute(operation);

		if (*store == NULL)
		{
			return;
		}
	}

	if (collection_name)
	{
		j_store_get_collection(*store, collection, collection_name, operation);
		j_operation_execute(operation);

		if (*collection == NULL)
		{
			return;
		}
	}

	if (item_name)
	{
		j_collection_get_item(*collection, item, item_name, J_ITEM_STATUS_NONE, operation);
		j_operation_execute(operation);

		if (*item == NULL)
		{
			return;
		}
	}
}

static gboolean opt_create = FALSE;
static gboolean opt_list = FALSE;
static gboolean opt_remove = FALSE;
static gboolean opt_status = FALSE;

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;
	gchar const* store_name = NULL;
	gchar const* collection_name = NULL;
	gchar const* item_name = NULL;

	GOptionEntry entries[] = {
		{ "create", 'c', 0, G_OPTION_ARG_NONE, &opt_create, "Create a store/collection/item", NULL },
		{ "list", 'l', 0, G_OPTION_ARG_NONE, &opt_list, "List stores/collections/items", NULL },
		{ "remove", 'r', 0, G_OPTION_ARG_NONE, &opt_remove, "Remove a store/collection/item", NULL },
		{ "status", 's', 0, G_OPTION_ARG_NONE, &opt_status, "Get item status", NULL },
		{ NULL }
	};

	context = g_option_context_new("[STORE] [COLLECTION] [ITEM]");
	g_option_context_add_main_entries(context, entries, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		g_option_context_free(context);

		if (error)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	if (FALSE)
	{
		gchar* help;

		help = g_option_context_get_help(context, TRUE, NULL);
		g_option_context_free(context);

		g_print("%s", help);
		g_free(help);

		return 1;
	}

	g_option_context_free(context);

	switch (argc)
	{
		case 4:
			item_name = argv[3];
		case 3:
			collection_name = argv[2];
		case 2:
			store_name = argv[1];
			break;
		default:
			g_warn_if_reached();
	}

	if (!j_init(&argc, &argv))
	{
		g_printerr("Could not initialize.\n");
		return 1;
	}

	if (opt_create)
	{
		j_cmd_create(store_name, collection_name, item_name);
	}
	else if (opt_list)
	{
		j_cmd_list(store_name, collection_name, item_name);
	}
	else if (opt_remove)
	{
		j_cmd_remove(store_name, collection_name, item_name);
	}
	else if (opt_status)
	{
		j_cmd_status(store_name, collection_name, item_name);
	}

	j_fini();

	return 0;
}
