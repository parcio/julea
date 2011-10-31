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

gboolean
j_cmd_create (gchar const* store_name, gchar const* collection_name, gchar const* item_name, gchar const** remaining)
{
	gboolean ret = TRUE;
	JStore* store = NULL;
	JCollection* collection = NULL;
	JItem* item = NULL;
	JOperation* operation;

	if (j_cmd_remaining_length(remaining) > 0)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	if (!j_cmd_parse(store_name, collection_name, item_name, &store, &collection, &item)
	    && !j_cmd_error_last(store_name, collection_name, item_name, store, collection, item))
	{
		gchar* error;

		ret = FALSE;

		error = j_cmd_error_exists(store_name, collection_name, item_name, store, collection, item);
		g_print("Error: %s\n", error);
		g_free(error);

		goto end;
	}

	operation = j_operation_new();

	if (item != NULL)
	{
		ret = FALSE;
		g_print("Error: Item “%s” already exists.\n", j_item_get_name(item));
	}
	else if (collection != NULL)
	{
		if (item_name != NULL)
		{
			item = j_item_new(item_name);
			j_collection_add_item(collection, item, operation);
			j_operation_execute(operation);
		}
		else
		{
			ret = FALSE;
			g_print("Error: Collection “%s” already exists.\n", j_collection_get_name(collection));
		}
	}
	else if (store != NULL)
	{
		if (collection_name != NULL)
		{
			collection = j_collection_new(collection_name);
			j_store_add_collection(store, collection, operation);
			j_operation_execute(operation);
		}
		else
		{
			ret = FALSE;
			g_print("Error: Store “%s” already exists.\n", j_store_get_name(store));
		}
	}
	else
	{
		if (store_name != NULL)
		{
			store = j_store_new(store_name);
			j_add_store(store, operation);
			j_operation_execute(operation);
		}
		else
		{
			ret = FALSE;
			j_cmd_usage();
		}
	}

	j_operation_free(operation);

end:
	if (item != NULL)
	{
		j_item_unref(item);
	}

	if (collection != NULL)
	{
		j_collection_unref(collection);
	}

	if (store != NULL)
	{
		j_store_unref(store);
	}

	return ret;
}
