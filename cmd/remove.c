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
j_cmd_remove (gchar const* store_name, gchar const* collection_name, gchar const* item_name)
{
	gboolean ret = TRUE;
	JStore* store = NULL;
	JCollection* collection = NULL;
	JItem* item = NULL;
	JOperation* operation;

	if (!j_cmd_parse(store_name, collection_name, item_name, &store, &collection, &item))
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
		j_collection_delete_item(collection, item, operation);
		j_operation_execute(operation);
	}
	else if (collection != NULL)
	{
		j_store_delete_collection(store, collection, operation);
		j_operation_execute(operation);
	}
	else if (store != NULL)
	{
		j_delete_store(store, operation);
		j_operation_execute(operation);
	}
	else
	{
		ret = FALSE;
		j_cmd_usage();
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
