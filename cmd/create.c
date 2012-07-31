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

#include <julea-config.h>

#include "julea.h"

#include <juri.h>

gboolean
j_cmd_create (gchar const** arguments)
{
	gboolean ret = TRUE;
	JOperation* operation;
	JURI* uri = NULL;
	GError* error = NULL;

	if (j_cmd_arguments_length(arguments) != 1)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	if ((uri = j_uri_new(arguments[0])) == NULL)
	{
		ret = FALSE;
		g_print("Error: Invalid argument “%s”.\n", arguments[0]);
		goto end;
	}

	if (j_uri_get(uri, &error))
	{
		ret = FALSE;

		if (j_uri_get_item(uri) != NULL)
		{
			g_print("Error: Item “%s” already exists.\n", j_item_get_name(j_uri_get_item(uri)));
		}
		else if (j_uri_get_collection(uri) != NULL)
		{
			g_print("Error: Collection “%s” already exists.\n", j_collection_get_name(j_uri_get_collection(uri)));
		}
		else if (j_uri_get_store(uri) != NULL)
		{
			g_print("Error: Store “%s” already exists.\n", j_store_get_name(j_uri_get_store(uri)));
		}

		goto end;
	}
	else
	{
		if (!j_cmd_error_last(uri))
		{
			ret = FALSE;
			g_print("Error: %s\n", error->message);
			g_error_free(error);
			goto end;
		}

		g_error_free(error);
	}

	operation = j_operation_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (j_uri_get_collection(uri) != NULL)
	{
		JItem* item;

		item = j_item_new(j_uri_get_item_name(uri));
		j_collection_create_item(j_uri_get_collection(uri), item, operation);
		j_operation_execute(operation);
	}
	else if (j_uri_get_store(uri) != NULL)
	{
		JCollection* collection;

		collection = j_collection_new(j_uri_get_collection_name(uri));
		j_store_create_collection(j_uri_get_store(uri), collection, operation);
		j_operation_execute(operation);
	}
	else
	{
		JStore* store;

		store = j_store_new(j_uri_get_store_name(uri));
		j_create_store(store, operation);
		j_operation_execute(operation);
	}

	j_operation_unref(operation);

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
