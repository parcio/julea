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
j_cmd_create (gchar const* store_name, gchar const* collection_name, gchar const* item_name)
{
	JStore* store = NULL;
	JCollection* collection = NULL;
	JItem* item = NULL;
	JOperation* operation;

	j_cmd_parse(store_name, collection_name, item_name, &store, &collection, &item);

	operation = j_operation_new();

	if (item != NULL)
	{
	}
	else if (collection != NULL)
	{
		item = j_item_new(item_name);
		j_collection_add_item(collection, item, operation);
		j_operation_execute(operation);
	}
	else if (store != NULL)
	{
		collection = j_collection_new(collection_name);
		j_store_add_collection(store, collection, operation);
		j_operation_execute(operation);
	}
	else
	{
		store = j_store_new(store_name);
		j_add_store(store, operation);
		j_operation_execute(operation);
	}

	j_operation_free(operation);
}
