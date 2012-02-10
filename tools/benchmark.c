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

#include <glib.h>
#include <glib-object.h>

#include <julea.h>

static
void
on_operation_completed (JOperation* operation, gpointer data)
{
	g_print("Operation %p completed! (user_data=%p)\n", (gpointer)operation, data);
}

int
main (int argc, char** argv)
{
	JCollectionIterator* citerator;
	JStore* store;
	JStoreIterator* siterator;
	JSemantics* semantics;
	JOperation* delete_operation;
	JOperation* operation;

	if (!j_init(&argc, &argv))
	{
		g_printerr("Could not initialize.\n");
		return 1;
	}

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_EVENTUAL);
//	j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_EVENTUAL);

	delete_operation = j_operation_new(NULL);
	operation = j_operation_new(semantics);

	store = j_store_new("JULEA");
	j_create_store(store, operation);

	j_operation_execute(operation);

	for (guint i = 0; i < 10; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%u", i);

		collection = j_collection_new(name);
		j_store_create_collection(store, collection, operation);

		g_free(name);

		j_operation_execute(operation);

		for (guint j = 0; j < 10000; j++)
		{
			JItem* item;

			name = g_strdup_printf("test-%u", j);

			item = j_item_new(name);
			j_collection_create_item(collection, item, operation);
			j_collection_delete_item(collection, item, delete_operation);
			j_item_unref(item);

			g_free(name);
		}

		j_store_delete_collection(store, collection, delete_operation);
		j_collection_unref(collection);

		j_operation_execute(operation);
	}

	j_delete_store(store, delete_operation);

	{
		JCollection* first_collection = NULL;
		gboolean is_first;

		siterator = j_store_iterator_new(store);

		is_first = TRUE;

		while (j_store_iterator_next(siterator))
		{
			JCollection* collection = j_store_iterator_get(siterator);

			g_print("%s ", j_collection_get_name(collection));

			if (is_first)
			{
				is_first = FALSE;
				first_collection = j_collection_ref(collection);
			}

			j_collection_unref(collection);
		}

		g_print("\n");

		j_store_iterator_free(siterator);

		citerator = j_collection_iterator_new(first_collection, J_ITEM_STATUS_MODIFICATION_TIME);
		j_collection_unref(first_collection);

		is_first = TRUE;

		while (j_collection_iterator_next(citerator))
		{
			JItem* item = j_collection_iterator_get(citerator);

			g_print("%s (%" G_GUINT64_FORMAT ") ", j_item_get_name(item), j_item_get_modification_time(item));

			if (is_first)
			{
				gchar* buf;
				guint64 bytes;

				is_first = FALSE;

				buf = g_new0(gchar, 1024 * 1024 + 1);

				j_item_write(item, buf, 1024 * 1024 + 1, 0, &bytes, operation);
				j_item_write(item, buf, 1024 * 1024 + 1, 1024 * 1024 + 1, &bytes, operation);
				j_item_read(item, buf, 1024 * 1024 + 1, 1024 * 1024 + 1, &bytes, operation);
				j_item_read(item, buf, 1024 * 1024 + 1, 0, &bytes, operation);

				j_operation_execute_async(operation, on_operation_completed, NULL);
				j_operation_wait(operation);

				j_item_get_status(item, J_ITEM_STATUS_SIZE, operation);
				j_operation_execute(operation);

				g_print("(%" G_GUINT64_FORMAT ") ", j_item_get_size(item));

				g_free(buf);
			}

			j_item_unref(item);
		}

		g_print("\n");

		j_collection_iterator_free(citerator);
	}

	j_operation_execute(delete_operation);

	j_semantics_unref(semantics);
	j_store_unref(store);

	j_operation_free(delete_operation);
	j_operation_free(operation);

	j_fini();

	return 0;
}
