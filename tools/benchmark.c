/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <glib.h>
#include <glib-object.h>

#include <julea.h>

static
void
on_operation_completed (JBatch* batch, gboolean ret, gpointer user_data)
{
	g_print("Operation %p completed! (ret=%d, user_data=%p)\n", (gpointer)batch, ret, user_data);
}

int
main (int argc, char** argv)
{
	JCollectionIterator* citerator;
	JStore* store;
	JStoreIterator* siterator;
	JSemantics* semantics;
	JBatch* delete_batch;
	JBatch* batch;

	j_init();

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_EVENTUAL);
//	j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_EVENTUAL);

	delete_batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	batch = j_batch_new(semantics);

	store = j_store_new("JULEA");
	j_create_store(store, batch);

	j_batch_execute(batch);

	for (guint i = 0; i < 10; i++)
	{
		JCollection* collection;
		gchar* name;

		name = g_strdup_printf("test-%u", i);

		collection = j_collection_new(name);
		j_store_create_collection(store, collection, batch);

		g_free(name);

		j_batch_execute(batch);

		for (guint j = 0; j < 10000; j++)
		{
			JItem* item;

			name = g_strdup_printf("test-%u", j);

			item = j_item_new(name);
			j_collection_create_item(collection, item, batch);
			j_collection_delete_item(collection, item, delete_batch);
			j_item_unref(item);

			g_free(name);
		}

		j_store_delete_collection(store, collection, delete_batch);
		j_collection_unref(collection);

		j_batch_execute(batch);
	}

	j_delete_store(store, delete_batch);

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

		citerator = j_collection_iterator_new(first_collection);
		j_collection_unref(first_collection);

		is_first = TRUE;

		while (j_collection_iterator_next(citerator))
		{
			JItem* item = j_collection_iterator_get(citerator);

			j_item_get_status(item, J_ITEM_STATUS_MODIFICATION_TIME, batch);
			j_batch_execute(batch);

			g_print("%s (%" G_GUINT64_FORMAT ") ", j_item_get_name(item), j_item_get_modification_time(item));

			if (is_first)
			{
				gchar* buf;
				guint64 bytes;

				is_first = FALSE;

				buf = g_new0(gchar, 1024 * 1024 + 1);

				j_item_write(item, buf, 1024 * 1024 + 1, 0, &bytes, batch);
				j_item_write(item, buf, 1024 * 1024 + 1, 1024 * 1024 + 1, &bytes, batch);
				j_item_read(item, buf, 1024 * 1024 + 1, 1024 * 1024 + 1, &bytes, batch);
				j_item_read(item, buf, 1024 * 1024 + 1, 0, &bytes, batch);

				j_batch_execute_async(batch, on_operation_completed, NULL);
				j_batch_wait(batch);

				j_item_get_status(item, J_ITEM_STATUS_SIZE, batch);
				j_batch_execute(batch);

				g_print("(%" G_GUINT64_FORMAT ") ", j_item_get_size(item));

				g_free(buf);
			}

			j_item_unref(item);
		}

		g_print("\n");

		j_collection_iterator_free(citerator);
	}

	j_batch_execute(delete_batch);

	j_semantics_unref(semantics);
	j_store_unref(store);

	j_batch_unref(delete_batch);
	j_batch_unref(batch);

	j_fini();

	return 0;
}
