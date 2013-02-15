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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <jstore.h>
#include <jstore-internal.h>

#include <jcollection.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jconnection-pool-internal.h>
#include <jhelper-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>

/**
 * \defgroup JStore Store
 *
 * Data structures and functions for managing stores.
 *
 * @{
 **/

/**
 * A JStore.
 **/
struct JStore
{
	/**
	 * The name.
	 **/
	gchar* name;

	struct
	{
		gchar* collections;
	}
	collection;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Creates a new store.
 *
 * \author Michael Kuhn
 *
 * \code
 * JStore* s;
 *
 * s = j_store_new("JULEA");
 * \endcode
 *
 * \param name A store name.
 *
 * \return A new store. Should be freed with j_store_unref().
 **/
JStore*
j_store_new (gchar const* name)
{
	JStore* store;
	/*
	: m_initialized(true),
	*/

	g_return_val_if_fail(name != NULL, NULL);

	store = g_slice_new(JStore);
	store->name = g_strdup(name);
	store->collection.collections = NULL;
	store->ref_count = 1;

	return store;
}

/**
 * Increases an store's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JStore* s;
 *
 * ...
 *
 * j_store_ref(s);
 * \endcode
 *
 * \param store A store.
 *
 * \return #store.
 **/
JStore*
j_store_ref (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	g_atomic_int_inc(&(store->ref_count));

	return store;
}

/**
 * Decreases a store's reference count.
 * When the reference count reaches zero, frees the memory allocated for the store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store A store.
 **/
void
j_store_unref (JStore* store)
{
	g_return_if_fail(store != NULL);

	if (g_atomic_int_dec_and_test(&(store->ref_count)))
	{
		g_free(store->collection.collections);
		g_free(store->name);

		g_slice_free(JStore, store);
	}
}

/**
 * Returns a store's name.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store A store.
 *
 * \return The name.
 **/
gchar const*
j_store_get_name (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	return store->name;
}

/**
 * Creates a collection in a store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store      A store.
 * \param collection A collection.
 * \param batch      A batch.
 **/
void
j_store_create_collection (JStore* store, JCollection* collection, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collection != NULL);

	operation = j_operation_new(J_OPERATION_STORE_CREATE_COLLECTION);
	operation->key = store;
	operation->u.store_create_collection.store = j_store_ref(store);
	operation->u.store_create_collection.collection = j_collection_ref(collection);

	j_collection_set_store(collection, store);

	j_batch_add(batch, operation);
}

/**
 * Gets a collection from a store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store      A store.
 * \param collection A pointer to a collection.
 * \param name       A name.
 * \param batch      A batch.
 **/
void
j_store_get_collection (JStore* store, JCollection** collection, gchar const* name, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(name != NULL);

	operation = j_operation_new(J_OPERATION_STORE_GET_COLLECTION);
	operation->key = store;
	operation->u.store_get_collection.store = j_store_ref(store);
	operation->u.store_get_collection.collection = collection;
	operation->u.store_get_collection.name = g_strdup(name);

	j_batch_add(batch, operation);
}

/**
 * Deletes a collection from a store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store      A store.
 * \param collection A collection.
 * \param batch      A batch.
 **/
void
j_store_delete_collection (JStore* store, JCollection* collection, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collection != NULL);

	operation = j_operation_new(J_OPERATION_STORE_DELETE_COLLECTION);
	operation->key = store;
	operation->u.store_delete_collection.store = j_store_ref(store);
	operation->u.store_delete_collection.collection = j_collection_ref(collection);

	j_batch_add(batch, operation);
}

/* Internal */

/**
 * Returns the MongoDB collection used for collections.
 *
 * \author Michael Kuhn
 *
 * \param store A store.
 *
 * \return The MongoDB collection.
 */
gchar const*
j_store_collection_collections (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	/*
		IsInitialized(true);
	*/

	if (G_UNLIKELY(store->collection.collections == NULL))
	{
		store->collection.collections = g_strdup_printf("%s.Collections", store->name);
	}

	return store->collection.collections;
}

/**
 * Creates collections.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_store_create_collection_internal (JBatch* batch, JList* operations)
{
	JConnection* connection;
	JListIterator* it;
	JStore* store = NULL;
	bson** obj;
	bson index;
	mongo* mongo_connection;
	mongo_write_concern write_concern[1];
	gboolean ret = FALSE;
	guint i;
	guint length;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
	IsInitialized(true);
	*/

	i = 0;
	length = j_list_length(operations);
	obj = g_new(bson*, length);
	it = j_list_iterator_new(operations);

	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection* collection = operation->u.store_create_collection.collection;
		bson* b;

		store = operation->u.store_create_collection.store;
		b = j_collection_serialize(collection);

		obj[i] = b;
		i++;
	}

	j_list_iterator_free(it);

	if (store == NULL)
	{
		goto end;
	}

	j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	bson_init(&index);
	bson_append_int(&index, "Name", 1);
	bson_finish(&index);

	connection = j_connection_pool_pop();
	mongo_connection = j_connection_get_connection(connection);

	mongo_create_index(mongo_connection, j_store_collection_collections(store), &index, MONGO_INDEX_UNIQUE, NULL);
	ret = (mongo_insert_batch(mongo_connection, j_store_collection_collections(store), (bson const**)obj, length, write_concern, MONGO_CONTINUE_ON_ERROR) == MONGO_OK);

	j_connection_pool_push(connection);

	bson_destroy(&index);

	mongo_write_concern_destroy(write_concern);

end:
	for (i = 0; i < length; i++)
	{
		bson_destroy(obj[i]);
		g_slice_free(bson, obj[i]);
	}

	g_free(obj);

	/*
	{
		bson oerr;

		mongo_cmd_get_last_error(mc, store->name, &oerr);
		bson_print(&oerr);
		bson_destroy(&oerr);
	}
	*/

	return ret;
}

/**
 * Deletes collections.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_store_delete_collection_internal (JBatch* batch, JList* operations)
{
	JConnection* connection;
	JListIterator* it;
	JStore* store = NULL;
	mongo* mongo_connection;
	mongo_write_concern write_concern[1];
	gboolean ret = TRUE;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	it = j_list_iterator_new(operations);
	connection = j_connection_pool_pop();
	mongo_connection = j_connection_get_connection(connection);

	/* FIXME do some optimizations for len(operations) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection* collection = operation->u.store_delete_collection.collection;
		bson b;

		store = operation->u.store_delete_collection.store;

		bson_init(&b);
		bson_append_oid(&b, "_id", j_collection_get_id(collection));
		bson_finish(&b);

		/* FIXME do not send write_concern on each remove */
		ret = (mongo_remove(mongo_connection, j_store_collection_collections(store), &b, write_concern) == MONGO_OK) && ret;

		bson_destroy(&b);
	}

	j_connection_pool_push(connection);
	j_list_iterator_free(it);

	mongo_write_concern_destroy(write_concern);

	return ret;
}

/**
 * Gets collections.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_store_get_collection_internal (JBatch* batch, JList* operations)
{
	JConnection* connection;
	JListIterator* it;
	mongo* mongo_connection;
	gboolean ret = TRUE;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(operations);
	connection = j_connection_pool_pop();
	mongo_connection = j_connection_get_connection(connection);

	/* FIXME do some optimizations for len(operations) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection** collection = operation->u.store_get_collection.collection;
		JStore* store = operation->u.store_get_collection.store;
		bson b;
		mongo_cursor* cursor;
		gchar const* name = operation->u.store_get_collection.name;

		bson_init(&b);
		bson_append_string(&b, "Name", name);
		bson_finish(&b);

		cursor = mongo_find(mongo_connection, j_store_collection_collections(store), &b, NULL, 1, 0, 0);

		bson_destroy(&b);

		*collection = NULL;

		// FIXME ret
		while (mongo_cursor_next(cursor) == MONGO_OK)
		{
			*collection = j_collection_new_from_bson(store, mongo_cursor_bson(cursor));
		}

		mongo_cursor_destroy(cursor);
	}

	j_connection_pool_push(connection);
	j_list_iterator_free(it);

	return ret;
}

/*
#include "exception.h"

namespace JULEA
{
	void _Store::IsInitialized (bool check) const
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Store not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Store already initialized.");
			}
		}
	}
}
*/

/**
 * @}
 **/
