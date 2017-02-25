/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
		gchar* items;
		gchar* locks;
	}
	collection;

	/**
	 * Whether the index has been created.
	 **/
	struct
	{
		gboolean collections;
		gboolean items;
		gboolean locks;
	}
	index;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

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
		g_free(store->collection.items);
		g_free(store->collection.locks);
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
JCollection*
j_store_create_collection (JStore* store, gchar const* name, JBatch* batch)
{
	JCollection* collection;
	JOperation* operation;

	g_return_val_if_fail(store != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	if ((collection = j_collection_new(store, name)) == NULL)
	{
		goto end;
	}

	operation = j_operation_new(J_OPERATION_STORE_CREATE_COLLECTION);
	operation->key = store;
	operation->u.store_create_collection.store = j_store_ref(store);
	operation->u.store_create_collection.collection = j_collection_ref(collection);

	j_batch_add(batch, operation);

end:
	return collection;
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
	JStore* store = NULL;
	/*
	: m_initialized(true),
	*/

	g_return_val_if_fail(name != NULL, NULL);

	if (strpbrk(name, "./") != NULL)
	{
		goto end;
	}

	store = g_slice_new(JStore);
	store->name = g_strdup(name);
	store->collection.collections = NULL;
	store->collection.items = NULL;
	store->collection.locks = NULL;
	store->index.collections = FALSE;
	store->index.items = FALSE;
	store->index.locks = FALSE;
	store->ref_count = 1;

end:
	return store;
}

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
j_store_collection (JStore* store, JStoreCollection collection)
{
	gchar const* m_collection = NULL;

	g_return_val_if_fail(store != NULL, NULL);

	/*
		IsInitialized(true);
	*/

	j_trace_enter(G_STRFUNC);

	switch (collection)
	{
		case J_STORE_COLLECTION_COLLECTIONS:
			if (G_UNLIKELY(store->collection.collections == NULL))
			{
				store->collection.collections = g_strdup_printf("%s.Collections", store->name);
			}

			m_collection = store->collection.collections;
			break;
		case J_STORE_COLLECTION_ITEMS:
			if (G_UNLIKELY(store->collection.items == NULL))
			{
				store->collection.items = g_strdup_printf("%s.Items", store->name);
			}

			m_collection = store->collection.items;
			break;
		case J_STORE_COLLECTION_LOCKS:
			if (G_UNLIKELY(store->collection.locks == NULL))
			{
				store->collection.locks = g_strdup_printf("%s.Locks", store->name);
			}

			m_collection = store->collection.locks;
			break;
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);

	return m_collection;
}

void
j_store_create_index (JStore* store, JStoreCollection collection, mongoc_client_t* connection, bson_t const* index)
{
	g_return_if_fail(store != NULL);
	g_return_if_fail(connection != NULL);
	g_return_if_fail(index != NULL);

	j_trace_enter(G_STRFUNC);

	switch (collection)
	{
		case J_STORE_COLLECTION_COLLECTIONS:
			if (G_UNLIKELY(!store->index.collections))
			{
				mongoc_collection_t* m_collection;
				mongoc_index_opt_t m_index_opt[1];

				mongoc_index_opt_init(m_index_opt);
				m_index_opt->unique = TRUE;

				/* FIXME */
				m_collection = mongoc_client_get_collection(connection, store->name, "Collections");
				mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);
				store->index.collections = TRUE;
			}
			break;
		case J_STORE_COLLECTION_ITEMS:
			if (G_UNLIKELY(!store->index.items))
			{
				mongoc_collection_t* m_collection;
				mongoc_index_opt_t m_index_opt[1];

				mongoc_index_opt_init(m_index_opt);
				m_index_opt->unique = TRUE;

				/* FIXME */
				m_collection = mongoc_client_get_collection(connection, store->name, "Items");
				mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);
				store->index.items = TRUE;
			}
			break;
		case J_STORE_COLLECTION_LOCKS:
			if (G_UNLIKELY(!store->index.locks))
			{
				mongoc_collection_t* m_collection;
				mongoc_index_opt_t m_index_opt[1];

				mongoc_index_opt_init(m_index_opt);
				m_index_opt->unique = TRUE;

				/* FIXME */
				m_collection = mongoc_client_get_collection(connection, store->name, "Locks");
				mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);
				store->index.locks = TRUE;
			}
			break;
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);
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
	JListIterator* it;
	JStore* store = NULL;
	bson_t** obj;
	bson_t index[1];
	mongoc_client_t* mongo_connection;
	mongoc_collection_t* mongo_collection;
	mongoc_write_concern_t* write_concern;
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
	obj = g_new(bson_t*, length);
	it = j_list_iterator_new(operations);

	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection* collection = operation->u.store_create_collection.collection;
		bson_t* b;

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

	write_concern = mongoc_write_concern_new();
	j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	bson_init(index);
	bson_append_int32(index, "Name", -1, 1);
	//bson_finish(index);

	mongo_connection = j_connection_pool_pop_meta(0);

	j_store_create_index(store, J_STORE_COLLECTION_COLLECTIONS, mongo_connection, index);
	/* FIXME */
	mongo_collection = mongoc_client_get_collection(mongo_connection, store->name, "Collections");
	ret = j_helper_insert_batch(mongo_collection, obj, length, write_concern);

	/*
	if (!ret)
	{
		bson_t error[1];

		mongo_cmd_get_last_error(mongo_connection, store->name, error);
		bson_print(error);
		bson_destroy(error);
	}
	*/

	j_connection_pool_push_meta(0, mongo_connection);

	bson_destroy(index);

	mongoc_write_concern_destroy(write_concern);

end:
	for (i = 0; i < length; i++)
	{
		bson_destroy(obj[i]);
		g_slice_free(bson_t, obj[i]);
	}

	g_free(obj);

	/*
	{
		bson_t oerr;

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
	JListIterator* it;
	JStore* store = NULL;
	mongoc_client_t* mongo_connection;
	mongoc_write_concern_t* write_concern;
	gboolean ret = TRUE;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	write_concern = mongoc_write_concern_new();
	j_helper_set_write_concern(write_concern, j_batch_get_semantics(batch));

	it = j_list_iterator_new(operations);
	mongo_connection = j_connection_pool_pop_meta(0);

	/* FIXME do some optimizations for len(operations) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection* collection = operation->u.store_delete_collection.collection;
		bson_t b;
		mongoc_collection_t* m_collection;

		store = operation->u.store_delete_collection.store;

		bson_init(&b);
		bson_append_oid(&b, "_id", -1, j_collection_get_id(collection));
		//bson_finish(&b);

		/* FIXME */
		m_collection = mongoc_client_get_collection(mongo_connection, store->name, "Collections");
		/* FIXME do not send write_concern on each remove */
		ret = mongoc_collection_remove(m_collection, MONGOC_DELETE_SINGLE_REMOVE, &b, write_concern, NULL)&& ret;

		bson_destroy(&b);
	}

	j_connection_pool_push_meta(0, mongo_connection);
	j_list_iterator_free(it);

	mongoc_write_concern_destroy(write_concern);

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
	JListIterator* it;
	mongoc_client_t* mongo_connection;
	gboolean ret = TRUE;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(operations);
	mongo_connection = j_connection_pool_pop_meta(0);

	/* FIXME do some optimizations for len(operations) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection** collection = operation->u.store_get_collection.collection;
		JStore* store = operation->u.store_get_collection.store;
		bson_t b;
		bson_t const* b_cur;
		bson_t opts;
		mongoc_collection_t* m_collection;
		mongoc_cursor_t* cursor;
		gchar const* name = operation->u.store_get_collection.name;

		bson_init(&b);
		bson_append_utf8(&b, "Name", -1, name, -1);
		//bson_finish(&b);

		bson_init(&opts);
		bson_append_int32(&opts, "limit", -1, 1);

		/* FIXME */
		m_collection = mongoc_client_get_collection(mongo_connection, store->name, "Collections");
		cursor = mongoc_collection_find_with_opts(m_collection, &b, &opts, NULL);

		bson_destroy(&opts);
		bson_destroy(&b);

		*collection = NULL;

		// FIXME ret
		while (mongoc_cursor_next(cursor, &b_cur))
		{
			*collection = j_collection_new_from_bson(store, b_cur);
		}

		mongoc_cursor_destroy(cursor);
	}

	j_connection_pool_push_meta(0, mongo_connection);
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
