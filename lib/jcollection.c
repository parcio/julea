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

#include <bson.h>
#include <mongo.h>

#include <jcollection.h>
#include <jcollection-internal.h>

#include <jcommon-internal.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jconnection-pool-internal.h>
#include <jcredentials-internal.h>
#include <jhelper-internal.h>
#include <jitem.h>
#include <jitem-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>
#include <jstore.h>
#include <jstore-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JCollection Collection
 *
 * Data structures and functions for managing collections.
 *
 * @{
 **/

/**
 * A collection of items.
 **/
struct JCollection
{
	/**
	 * The ID.
	 **/
	bson_oid_t id;

	/**
	 * The name.
	 **/
	gchar* name;

	JCredentials* credentials;

	struct
	{
		gchar* items;
		gchar* locks;
	}
	collection;

	/**
	 * The parent store.
	 **/
	JStore* store;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Creates a new collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 *
 * c = j_collection_new("JULEA");
 * \endcode
 *
 * \param name  A collection name.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_collection_new (gchar const* name)
{
	JCollection* collection;

	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	/*
	: m_initialized(false),
	*/

	collection = g_slice_new(JCollection);
	bson_oid_gen(&(collection->id));
	collection->name = g_strdup(name);
	collection->credentials = j_credentials_new();
	collection->collection.items = NULL;
	collection->collection.locks = NULL;
	collection->store = NULL;
	collection->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Increases a collection's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 *
 * j_collection_ref(c);
 * \endcode
 *
 * \param collection A collection.
 *
 * \return #collection.
 **/
JCollection*
j_collection_ref (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	g_atomic_int_inc(&(collection->ref_count));

	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Decreases a collection's reference count.
 * When the reference count reaches zero, frees the memory allocated for the collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 **/
void
j_collection_unref (JCollection* collection)
{
	g_return_if_fail(collection != NULL);

	j_trace_enter(G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(collection->ref_count)))
	{
		if (collection->store != NULL)
		{
			j_store_unref(collection->store);
		}

		j_credentials_unref(collection->credentials);

		g_free(collection->collection.items);
		g_free(collection->collection.locks);
		g_free(collection->name);

		g_slice_free(JCollection, collection);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Returns a collection's name.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return A collection name.
 **/
gchar const*
j_collection_get_name (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return collection->name;
}

/**
 * Creates an item in a collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param item       An item.
 * \param batch      A batch.
 **/
void
j_collection_create_item (JCollection* collection, JItem* item, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC);

	operation = j_operation_new(J_OPERATION_COLLECTION_CREATE_ITEM);
	operation->key = collection;
	operation->u.collection_create_item.collection = j_collection_ref(collection);
	operation->u.collection_create_item.item = j_item_ref(item);

	j_item_set_collection(item, collection);

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * Gets an item from a collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param item       A pointer to an item.
 * \param name       A name.
 * \param batch      A batch.
 **/
void
j_collection_get_item (JCollection* collection, JItem** item, gchar const* name, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);
	g_return_if_fail(name != NULL);

	j_trace_enter(G_STRFUNC);

	operation = j_operation_new(J_OPERATION_COLLECTION_GET_ITEM);
	operation->key = collection;
	operation->u.collection_get_item.collection = j_collection_ref(collection);
	operation->u.collection_get_item.item = item;
	operation->u.collection_get_item.name = g_strdup(name);

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * Deletes an item from a collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param item       An item.
 * \param batch      A batch.
 **/
void
j_collection_delete_item (JCollection* collection, JItem* item, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC);

	operation = j_operation_new(J_OPERATION_COLLECTION_DELETE_ITEM);
	operation->key = collection;
	operation->u.collection_delete_item.collection = j_collection_ref(collection);
	operation->u.collection_delete_item.item = j_item_ref(item);

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/* Internal */

/**
 * Creates a new collection from a BSON object.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store A store.
 * \param b     A BSON object.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_collection_new_from_bson (JStore* store, bson const* b)
{
	/*
		: m_initialized(true),
	*/
	JCollection* collection;

	g_return_val_if_fail(store != NULL, NULL);
	g_return_val_if_fail(b != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	collection = g_slice_new(JCollection);
	collection->name = NULL;
	collection->credentials = j_credentials_new();
	collection->collection.items = NULL;
	collection->collection.locks = NULL;
	collection->store = j_store_ref(store);
	collection->ref_count = 1;

	j_collection_deserialize(collection, b);

	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Returns the MongoDB collection used for items.
 *
 * \author Michael Kuhn
 *
 * \param collection A collection.
 *
 * \return The MongoDB collection.
 */
gchar const*
j_collection_collection_items (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(collection->store != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	if (G_UNLIKELY(collection->collection.items == NULL))
	{
		collection->collection.items = g_strdup_printf("%s.Items", j_store_get_name(collection->store));
	}

	j_trace_leave(G_STRFUNC);

	return collection->collection.items;
}

/**
 * Returns the MongoDB collection used for locks.
 *
 * \author Michael Kuhn
 *
 * \param collection A collection.
 *
 * \return The MongoDB collection.
 */
gchar const*
j_collection_collection_locks (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(collection->store != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	if (G_UNLIKELY(collection->collection.locks == NULL))
	{
		collection->collection.locks = g_strdup_printf("%s.Locks", j_store_get_name(collection->store));
	}

	j_trace_leave(G_STRFUNC);

	return collection->collection.locks;
}

/**
 * Returns a collection's store.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return A store.
 **/
JStore*
j_collection_get_store (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return collection->store;
}

/**
 * Serializes a collection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
bson*
j_collection_serialize (JCollection* collection)
{
	/*
			.append("User", m_owner.User())
			.append("Group", m_owner.Group())
	*/

	bson* b;
	bson* b_cred;

	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	b_cred = j_credentials_serialize(collection->credentials);

	b = g_slice_new(bson);
	bson_init(b);
	bson_append_oid(b, "_id", &(collection->id));
	bson_append_string(b, "Name", collection->name);
	bson_append_bson(b, "Credentials", b_cred);
	bson_finish(b);

	bson_destroy(b_cred);
	g_slice_free(bson, b_cred);

	j_trace_leave(G_STRFUNC);

	return b;
}

/**
 * Deserializes a collection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param b          A BSON object.
 **/
void
j_collection_deserialize (JCollection* collection, bson const* b)
{
	bson_iterator iterator;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC);

	//j_bson_print(bson);

	bson_iterator_init(&iterator, b);

	/*
		m_owner.m_user = o.getField("User").Int();
		m_owner.m_group = o.getField("Group").Int();
	*/

	while (bson_iterator_next(&iterator))
	{
		gchar const* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "_id") == 0)
		{
			collection->id = *bson_iterator_oid(&iterator);
		}
		else if (g_strcmp0(key, "Name") == 0)
		{
			g_free(collection->name);
			collection->name = g_strdup(bson_iterator_string(&iterator));
		}
		else if (g_strcmp0(key, "Credentials") == 0)
		{
			bson b_cred[1];

			bson_iterator_subobject(&iterator, b_cred);
			j_credentials_deserialize(collection->credentials, b_cred);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Returns a collection's ID.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return An ID.
 **/
bson_oid_t const*
j_collection_get_id (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return &(collection->id);
}

void
j_collection_set_store (JCollection* collection, JStore* store)
{
	g_return_if_fail(collection != NULL);
	g_return_if_fail(store != NULL);
	g_return_if_fail(collection->store == NULL);

	j_trace_enter(G_STRFUNC);

	collection->store = j_store_ref(store);

	j_trace_leave(G_STRFUNC);
}

gboolean
j_collection_create_item_internal (JBatch* batch, JList* operations)
{
	JCollection* collection = NULL;
	JConnection* connection;
	JListIterator* it;
	JSemantics* semantics;
	bson** obj;
	bson index;
	mongo* mongo_connection;
	mongo_write_concern write_concern[1];
	gboolean ret = FALSE;
	guint i;
	guint length;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	/*
	IsInitialized(true);
	*/

	semantics = j_batch_get_semantics(batch);

	i = 0;
	length = j_list_length(operations);
	obj = g_new(bson*, length);
	it = j_list_iterator_new(operations);

	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JItem* item = operation->u.collection_create_item.item;
		bson* b;

		collection = operation->u.collection_create_item.collection;
		b = j_item_serialize(item, semantics);

		obj[i] = b;
		i++;
	}

	j_list_iterator_free(it);

	if (collection == NULL)
	{
		goto end;
	}

	j_helper_set_write_concern(write_concern, semantics);

	bson_init(&index);
	bson_append_int(&index, "Collection", 1);
	bson_append_int(&index, "Name", 1);
	bson_finish(&index);

	connection = j_connection_pool_pop();
	mongo_connection = j_connection_get_connection(connection);

	mongo_create_index(mongo_connection, j_collection_collection_items(collection), &index, MONGO_INDEX_UNIQUE, NULL);
	ret = j_helper_insert_batch(mongo_connection, j_collection_collection_items(collection), obj, length, write_concern);

	/*
	if (ret != MONGO_OK)
	{
		bson error[1];

		mongo_cmd_get_last_error(mongo_connection, j_store_get_name(collection->store), error);
		bson_print(error);
		bson_destroy(error);
	}
	*/

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

	j_trace_leave(G_STRFUNC);

	return ret;
}

gboolean
j_collection_delete_item_internal (JBatch* batch, JList* operations)
{
	JCollection* collection = NULL;
	JConnection* connection;
	JListIterator* it;
	mongo* mongo_connection;
	mongo_write_concern write_concern[1];
	gboolean ret = TRUE;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

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
		JItem* item = operation->u.collection_delete_item.item;
		bson b;

		collection = operation->u.collection_delete_item.collection;

		bson_init(&b);
		bson_append_oid(&b, "_id", j_item_get_id(item));
		bson_finish(&b);

		/* FIXME do not send write_concern on each remove */
		ret = (mongo_remove(mongo_connection, j_collection_collection_items(collection), &b, write_concern) == MONGO_OK) && ret;

		bson_destroy(&b);

		/* FIXME also delete data */
	}

	j_connection_pool_push(connection);
	j_list_iterator_free(it);

	mongo_write_concern_destroy(write_concern);

	j_trace_leave(G_STRFUNC);

	return ret;
}

gboolean
j_collection_get_item_internal (JBatch* batch, JList* operations)
{
	JConnection* connection;
	JListIterator* it;
	mongo* mongo_connection;
	gint semantics_concurrency;
	gboolean ret = TRUE;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(operations);
	connection = j_connection_pool_pop();
	mongo_connection = j_connection_get_connection(connection);

	semantics_concurrency = j_semantics_get(j_batch_get_semantics(batch), J_SEMANTICS_CONCURRENCY);

	/* FIXME do some optimizations for len(operations) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JCollection* collection = operation->u.collection_get_item.collection;
		JItem** item = operation->u.collection_get_item.item;
		bson b;
		bson fields;
		mongo_cursor* cursor;
		gchar const* name = operation->u.collection_get_item.name;

		bson_init(&fields);
		bson_append_int(&fields, "_id", 1);
		bson_append_int(&fields, "Name", 1);

		if (semantics_concurrency == J_SEMANTICS_CONCURRENCY_NONE)
		{
			bson_append_int(&fields, "Status", 1);
		}

		bson_finish(&fields);

		bson_init(&b);
		bson_append_oid(&b, "Collection", &(collection->id));
		bson_append_string(&b, "Name", name);
		bson_finish(&b);

		cursor = mongo_find(mongo_connection, j_collection_collection_items(collection), &b, &fields, 1, 0, 0);

		bson_destroy(&fields);
		bson_destroy(&b);

		*item = NULL;

		// FIXME ret
		while (mongo_cursor_next(cursor) == MONGO_OK)
		{
			*item = j_item_new_from_bson(collection, mongo_cursor_bson(cursor));
		}

		mongo_cursor_destroy(cursor);
	}

	j_connection_pool_push(connection);
	j_list_iterator_free(it);

	j_trace_leave(G_STRFUNC);

	return ret;
}

/*
namespace JULEA
{
	void _Collection::IsInitialized (bool check) const
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Collection not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Collection already initialized.");
			}
		}
	}
}
*/

/**
 * @}
 **/
