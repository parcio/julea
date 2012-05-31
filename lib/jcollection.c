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

/**
 * \file
 **/

#include <glib.h>

#include <bson.h>
#include <mongo.h>

#include <jcollection.h>
#include <jcollection-internal.h>

#include <jcommon-internal.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jitem.h>
#include <jitem-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation.h>
#include <joperation-internal.h>
#include <joperation-part-internal.h>
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

	struct
	{
		gchar* items;
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	/*
	: m_initialized(false),
	*/

	collection = g_slice_new(JCollection);
	bson_oid_gen(&(collection->id));
	collection->name = g_strdup(name);
	collection->collection.items = NULL;
	collection->store = NULL;
	collection->ref_count = 1;

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	g_atomic_int_inc(&(collection->ref_count));

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(collection->ref_count)))
	{
		if (collection->store != NULL)
		{
			j_store_unref(collection->store);
		}

		g_free(collection->collection.items);
		g_free(collection->name);

		g_slice_free(JCollection, collection);
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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
 * \param operation  An operation.
 **/
void
j_collection_create_item (JCollection* collection, JItem* item, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_operation_part_new(J_OPERATION_COLLECTION_CREATE_ITEM);
	part->key = collection;
	part->u.collection_create_item.collection = j_collection_ref(collection);
	part->u.collection_create_item.item = j_item_ref(item);

	j_operation_add(operation, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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
 * \param operation  An operation.
 **/
void
j_collection_get_item (JCollection* collection, JItem** item, gchar const* name, JItemStatusFlags flags, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);
	g_return_if_fail(name != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_operation_part_new(J_OPERATION_COLLECTION_GET_ITEM);
	part->key = collection;
	part->u.collection_get_item.collection = j_collection_ref(collection);
	part->u.collection_get_item.item = item;
	part->u.collection_get_item.name = g_strdup(name);
	part->u.collection_get_item.flags = flags;

	j_operation_add(operation, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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
 * \param operation  An operation.
 **/
void
j_collection_delete_item (JCollection* collection, JItem* item, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_operation_part_new(J_OPERATION_COLLECTION_DELETE_ITEM);
	part->key = collection;
	part->u.collection_delete_item.collection = j_collection_ref(collection);
	part->u.collection_delete_item.item = j_item_ref(item);

	j_operation_add(operation, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	collection = g_slice_new(JCollection);
	collection->name = NULL;
	collection->collection.items = NULL;
	collection->store = j_store_ref(store);
	collection->ref_count = 1;

	j_collection_deserialize(collection, b);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (G_UNLIKELY(collection->collection.items == NULL))
	{
		collection->collection.items = g_strdup_printf("%s.Items", j_store_get_name(collection->store));
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return collection->collection.items;
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	b = g_slice_new(bson);
	bson_init(b);
	bson_append_oid(b, "_id", &(collection->id));
	bson_append_string(b, "Name", collection->name);
	bson_finish(b);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

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
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return &(collection->id);
}

void
j_collection_set_store (JCollection* collection, JStore* store)
{
	g_return_if_fail(collection != NULL);
	g_return_if_fail(store != NULL);
	g_return_if_fail(collection->store == NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	collection->store = j_store_ref(store);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

gboolean
j_collection_create_item_internal (JOperation* operation, JList* parts)
{
	JCollection* collection = NULL;
	JListIterator* it;
	JSemantics* semantics;
	bson** obj;
	bson index;
	mongo* connection;
	mongo_write_concern write_concern[1];
	guint i;
	guint length;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	/*
	IsInitialized(true);
	*/

	i = 0;
	length = j_list_length(parts);
	obj = g_new(bson*, length);
	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JItem* item = part->u.collection_create_item.item;
		bson* b;

		collection = part->u.collection_create_item.collection;
		j_item_set_collection(item, collection);
		b = j_item_serialize(item);

		obj[i] = b;
		i++;
	}

	j_list_iterator_free(it);

	if (collection == NULL)
	{
		goto end;
	}

	connection = j_connection_get_connection(j_store_get_connection(j_collection_get_store(collection)));
	semantics = j_operation_get_semantics(operation);

	mongo_write_concern_init(write_concern);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_IMMEDIATE)
	{
		write_concern->j = 1;
		write_concern->w = 1;
	}

	mongo_write_concern_finish(write_concern);

	bson_init(&index);
	bson_append_int(&index, "Collection", 1);
	bson_append_int(&index, "Name", 1);
	bson_finish(&index);

	mongo_create_index(connection, j_collection_collection_items(collection), &index, MONGO_INDEX_UNIQUE, NULL);
	/* FIXME MONGO_CONTINUE_ON_ERROR */
	mongo_insert_batch(connection, j_collection_collection_items(collection), (bson const**)obj, length, write_concern, 0);

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
	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_IMMEDIATE)
	{
		mongo_simple_int_command(connection, "admin", "fsync", 1, NULL);
	}
	*/

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return TRUE;
}

gboolean
j_collection_delete_item_internal (JOperation* operation, JList* parts)
{
	JCollection* collection = NULL;
	JListIterator* it;
	JSemantics* semantics;
	mongo* connection;
	mongo_write_concern write_concern[1];

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	/*
		IsInitialized(true);
	*/

	semantics = j_operation_get_semantics(operation);

	mongo_write_concern_init(write_concern);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_IMMEDIATE)
	{
		write_concern->j = 1;
		write_concern->w = 1;
	}

	mongo_write_concern_finish(write_concern);

	it = j_list_iterator_new(parts);

	/* FIXME do some optimizations for len(parts) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JItem* item = part->u.collection_delete_item.item;
		bson b;

		collection = part->u.collection_delete_item.collection;

		bson_init(&b);
		bson_append_oid(&b, "_id", j_item_get_id(item));
		bson_finish(&b);

		connection = j_connection_get_connection(j_store_get_connection(j_collection_get_store(collection)));
		/* FIXME do not send write_concern on each remove */
		mongo_remove(connection, j_collection_collection_items(collection), &b, write_concern);

		bson_destroy(&b);
	}

	j_list_iterator_free(it);

	mongo_write_concern_destroy(write_concern);

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_IMMEDIATE)
	{
		mongo_simple_int_command(connection, "admin", "fsync", 1, NULL);
	}
	*/

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return TRUE;
}

gboolean
j_collection_get_item_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	/* FIXME do some optimizations for len(parts) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JCollection* collection = part->u.collection_get_item.collection;
		JItem** item = part->u.collection_get_item.item;
		JItemStatusFlags flags = part->u.collection_get_item.flags;
		bson b;
		bson fields;
		mongo* connection;
		mongo_cursor* cursor;
		gchar const* name = part->u.collection_get_item.name;

		bson_init(&fields);

		bson_append_int(&fields, "_id", 1);
		bson_append_int(&fields, "Name", 1);

		if (flags & J_ITEM_STATUS_SIZE)
		{
			bson_append_int(&fields, "Size", 1);
		}

		if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
		{
			bson_append_int(&fields, "ModificationTime", 1);
		}

		bson_finish(&fields);

		bson_init(&b);
		bson_append_oid(&b, "Collection", &(collection->id));
		bson_append_string(&b, "Name", name);
		bson_finish(&b);

		connection = j_connection_get_connection(j_store_get_connection(j_collection_get_store(collection)));
		cursor = mongo_find(connection, j_collection_collection_items(collection), &b, &fields, 1, 0, 0);

		bson_destroy(&fields);
		bson_destroy(&b);

		*item = NULL;

		while (mongo_cursor_next(cursor) == MONGO_OK)
		{
			*item = j_item_new_from_bson(collection, mongo_cursor_bson(cursor));
		}

		mongo_cursor_destroy(cursor);
	}

	j_list_iterator_free(it);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return TRUE;
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
