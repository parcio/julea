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

#include <jstore.h>
#include <jstore-internal.h>

#include <jcollection.h>
#include <jcollection-internal.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation.h>
#include <joperation-internal.h>
#include <joperation-part-internal.h>
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
	gchar* name;

	struct
	{
		gchar* collections;
	}
	collection;

	JConnection* connection;

	gint ref_count;
};

/**
 * Creates a new JStore.
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
	store->connection = NULL;
	store->ref_count = 1;

	return store;
}

JStore*
j_store_ref (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	g_atomic_int_inc(&(store->ref_count));

	return store;
}

void
j_store_unref (JStore* store)
{
	g_return_if_fail(store != NULL);

	if (g_atomic_int_dec_and_test(&(store->ref_count)))
	{
		j_connection_unref(store->connection);

		g_free(store->collection.collections);
		g_free(store->name);

		g_slice_free(JStore, store);
	}
}

gchar const*
j_store_get_name (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	return store->name;
}

JConnection*
j_store_get_connection (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	return store->connection;
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
 * \param operation  An operation.
 **/
void
j_store_create_collection (JStore* store, JCollection* collection, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collection != NULL);

	part = j_operation_part_new(J_OPERATION_STORE_CREATE_COLLECTION);
	part->key = store;
	part->u.store_create_collection.store = j_store_ref(store);
	part->u.store_create_collection.collection = j_collection_ref(collection);

	j_operation_add(operation, part);
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
 * \param collection A collection.
 * \param name       A name.
 * \param operation  An operation.
 **/
void
j_store_get_collection (JStore* store, JCollection** collection, gchar const* name, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(name != NULL);

	part = j_operation_part_new(J_OPERATION_STORE_GET_COLLECTION);
	part->key = store;
	part->u.store_get_collection.store = j_store_ref(store);
	part->u.store_get_collection.collection = collection;
	part->u.store_get_collection.name = g_strdup(name);

	j_operation_add(operation, part);
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
 * \param operation  An operation.
 **/
void
j_store_delete_collection (JStore* store, JCollection* collection, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collection != NULL);

	part = j_operation_part_new(J_OPERATION_STORE_DELETE_COLLECTION);
	part->key = store;
	part->u.store_delete_collection.store = j_store_ref(store);
	part->u.store_delete_collection.collection = j_collection_ref(collection);

	j_operation_add(operation, part);
}

/* Internal */

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

void
j_store_set_connection (JStore* store, JConnection* connection)
{
	g_return_if_fail(store != NULL);
	g_return_if_fail(connection != NULL);
	g_return_if_fail(store->connection == NULL);

	store->connection = j_connection_ref(connection);
}

void
j_store_create_collection_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;
	JSemantics* semantics;
	JStore* store;
	bson** obj;
	bson index;
	mongo* connection;
	guint i;
	guint length;

	g_return_if_fail(operation != NULL);
	g_return_if_fail(parts != NULL);

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
		JCollection* collection = part->u.store_create_collection.collection;
		bson* b;

		store = part->u.store_create_collection.store;
		j_collection_set_store(collection, store);
		b = j_collection_serialize(collection);

		obj[i] = b;
		i++;
	}

	j_list_iterator_free(it);

	connection = j_connection_get_connection(j_store_get_connection(store));

	bson_init(&index);
	bson_append_int(&index, "Name", 1);
	bson_finish(&index);

	mongo_create_index(connection, j_store_collection_collections(store), &index, MONGO_INDEX_UNIQUE, NULL);
	mongo_insert_batch(connection, j_store_collection_collections(store), obj, length);

	bson_destroy(&index);

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

	semantics = j_operation_get_semantics(operation);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_STRICT)
	{
		mongo_simple_int_command(connection, "admin", "fsync", 1, NULL);
	}
}

void
j_store_delete_collection_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_if_fail(operation != NULL);
	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	/* FIXME do some optimizations for len(parts) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JCollection* collection = part->u.store_delete_collection.collection;
		JStore* store = part->u.store_delete_collection.store;
		bson b;
		mongo* connection;

		bson_init(&b);
		bson_append_oid(&b, "_id", j_collection_get_id(collection));
		bson_finish(&b);

		connection = j_connection_get_connection(j_store_get_connection(store));
		mongo_remove(connection, j_store_collection_collections(store), &b);

		bson_destroy(&b);
	}

	j_list_iterator_free(it);
}

void
j_store_get_collection_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_if_fail(operation != NULL);
	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	/* FIXME do some optimizations for len(parts) > 1 */
	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JCollection** collection = part->u.store_get_collection.collection;
		JStore* store = part->u.store_get_collection.store;
		bson b;
		mongo* connection;
		mongo_cursor* cursor;
		gchar const* name = part->u.store_get_collection.name;

		bson_init(&b);
		bson_append_string(&b, "Name", name);
		bson_finish(&b);

		connection = j_connection_get_connection(j_store_get_connection(store));
		cursor = mongo_find(connection, j_store_collection_collections(store), &b, NULL, 1, 0, 0);

		bson_destroy(&b);

		*collection = NULL;

		while (mongo_cursor_next(cursor) == MONGO_OK)
		{
			*collection = j_collection_new_from_bson(store, mongo_cursor_bson(cursor));
		}

		mongo_cursor_destroy(cursor);
	}

	j_list_iterator_free(it);
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
