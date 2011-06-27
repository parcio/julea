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

/**
 * \file
 **/

#include <glib.h>

#include "jcollection.h"
#include "jcollection-internal.h"

#include "jbson.h"
#include "jbson-iterator.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jitem.h"
#include "jitem-internal.h"
#include "jitem-status.h"
#include "jitem-status-internal.h"
#include "jlist.h"
#include "jlist-iterator.h"
#include "jmongo.h"
#include "jmongo-connection.h"
#include "jobjectid.h"
#include "joperation.h"
#include "joperation-internal.h"
#include "jsemantics.h"
#include "jstore.h"
#include "jstore-internal.h"

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
	JObjectID* id;

	/**
	 * The collection's name.
	 **/
	gchar* name;

	struct
	{
		gchar* items;
		gchar* item_statuses;
	}
	collection;

	/**
	 * Pointer to the used semantics.
	 **/
	JSemantics* semantics;

	/**
	 * Pointer to the parent store.
	 **/
	JStore* store;

	/**
	 * The reference count.
	 **/
	guint ref_count;
};

/**
 * Creates a new JCollection.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 * c = j_collection_new("JULEA");
 * \endcode
 *
 * \param name The collection's name.
 *
 * \return A new JCollection.
 **/
JCollection*
j_collection_new (JStore* store, const gchar* name)
{
	JCollection* collection;

	g_return_val_if_fail(store != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	/*
	: m_initialized(false),
	*/

	collection = g_slice_new(JCollection);
	collection->id = j_object_id_new(TRUE);
	collection->name = g_strdup(name);
	collection->collection.items = NULL;
	collection->semantics = NULL;
	collection->store = j_store_ref(store);
	collection->ref_count = 1;

	return collection;
}

/**
 * Increases the collection's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 * j_collection_ref(c);
 * \endcode
 *
 * \param collection The JCollection.
 *
 * \return #collection.
 **/
JCollection*
j_collection_ref (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	collection->ref_count++;

	return collection;
}

void
j_collection_unref (JCollection* collection)
{
	g_return_if_fail(collection != NULL);

	collection->ref_count--;

	if (collection->ref_count == 0)
	{
		if (collection->semantics != NULL)
		{
			j_semantics_unref(collection->semantics);
		}

		if (collection->store != NULL)
		{
			j_store_unref(collection->store);
		}

		if (collection->id != NULL)
		{
			j_object_id_free(collection->id);
		}

		g_free(collection->collection.items);
		g_free(collection->name);

		g_slice_free(JCollection, collection);
	}
}

const gchar*
j_collection_name (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	return collection->name;
}

/**
 * Gets the statuses of the given items.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection The collection.
 * \param names      A list of items.
 * \param flags      Item status flags.
 **/
void
j_collection_get_status (JCollection* collection, JList* items, JItemStatusFlags flags)
{
	JBSON* empty;
	JBSON* fields;
	JBSON* bson;
	JMongoConnection* connection;
	JMongoIterator* iterator;
	guint length;
	guint n = 0;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(items != NULL);

	/*
		IsInitialized(true);
	*/

	if (flags == J_ITEM_STATUS_NONE)
	{
		JItemStatus* status;
		JListIterator* it;

		status = j_item_status_new(flags);
		it = j_list_iterator_new(items);

		while (j_list_iterator_next(it))
		{
			JItem* item = j_list_iterator_get(it);

			j_item_set_status(item, j_item_status_ref(status));
		}

		j_list_iterator_free(it);

		j_item_status_unref(status);

		return;
	}

	bson = j_bson_new();
	length = j_list_length(items);

	if (length == 1)
	{
		JItem* item = j_list_get(items, 0);

		j_bson_append_object_id(bson, "Item", j_item_id(item));
		n = 1;
	}
	else if (length > 1)
	{
		JBSON* items_bson;
		JListIterator* it;

		items_bson = j_bson_new();
		it = j_list_iterator_new(items);

		while (j_list_iterator_next(it))
		{
			JItem* item = j_list_iterator_get(it);

			j_bson_append_object_id(items_bson, "Item", j_item_id(item));
		}

		j_list_iterator_free(it);

		j_bson_append_document(bson, "$or", items_bson);
		j_bson_unref(items_bson);
	}

	empty = j_bson_new_empty();
	fields = j_bson_new();

	if (flags & J_ITEM_STATUS_SIZE)
	{
		j_bson_append_int32(fields, "Size", 1);
	}

	if (flags & J_ITEM_STATUS_ACCESS_TIME)
	{
		j_bson_append_int32(fields, "AccessTime", 1);
	}

	if (flags & J_ITEM_STATUS_MODIFICATION_TIME)
	{
		j_bson_append_int32(fields, "ModificationTime", 1);
	}

	connection = j_connection_connection(j_store_connection(collection->store));
	iterator = j_mongo_find(connection, j_collection_collection_item_statuses(collection), bson, fields, n, 0);

	while (j_mongo_iterator_next(iterator))
	{
		JBSON* status_bson;
		JItemStatus* status;

		status_bson = j_mongo_iterator_get(iterator);
		status = j_item_status_new(flags);
		j_item_status_deserialize(status, status_bson);
		// FIXME j_item_set_status(item, status);
		j_bson_unref(status_bson);
	}

	j_mongo_iterator_free(iterator);

	j_bson_unref(empty);
	j_bson_unref(fields);
	j_bson_unref(bson);
}

JSemantics*
j_collection_semantics (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	if (collection->semantics != NULL)
	{
		return collection->semantics;
	}

	return j_store_semantics(collection->store);
}

void
j_collection_set_semantics (JCollection* collection, JSemantics* semantics)
{
	g_return_if_fail(collection != NULL);
	g_return_if_fail(semantics != NULL);

	if (collection->semantics != NULL)
	{
		j_semantics_unref(collection->semantics);
	}

	collection->semantics = j_semantics_ref(semantics);
}

void
j_collection_create (JCollection* collection, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(collection != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_COLLECTION_CREATE;
	part->u.collection_create.collection = j_collection_ref(collection);

	j_operation_add(operation, part);
}

void
j_collection_get (JCollection* collection, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(collection != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_COLLECTION_GET;
	part->u.collection_get.collection = j_collection_ref(collection);

	j_operation_add(operation, part);
}

/* Internal */

JCollection*
j_collection_new_from_bson (JStore* store, JBSON* bson)
{
	/*
		: m_initialized(true),
	*/
	JCollection* collection;

	g_return_val_if_fail(store != NULL, NULL);
	g_return_val_if_fail(bson != NULL, NULL);

	collection = g_slice_new(JCollection);
	collection->id = NULL;
	collection->name = NULL;
	collection->collection.items = NULL;
	collection->semantics = NULL;
	collection->store = j_store_ref(store);
	collection->ref_count = 1;

	j_collection_deserialize(collection, bson);

	return collection;
}

const gchar*
j_collection_collection_items (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(collection->store != NULL, NULL);

	if (G_UNLIKELY(collection->collection.items == NULL))
	{
		collection->collection.items = g_strdup_printf("%s.Items", j_store_name(collection->store));
	}

	return collection->collection.items;
}

const gchar*
j_collection_collection_item_statuses (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(collection->store != NULL, NULL);

	if (G_UNLIKELY(collection->collection.item_statuses == NULL))
	{
		collection->collection.item_statuses = g_strdup_printf("%s.ItemStatuses", j_store_name(collection->store));
	}

	return collection->collection.item_statuses;
}

JStore*
j_collection_store (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	return collection->store;
}

JBSON*
j_collection_serialize (JCollection* collection)
{
	/*
			.append("User", m_owner.User())
			.append("Group", m_owner.Group())
	*/

	JBSON* bson;

	g_return_val_if_fail(collection != NULL, NULL);

	bson = j_bson_new();
	j_bson_append_object_id(bson, "_id", collection->id);
	j_bson_append_string(bson, "Name", collection->name);

	return bson;
}

/**
 * \private
 **/
void
j_collection_deserialize (JCollection* collection, JBSON* bson)
{
	JBSONIterator* iterator;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(bson != NULL);

	//j_bson_print(bson);

	iterator = j_bson_iterator_new(bson);

	/*
		m_owner.m_user = o.getField("User").Int();
		m_owner.m_group = o.getField("Group").Int();
	*/

	while (j_bson_iterator_next(iterator))
	{
		const gchar* key;

		key = j_bson_iterator_get_key(iterator);

		if (g_strcmp0(key, "_id") == 0)
		{
			if (collection->id != NULL)
			{
				j_object_id_free(collection->id);
			}

			collection->id = j_bson_iterator_get_id(iterator);
		}
		else if (g_strcmp0(key, "Name") == 0)
		{
			g_free(collection->name);
			collection->name = g_strdup(j_bson_iterator_get_string(iterator));
		}
	}

	j_bson_iterator_free(iterator);
}

JObjectID*
j_collection_id (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	return collection->id;
}

void
j_collection_create_internal (JList* parts)
{
	JBSON* index;
	JList* obj;
	JListIterator* it;
	JMongoConnection* connection;
	JSemantics* semantics;
	JStore* store;

	g_return_if_fail(parts != NULL);

	/*
	IsInitialized(true);
	*/

	obj = j_list_new((JListFreeFunc)j_bson_unref);
	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JCollection* collection = part->u.collection_create.collection;
		JBSON* bson;

		store = collection->store;
		bson = j_collection_serialize(collection);

		j_list_append(obj, bson);
	}

	j_list_iterator_free(it);

	connection = j_connection_connection(j_store_connection(store));

	index = j_bson_new();
	j_bson_append_int32(index, "Name", 1);

	j_mongo_create_index(connection, j_store_collection_collections(store), index, TRUE);
	j_mongo_insert_list(connection, j_store_collection_collections(store), obj);

	j_bson_unref(index);
	j_list_unref(obj);

	/*
	{
		bson oerr;

		mongo_cmd_get_last_error(mc, store->name, &oerr);
		bson_print(&oerr);
		bson_destroy(&oerr);
	}
	*/

	semantics = j_store_semantics(store);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_STRICT)
	{
		j_mongo_command_int(connection, "admin", "fsync", 1);
	}
}

void
j_collection_get_internal (JList* parts)
{
	JBSON* empty;
	JBSON* bson;
	JMongoConnection* connection;
	JMongoIterator* iterator;
	JStore* store;
	guint length;
	guint n = 0;

	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	bson = j_bson_new();
	length = j_list_length(parts);

	if (length == 1)
	{
		JOperationPart* part = j_list_get(parts, 0);
		JCollection* collection = part->u.collection_get.collection;

		store = collection->store;

		j_bson_append_string(bson, "Name", collection->name);
		n = 1;
	}
	else if (length > 1)
	{
		JBSON* names_bson;
		JListIterator* it;

		names_bson = j_bson_new();
		it = j_list_iterator_new(parts);

		while (j_list_iterator_next(it))
		{
			JOperationPart* part = j_list_iterator_get(it);
			JCollection* collection = part->u.collection_get.collection;

			store = collection->store;

			j_bson_append_string(names_bson, "Name", collection->name);
		}

		j_list_iterator_free(it);

		j_bson_append_document(bson, "$or", names_bson);
		j_bson_unref(names_bson);
	}

	empty = j_bson_new_empty();

	connection = j_connection_connection(j_store_connection(store));
	iterator = j_mongo_find(connection, j_store_collection_collections(store), bson, NULL, n, 0);

	while (j_mongo_iterator_next(iterator))
	{
		JBSON* collection_bson;

		collection_bson = j_mongo_iterator_get(iterator);
		//j_list_append(collections, j_collection_new_from_bson(store, collection_bson));
		j_bson_unref(collection_bson);
	}

	j_mongo_iterator_free(iterator);

	j_bson_unref(empty);
	j_bson_unref(bson);
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

	mongo::OID const& _Collection::ID () const
	{
		return m_id;
	}
}
*/

/**
 * @}
 **/
