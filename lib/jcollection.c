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
#include "jlist.h"
#include "jlist-iterator.h"
#include "jmongo.h"
#include "jmongo-connection.h"
#include "jobjectid.h"
#include "jsemantics.h"
#include "jstore.h"

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
j_collection_new (const gchar* name)
{
	JCollection* collection;

	g_return_val_if_fail(name != NULL, NULL);

	/*
	: m_initialized(false),
	*/

	collection = g_slice_new(JCollection);
	collection->id = j_object_id_new(TRUE);
	collection->name = g_strdup(name);
	collection->collection.items = NULL;
	collection->semantics = NULL;
	collection->store = NULL;
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

void
j_collection_create (JCollection* collection, JList* items)
{
	JBSON* index;
	JList* obj;
	JListIterator* it;
	JSemantics* semantics;
	JMongoConnection* connection;
	guint length;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(items != NULL);

	/*
	IsInitialized(true);
	*/

	length = j_list_length(items);

	if (length == 0)
	{
		return;
	}

	obj = j_list_new((JListFreeFunc)j_bson_unref);
	it = j_list_iterator_new(items);

	while (j_list_iterator_next(it))
	{
		JItem* item = j_list_iterator_get(it);
		JBSON* bson;

		j_item_associate(item, collection);
		bson = j_item_serialize(item);

		j_list_append(obj, bson);
	}

	j_list_iterator_free(it);

	connection = j_connection_connection(j_store_connection(collection->store));

	index = j_bson_new();
	j_bson_append_int32(index, "Collection", 1);
	j_bson_append_int32(index, "Name", 1);

	j_mongo_create_index(connection, j_collection_collection_items(collection), index, TRUE);
	j_mongo_insert_list(connection, j_collection_collection_items(collection), obj);

	j_bson_unref(index);
	j_list_unref(obj);

	semantics = j_collection_semantics(collection);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_STRICT)
	{
		j_mongo_command_int(connection, "admin", "fsync", 1);
	}
}

JList*
j_collection_get (JCollection* collection, JList* names)
{
	JBSON* empty;
	JBSON* bson;
	JMongoConnection* connection;
	JMongoIterator* iterator;
	//mongo_cursor* cursor;
	JList* items;
	guint length;
	guint n = 0;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(names != NULL, NULL);

	/*
		IsInitialized(true);
	*/

	bson = j_bson_new();
	length = j_list_length(names);

	j_bson_append_object_id(bson, "Collection", collection->id);

	if (length == 1)
	{
		const gchar* name = j_list_get(names, 0);

		j_bson_append_string(bson, "Name", name);
		n = 1;
	}
	else if (length > 1)
	{
		JBSON* names_bson;
		JListIterator* it;

		names_bson = j_bson_new();
		it = j_list_iterator_new(names);

		while (j_list_iterator_next(it))
		{
			const gchar* name = j_list_iterator_get(it);

			j_bson_append_string(names_bson, "Name", name);
		}

		j_list_iterator_free(it);

		j_bson_append_document(bson, "$or", names_bson);
		j_bson_unref(names_bson);
	}


	empty = j_bson_new_empty();

	connection = j_connection_connection(j_store_connection(collection->store));
	iterator = j_mongo_find(connection, j_collection_collection_items(collection), bson, NULL, n, 0);

	items = j_list_new((JListFreeFunc)j_item_unref);

	while (j_mongo_iterator_next(iterator))
	{
		JBSON* item_bson;

		item_bson = j_mongo_iterator_get(iterator);
		j_list_append(items, j_item_new_from_bson(collection, item_bson));
		j_bson_unref(item_bson);
	}

	j_mongo_iterator_free(iterator);

	j_bson_unref(empty);
	j_bson_unref(bson);

	return items;
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

JStore*
j_collection_store (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	return collection->store;
}

void
j_collection_associate (JCollection* collection, JStore* store)
{
	g_return_if_fail(collection != NULL);
	g_return_if_fail(store != NULL);

	/*
		IsInitialized(false);

		m_initialized = true;
	*/

	collection->store = j_store_ref(store);
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
