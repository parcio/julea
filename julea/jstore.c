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

#include <bson.h>
#include <mongo.h>

#include "jstore.h"
#include "jstore-internal.h"

#include "jbson.h"
#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jlist.h"
#include "jlist-iterator.h"

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
	JSemantics* semantics;

	guint ref_count;
};

/**
 * Creates a new JStore.
 **/
JStore*
j_store_new (JConnection* connection, const gchar* name)
{
	JStore* store;
	/*
	: m_initialized(true),
	m_collectionsCollection("")
	*/

	g_return_val_if_fail(connection != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	store = g_slice_new(JStore);
	store->name = g_strdup(name);
	store->collection.collections = NULL;
	store->connection = j_connection_ref(connection);
	store->semantics = j_semantics_new();
	store->ref_count = 1;

	return store;
}

JStore*
j_store_ref (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	store->ref_count++;

	return store;
}

void
j_store_unref (JStore* store)
{
	g_return_if_fail(store != NULL);

	store->ref_count--;

	if (store->ref_count == 0)
	{
		j_connection_unref(store->connection);
		j_semantics_unref(store->semantics);

		g_free(store->collection.collections);
		g_free(store->name);

		g_slice_free(JStore, store);
	}
}

const gchar*
j_store_name (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	return store->name;
}

JSemantics*
j_store_semantics (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	return store->semantics;
}

void
j_store_set_semantics (JStore* store, JSemantics* semantics)
{
	g_return_if_fail(store != NULL);
	g_return_if_fail(semantics != NULL);

	if (store->semantics != NULL)
	{
		j_semantics_unref(store->semantics);
	}

	store->semantics = j_semantics_ref(semantics);
}

JConnection*
j_store_connection (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	return store->connection;
}

void
j_store_create (JStore* store, JList* collections)
{
	JBSON* index;
	JBSON** jobj;
	JListIterator* it;
	mongo_connection* mc;
	bson** obj;
	guint length;
	guint i;

	g_return_if_fail(store != NULL);
	g_return_if_fail(collections != NULL);

	/*
	IsInitialized(true);
	*/

	length = j_list_length(collections);

	if (length == 0)
	{
		return;
	}

	jobj = g_new(JBSON*, length);
	obj = g_new(bson*, length);
	i = 0;
	it = j_list_iterator_new(collections);

	while (j_list_iterator_next(it))
	{
		JCollection* collection = j_list_iterator_get(it);
		JBSON* jbson;

		j_collection_associate(collection, store);
		jbson = j_collection_serialize(collection);

		jobj[i] = jbson;
		obj[i] = j_bson_get(jbson);

		i++;
	}

	j_list_iterator_free(it);

	mc = j_connection_connection(store->connection);

	index = j_bson_new();
	j_bson_append_int(index, "Name", 1);

	mongo_create_index(mc, j_store_collection_collections(store), j_bson_get(index), MONGO_INDEX_UNIQUE, NULL);

	j_bson_unref(index);

	mongo_insert_batch(mc, j_store_collection_collections(store), obj, length);

	for (i = 0; i < length; i++)
	{
		j_bson_unref(jobj[i]);
	}

	g_free(jobj);
	g_free(obj);

	/*
	{
		bson oerr;

		mongo_cmd_get_last_error(mc, store->name, &oerr);
		bson_print(&oerr);
		bson_destroy(&oerr);
	}
	*/

	if (j_semantics_get(store->semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_STRICT)
	{
		bson ores;

		mongo_simple_int_command(mc, "admin", "fsync", 1, &ores);
		//bson_print(&ores);
		bson_destroy(&ores);
	}
}

JList*
j_store_get (JStore* store, JList* names)
{
	JBSON* empty;
	JBSON* jbson;
	mongo_connection* mc;
	mongo_cursor* cursor;
	JList* collections;
	guint length;
	guint n = 0;

	g_return_val_if_fail(store != NULL, NULL);
	g_return_val_if_fail(names != NULL, NULL);

	/*
		IsInitialized(true);
	*/

	jbson = j_bson_new();
	length = j_list_length(names);

	if (length == 1)
	{
		const gchar* name = j_list_get(names, 0);

		j_bson_append_str(jbson, "Name", name);
		n = 1;
	}
	else
	{
		JListIterator* it;

		j_bson_append_object_start(jbson, "$or");
		it = j_list_iterator_new(names);

		while (j_list_iterator_next(it))
		{
			const gchar* name = j_list_iterator_get(it);

			j_bson_append_str(jbson, "Name", name);
		}

		j_list_iterator_free(it);
		j_bson_append_object_end(jbson);
	}

	empty = j_bson_new_empty();

	mc = j_connection_connection(store->connection);
	cursor = mongo_find(mc, j_store_collection_collections(store), j_bson_get(jbson), j_bson_get(empty), n, 0, 0);

	collections = j_list_new((JListFreeFunc)j_collection_unref);

	while (mongo_cursor_next(cursor))
	{
		JBSON* collection_bson;

		collection_bson = j_bson_new_from_bson(&(cursor->current));
		j_list_append(collections, j_collection_new_from_bson(store, collection_bson));
		j_bson_unref(collection_bson);
	}

	mongo_cursor_destroy(cursor);

	j_bson_unref(empty);
	j_bson_unref(jbson);

	return collections;
}

/* Internal */

const gchar*
j_store_collection_collections (JStore* store)
{
	g_return_val_if_fail(store != NULL, NULL);

	/*
		IsInitialized(true);
	*/

	if (store->collection.collections == NULL)
	{
		store->collection.collections = g_strdup_printf("%s.Collections", store->name);
	}

	return store->collection.collections;
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
