/*
 * Copyright (c) 2010 Michael Kuhn
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

#include <mongo.h>

#include "collection.h"
#include "collection-internal.h"

#include "bson.h"
#include "connection.h"
#include "connection-internal.h"
#include "item.h"
#include "item-internal.h"
#include "semantics.h"
#include "store.h"

struct JCollection
{
	gchar* name;

	struct
	{
		gchar* items;
	}
	collection;

	JSemantics* semantics;
	JStore* store;

	guint ref_count;
};

static
const gchar*
j_collection_collection_items (JCollection* collection)
{
	g_return_val_if_fail(collection->store != NULL, NULL);

	if (collection->collection.items == NULL)
	{
		collection->collection.items = g_strdup_printf("%s.Items", j_store_name(collection->store));
	}

	return collection->collection.items;
}

JCollection*
j_collection_new (const gchar* name)
{
	JCollection* collection;
	/*
	: m_initialized(false),
	m_itemsCollection("")

	m_id.init();
	*/

	collection = g_new(JCollection, 1);
	collection->name = g_strdup(name);
	collection->collection.items = NULL;
	collection->semantics = NULL;
	collection->store = NULL;
	collection->ref_count = 1;

	return collection;
}

JCollection*
j_collection_ref (JCollection* collection)
{
	collection->ref_count++;

	return collection;
}

void
j_collection_unref (JCollection* collection)
{
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

		g_free(collection->collection.items);
		g_free(collection->name);
		g_free(collection);
	}
}

const gchar*
j_collection_name (JCollection* collection)
{
	return collection->name;
}

void
j_collection_create (JCollection* collection, GList* items)
{
	JBSON* index;
	JBSON** jobj;
	JSemantics* semantics;
	mongo_connection* mc;
	bson** obj;
	guint length;
	guint i;

	/*
	IsInitialized(true);
	*/

	length = g_list_length(items);

	if (length == 0)
	{
		return;
	}

	jobj = g_new(JBSON*, length);
	obj = g_new(bson*, length);
	i = 0;

	for (GList* l = items; l != NULL; l = l->next)
	{
		JItem* item = l->data;
		JBSON* jbson;

		j_item_associate(item, collection);
		jbson = j_item_serialize(item);

		jobj[i] = jbson;
		obj[i] = j_bson_get(jbson);

		i++;
	}

	mc = j_connection_connection(j_store_connection(collection->store));

	index = j_bson_new();
	j_bson_append_int(index, "Collection", 1);
	j_bson_append_int(index, "Name", 1);

	mongo_create_index(mc, j_collection_collection_items(collection), j_bson_get(index), MONGO_INDEX_UNIQUE, NULL);

	j_bson_free(index);

	mongo_insert_batch(mc, j_collection_collection_items(collection), obj, length);

	for (i = 0; i < length; i++)
	{
		j_bson_free(jobj[i]);
	}

	g_free(jobj);
	g_free(obj);

	semantics = j_collection_semantics(collection);

	if (j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_STRICT)
	{
		bson ores;

		mongo_simple_int_command(mc, "admin", "fsync", 1, &ores);
		//bson_print(&ores);
		bson_destroy(&ores);
	}
}

JSemantics*
j_collection_semantics (JCollection* collection)
{
	if (collection->semantics != NULL)
	{
		return collection->semantics;
	}

	return j_store_semantics(collection->store);
}

void
j_collection_set_semantics (JCollection* collection, JSemantics* semantics)
{
	if (collection->semantics != NULL)
	{
		j_semantics_unref(collection->semantics);
	}

	collection->semantics = j_semantics_ref(semantics);
}

/* Internal */

void
j_collection_associate (JCollection* collection, JStore* store)
{
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

	JBSON* jbson;

	jbson = j_bson_new();
	j_bson_append_new_id(jbson, "_id");
	j_bson_append_str(jbson, "Name", collection->name);

	return jbson;
}

/*
namespace JULEA
{
	_Collection::_Collection (_Store* store, BSONObj const& obj)
		: m_initialized(true),
		  m_name(""),
		  m_semantics(0),
		  m_store(store->Ref()),
		  m_itemsCollection("")
	{
		m_id.init();

		Deserialize(obj);
	}

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

	void _Collection::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		m_name = o.getField("Name").String();

		m_owner.m_user = o.getField("User").Int();
		m_owner.m_group = o.getField("Group").Int();
	}

	string const& _Collection::ItemsCollection ()
	{
		IsInitialized(true);

		if (m_itemsCollection.empty())
		{
			m_itemsCollection = m_store->Name() + ".Items";
		}

		return m_itemsCollection;
	}

	mongo::OID const& _Collection::ID () const
	{
		return m_id;
	}

	list<Item> _Collection::Get (list<string> names)
	{
		BSONObjBuilder ob;
		int n = 0;

		IsInitialized(true);

		ob.append("Collection", m_id);

		if (names.size() == 1)
		{
			ob.append("Name", names.front());
			n = 1;
		}
		else
		{
			BSONObjBuilder obv;
			list<string>::iterator it;

			for (it = names.begin(); it != names.end(); ++it)
			{
				obv.append("Name", *it);
			}

			ob.append("$or", obv.obj());
		}

		list<Item> items;
		ScopedDbConnection* c = m_store->Connection()->GetMongoDB();
		DBClientBase* b = c->get();

		auto_ptr<DBClientCursor> cur = b->query(ItemsCollection(), ob.obj(), n);

		while (cur->more())
		{
			items.push_back(Item(this, cur->next()));
		}

		c->done();
		delete c;

		return items;
	}

	Collection::Iterator::Iterator (Collection const& collection)
		: m_connection(collection->m_store->Connection()->GetMongoDB()),
		  m_collection(collection->Ref())
	{
		DBClientBase* b = m_connection->get();
		BSONObjBuilder ob;

		ob.append("Collection", m_collection->ID());
		m_cursor = b->query(m_collection->ItemsCollection(), ob.obj());
	}

	Collection::Iterator::~Iterator ()
	{
		m_connection->done();
		delete m_connection;

		m_collection->Unref();
	}

	bool Collection::Iterator::More ()
	{
		return m_cursor->more();
	}

	Item Collection::Iterator::Next ()
	{
		return Item(m_collection, m_cursor->next());
	}
}
*/
