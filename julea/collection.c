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

#include "collection.h"

#include "semantics.h"
#include "store.h"

struct JCollection
{
	gchar* name;

	JSemantics* semantics;
	JStore* store;

	guint ref_count;
};

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

		g_free(collection->name);
		g_free(collection);
	}
}

void
j_collection_create (JCollection* collection, GList* items)
{
	/*
	IsInitialized(true);

	if (items.size() == 0)
	{
		return;
	}

	BSONObj o;
	vector<BSONObj> obj;
	list<Item>::iterator it;

	o = BSONObjBuilder()
		.append("Collection", 1)
		.append("Name", 1)
		.obj();

	for (it = items.begin(); it != items.end(); ++it)
	{
		(*it)->Associate(this);
		obj.push_back((*it)->Serialize());
	}

	ScopedDbConnection* c = m_store->Connection()->GetMongoDB();
	DBClientBase* b = c->get();

	b->ensureIndex(ItemsCollection(), o, true);
	b->insert(ItemsCollection(), obj);

	if (GetSemantics()->GetPersistency() == Persistency::Strict)
	{
		BSONObj ores;

		b->runCommand("admin", BSONObjBuilder().append("fsync", 1).obj(), ores);
		//cout << ores << endl;
	}

	c->done();
	delete c;
	*/
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

	BSONObj _Collection::Serialize ()
	{
		BSONObj o;

		o = BSONObjBuilder()
			.append("_id", m_id)
			.append("Name", m_name)
			.append("User", m_owner.User())
			.append("Group", m_owner.Group())
			.obj();

		return o;
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

	void _Collection::Associate (_Store* store)
	{
		IsInitialized(false);

		m_store = store->Ref();
		m_initialized = true;
	}

	string const& _Collection::Name () const
	{
		return m_name;
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

	_Semantics const* _Collection::GetSemantics ()
	{
		if (m_semantics != 0)
		{
			return m_semantics;
		}

		return m_store->GetSemantics();
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
