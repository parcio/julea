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

#include <bson.h>
#include <mongo.h>

#include "store.h"

#include "bson.h"
#include "collection.h"
#include "collection-internal.h"
#include "connection.h"
#include "connection-internal.h"

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

static
const gchar*
j_store_collection_collections (JStore* store)
{
	/*
		IsInitialized(true);
	*/
	if (store->collection.collections == NULL)
	{
		store->collection.collections = g_strdup_printf("%s.Collections", store->name);
	}

	return store->collection.collections;
}

JStore*
j_store_new (JConnection* connection, const gchar* name)
{
	JStore* store;
	/*
	: m_initialized(true),
	m_collectionsCollection("")
	*/

	store = g_new(JStore, 1);
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
	store->ref_count++;

	return store;
}

void
j_store_unref (JStore* store)
{
	store->ref_count--;

	if (store->ref_count == 0)
	{
		j_connection_unref(store->connection);
		j_semantics_unref(store->semantics);

		g_free(store->collection.collections);
		g_free(store->name);
		g_free(store);
	}
}

const gchar*
j_store_name (JStore* store)
{
	return store->name;
}

JSemantics*
j_store_semantics (JStore* store)
{
	return store->semantics;
}

void
j_store_set_semantics (JStore* store, JSemantics* semantics)
{
	if (store->semantics != NULL)
	{
		j_semantics_unref(store->semantics);
	}

	store->semantics = j_semantics_ref(semantics);
}

JConnection*
j_store_connection (JStore* store)
{
	return store->connection;
}

void
j_store_create (JStore* store, GList* collections)
{
	mongo_connection* mc;
	JBSON* index;
	JBSON** jobj;
	bson** obj;
	guint length;
	guint i;

	/*
	IsInitialized(true);
	*/

	length = g_list_length(collections);

	if (length == 0)
	{
		return;
	}

	jobj = g_new(JBSON*, length);
	obj = g_new(bson*, length);
	i = 0;

	for (GList* l = collections; l != NULL; l = l->next)
	{
		JCollection* collection = l->data;
		JBSON* jbson;

		j_collection_associate(collection, store);
		jbson = j_collection_serialize(collection);

		jobj[i] = jbson;
		obj[i] = j_bson_get(jbson);

		i++;
	}

	mc = j_connection_connection(store->connection);

	index = j_bson_new();
	j_bson_append_int(index, "Name", 1);

	mongo_create_index(mc, j_store_collection_collections(store), j_bson_get(index), MONGO_INDEX_UNIQUE, NULL);

	j_bson_free(index);

	mongo_insert_batch(mc, j_store_collection_collections(store), obj, length);

	for (i = 0; i < length; i++)
	{
		j_bson_free(jobj[i]);
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

/*
#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "exception.h"

using namespace std;
using namespace mongo;

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

	_Connection* _Store::Connection ()
	{
		return m_connection;
	}

	list<Collection> _Store::Get (list<string> names)
	{
		BSONObjBuilder ob;
		int n = 0;

		IsInitialized(true);

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

		list<Collection> collections;
		ScopedDbConnection* c = m_connection->GetMongoDB();
		DBClientBase* b = c->get();

		auto_ptr<DBClientCursor> cur = b->query(CollectionsCollection(), ob.obj(), n);

		while (cur->more())
		{
			collections.push_back(Collection(this, cur->next()));
		}

		c->done();
		delete c;

		return collections;
	}
}
*/
