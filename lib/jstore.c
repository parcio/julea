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

#include "jstore.h"
#include "jstore-internal.h"

#include "jbson.h"
#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jlist.h"
#include "jlist-iterator.h"
#include "jmongo.h"
#include "jmongo-connection.h"

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

/* Internal */

const gchar*
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
