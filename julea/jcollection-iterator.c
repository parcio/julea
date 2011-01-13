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

#include "jcollection-iterator.h"

#include "jbson.h"
#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jitem.h"
#include "jitem-internal.h"
#include "jmongo.h"
#include "jmongo-iterator.h"

/**
 * \defgroup JCollectionIterator Collection Iterator
 *
 * Data structures and functions for iterating over collections.
 *
 * @{
 **/

struct JCollectionIterator
{
	JMongoIterator* iterator;

	JCollection* collection;
};

/**
 * Creates a new JCollectionIterator.
 *
 * \author Michael Kuhn
 *
 * \param collection A JCollection.
 *
 * \return A new JCollectionIterator.
 **/
JCollectionIterator*
j_collection_iterator_new (JCollection* collection)
{
	JBSON* bson;
	JCollectionIterator* iterator;
	JMongoConnection* connection;

	g_return_val_if_fail(collection != NULL, NULL);

	iterator = g_slice_new(JCollectionIterator);
	iterator->collection = j_collection_ref(collection);

	connection = j_connection_connection(j_store_connection(j_collection_store(iterator->collection)));

	bson = j_bson_new();

	// FIXME id
	j_bson_append_string(bson, "Collection", j_collection_name(iterator->collection));

	iterator->iterator = j_mongo_find(connection, j_collection_collection_items(iterator->collection), bson, NULL, 0, 0);

	j_bson_unref(bson);

	return iterator;
}

/**
 * Frees the memory allocated by the JCollectionIterator.
 *
 * \author Michael Kuhn
 *
 * \param iterator A JCollectionIterator.
 **/
void
j_collection_iterator_free (JCollectionIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	j_mongo_iterator_free(iterator->iterator);

	j_collection_unref(iterator->collection);

	g_slice_free(JCollectionIterator, iterator);
}

gboolean
j_collection_iterator_next (JCollectionIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return j_mongo_iterator_next(iterator->iterator);
}

JItem*
j_collection_iterator_get (JCollectionIterator* iterator)
{
	JBSON* bson;
	JItem* item;

	g_return_val_if_fail(iterator != NULL, NULL);

	bson = j_mongo_iterator_get(iterator->iterator);
	item = j_item_new_from_bson(iterator->collection, bson);
	j_bson_unref(bson);

	return item;
}

/**
 * @}
 **/
