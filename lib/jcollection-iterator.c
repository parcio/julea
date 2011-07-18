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

#include "jcollection-iterator.h"

#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jitem.h"
#include "jitem-internal.h"

/**
 * \defgroup JCollectionIterator Collection Iterator
 *
 * Data structures and functions for iterating over collections.
 *
 * @{
 **/

struct JCollectionIterator
{
	mongo_cursor* iterator;

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
	JCollectionIterator* iterator;
	bson b;
	mongo* connection;

	g_return_val_if_fail(collection != NULL, NULL);

	iterator = g_slice_new(JCollectionIterator);
	iterator->collection = j_collection_ref(collection);

	connection = j_connection_connection(j_store_connection(j_collection_store(iterator->collection)));

	bson_init(&b);
	bson_append_oid(&b, "Collection", j_collection_id(iterator->collection));
	bson_finish(&b);

	iterator->iterator = mongo_find(connection, j_collection_collection_items(iterator->collection), &b, NULL, 0, 0, 0);

	bson_destroy(&b);

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

	mongo_cursor_destroy(iterator->iterator);

	j_collection_unref(iterator->collection);

	g_slice_free(JCollectionIterator, iterator);
}

gboolean
j_collection_iterator_next (JCollectionIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return (mongo_cursor_next(iterator->iterator) == MONGO_OK);
}

JItem*
j_collection_iterator_get (JCollectionIterator* iterator)
{
	JItem* item;

	g_return_val_if_fail(iterator != NULL, NULL);

	item = j_item_new_from_bson(iterator->collection, &(iterator->iterator->current));

	return item;
}

/**
 * @}
 **/
