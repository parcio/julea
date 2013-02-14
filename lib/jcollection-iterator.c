/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <bson.h>
#include <mongo.h>

#include <jcollection-iterator.h>

#include <jcollection.h>
#include <jcollection-internal.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jconnection-pool-internal.h>
#include <jitem.h>
#include <jitem-internal.h>
#include <jbatch-internal.h>
#include <joperation-cache-internal.h>

/**
 * \defgroup JCollectionIterator Collection Iterator
 *
 * Data structures and functions for iterating over collections.
 *
 * @{
 **/

struct JCollectionIterator
{
	mongo_cursor* cursor;

	JCollection* collection;
	JConnection* connection;
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
	bson fields;

	g_return_val_if_fail(collection != NULL, NULL);

	j_operation_cache_flush();

	iterator = g_slice_new(JCollectionIterator);
	iterator->collection = j_collection_ref(collection);
	iterator->connection = j_connection_pool_pop();

	bson_init(&fields);
	bson_append_int(&fields, "_id", 1);
	bson_append_int(&fields, "Name", 1);
	bson_finish(&fields);

	bson_init(&b);
	bson_append_oid(&b, "Collection", j_collection_get_id(iterator->collection));
	bson_finish(&b);

	iterator->cursor = mongo_find(j_connection_get_connection(iterator->connection), j_collection_collection_items(iterator->collection), &b, &fields, 0, 0, 0);

	bson_destroy(&fields);
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

	mongo_cursor_destroy(iterator->cursor);
	j_connection_pool_push(iterator->connection);

	j_collection_unref(iterator->collection);

	g_slice_free(JCollectionIterator, iterator);
}

/**
 * Checks whether another item is available.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A collection iterator.
 *
 * \return TRUE on success, FALSE if the end of the collection is reached.
 **/
gboolean
j_collection_iterator_next (JCollectionIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return (mongo_cursor_next(iterator->cursor) == MONGO_OK);
}

/**
 * Returns the current item.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A collection iterator.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_collection_iterator_get (JCollectionIterator* iterator)
{
	JItem* item;

	g_return_val_if_fail(iterator != NULL, NULL);

	item = j_item_new_from_bson(iterator->collection, mongo_cursor_bson(iterator->cursor));

	return item;
}

/**
 * @}
 **/
