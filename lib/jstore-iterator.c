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

#include "jstore-iterator.h"

#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jstore.h"
#include "jstore-internal.h"

/**
 * \defgroup JStoreIterator Store Iterator
 *
 * Data structures and functions for iterating over stores.
 *
 * @{
 **/

struct JStoreIterator
{
	mongo_cursor* cursor;

	JStore* store;
};

/**
 * Creates a new JStoreIterator.
 *
 * \author Michael Kuhn
 *
 * \param store A JStore.
 *
 * \return A new JStoreIterator.
 **/
JStoreIterator*
j_store_iterator_new (JStore* store)
{
	JStoreIterator* iterator;
	bson empty;
	mongo* connection;

	g_return_val_if_fail(store != NULL, NULL);

	iterator = g_slice_new(JStoreIterator);
	iterator->store = j_store_ref(store);

	connection = j_connection_get_connection(j_store_get_connection(iterator->store));

	bson_empty(&empty);

	iterator->cursor = mongo_find(connection, j_store_collection_collections(iterator->store), &empty, NULL, 0, 0, 0);

	return iterator;
}

/**
 * Frees the memory allocated by the JStoreIterator.
 *
 * \author Michael Kuhn
 *
 * \param iterator A JStoreIterator.
 **/
void
j_store_iterator_free (JStoreIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	mongo_cursor_destroy(iterator->cursor);

	j_store_unref(iterator->store);

	g_slice_free(JStoreIterator, iterator);
}

/**
 * Checks whether another collection is available.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return TRUE on success, FALSE if the end of the store is reached.
 **/
gboolean
j_store_iterator_next (JStoreIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return (mongo_cursor_next(iterator->cursor) == MONGO_OK);
}

/**
 * Returns the current collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_store_iterator_get (JStoreIterator* iterator)
{
	JCollection* collection;

	g_return_val_if_fail(iterator != NULL, NULL);

	collection = j_collection_new_from_bson(iterator->store, mongo_cursor_bson(iterator->cursor));

	return collection;
}

/**
 * @}
 **/
