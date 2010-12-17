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

/**
 * \file
 **/

#include <glib.h>

#include <bson.h>
#include <mongo.h>

#include "jstore-iterator.h"

#include "jbson.h"
#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jstore.h"
#include "jstore-internal.h"

/**
 * \defgroup JStoreIterator Store Iterator
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
	JBSON* empty;
	JStoreIterator* iterator;
	mongo_connection* mc;

	iterator = g_new(JStoreIterator, 1);
	iterator->store = j_store_ref(store);

	mc = j_connection_connection(j_store_connection(iterator->store));

	empty = j_bson_new_empty();

	iterator->cursor = mongo_find(mc, j_store_collection_collections(iterator->store), j_bson_get(empty), j_bson_get(empty), 0, 0, 0);

	j_bson_free(empty);

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

	g_free(iterator);
}

gboolean
j_store_iterator_next (JStoreIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return (mongo_cursor_next(iterator->cursor) == 1);
}

JCollection*
j_store_iterator_get (JStoreIterator* iterator)
{
	JBSON* jbson;
	JCollection* collection;

	g_return_val_if_fail(iterator != NULL, NULL);

	jbson = j_bson_new_from_bson(&(iterator->cursor->current));
	collection = j_collection_new_from_bson(iterator->store, jbson);
	j_bson_free(jbson);

	return collection;
}

/**
 * @}
 **/
