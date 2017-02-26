/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
#include <mongoc.h>

#include <jitem-iterator.h>

#include <jcollection.h>
#include <jcollection-internal.h>
#include <jconnection-pool-internal.h>
#include <jitem.h>
#include <jitem-internal.h>
#include <joperation-cache-internal.h>

/**
 * \defgroup JItemIterator Collection Iterator
 *
 * Data structures and functions for iterating over collections.
 *
 * @{
 **/

struct JItemIterator
{
	mongoc_cursor_t* cursor;

	JCollection* collection;
	mongoc_client_t* connection;

	/**
	 * The current document.
	 **/
	bson_t const* current;
};

/**
 * Creates a new JItemIterator.
 *
 * \author Michael Kuhn
 *
 * \param collection A JCollection.
 *
 * \return A new JItemIterator.
 **/
JItemIterator*
j_item_iterator_new (JCollection* collection)
{
	JItemIterator* iterator;
	mongoc_collection_t* m_collection;
	bson_t b;

	g_return_val_if_fail(collection != NULL, NULL);

	j_operation_cache_flush();

	iterator = g_slice_new(JItemIterator);
	iterator->collection = j_collection_ref(collection);
	iterator->connection = j_connection_pool_pop_meta(0);
	iterator->current = NULL;

	bson_init(&b);
	bson_append_oid(&b, "Collection", -1, j_collection_get_id(iterator->collection));
	//bson_finish(&b);

	/* FIXME */
	m_collection = mongoc_client_get_collection(iterator->connection, "JULEA", "Items");
	iterator->cursor = mongoc_collection_find_with_opts(m_collection, &b, NULL, NULL);

	bson_destroy(&b);

	return iterator;
}

/**
 * Frees the memory allocated by the JItemIterator.
 *
 * \author Michael Kuhn
 *
 * \param iterator A JItemIterator.
 **/
void
j_item_iterator_free (JItemIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	mongoc_cursor_destroy(iterator->cursor);
	j_connection_pool_push_meta(0, iterator->connection);

	j_collection_unref(iterator->collection);

	g_slice_free(JItemIterator, iterator);
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
j_item_iterator_next (JItemIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	mongoc_cursor_next(iterator->cursor, &(iterator->current));

	return (iterator->current != NULL);
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
j_item_iterator_get (JItemIterator* iterator)
{
	JItem* item;

	g_return_val_if_fail(iterator != NULL, NULL);
	g_return_val_if_fail(iterator->current != NULL, NULL);

	item = j_item_new_from_bson(iterator->collection, iterator->current);

	return item;
}

/**
 * @}
 **/
