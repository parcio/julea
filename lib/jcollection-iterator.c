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

#include <jcollection-iterator.h>

#include <jcollection.h>
#include <jcollection-internal.h>
#include <jconnection-pool-internal.h>
#include <joperation-cache-internal.h>

/**
 * \defgroup JCollectionIterator Store Iterator
 *
 * Data structures and functions for iterating over stores.
 *
 * @{
 **/

struct JCollectionIterator
{
	mongoc_client_t* connection;

	/**
	 * The MongoDB cursor.
	 **/
	mongoc_cursor_t* cursor;

	/**
	 * The current document.
	 **/
	bson_t const* current;
};

/**
 * Creates a new JCollectionIterator.
 *
 * \author Michael Kuhn
 *
 * \param store A JStore.
 *
 * \return A new JCollectionIterator.
 **/
JCollectionIterator*
j_collection_iterator_new (void)
{
	JCollectionIterator* iterator;
	bson_t empty[1];
	mongoc_collection_t* m_collection;

	j_operation_cache_flush();

	iterator = g_slice_new(JCollectionIterator);
	iterator->connection = j_connection_pool_pop_meta(0);
	iterator->current = NULL;

	bson_init(empty);

	/* FIXME */
	m_collection = mongoc_client_get_collection(iterator->connection, "JULEA", "Collections");
	iterator->cursor = mongoc_collection_find_with_opts(m_collection, empty, NULL, NULL);

	bson_destroy(empty);

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

	mongoc_cursor_destroy(iterator->cursor);
	j_connection_pool_push_meta(0, iterator->connection);

	g_slice_free(JCollectionIterator, iterator);
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
j_collection_iterator_next (JCollectionIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	mongoc_cursor_next(iterator->cursor, &(iterator->current));

	return (iterator->current != NULL);
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
j_collection_iterator_get (JCollectionIterator* iterator)
{
	JCollection* collection;

	g_return_val_if_fail(iterator != NULL, NULL);
	g_return_val_if_fail(iterator->current != NULL, NULL);

	collection = j_collection_new_from_bson(iterator->current);

	return collection;
}

/**
 * @}
 **/
