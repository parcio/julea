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

#include <jitem-iterator.h>

#include <jbackend.h>
#include <jbackend-internal.h>
#include <jcollection.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
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
	JBackend* meta_backend;

	JCollection* collection;

	gpointer cursor;

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

	g_return_val_if_fail(collection != NULL, NULL);

	j_operation_cache_flush();

	iterator = g_slice_new(JItemIterator);
	iterator->meta_backend = j_metadata_backend();
	iterator->collection = j_collection_ref(collection);
	iterator->cursor = NULL;
	iterator->current = NULL;

	if (iterator->meta_backend != NULL)
	{
		bson_t query[1];

		bson_init(query);
		bson_append_oid(query, "Collection", -1, j_collection_get_id(iterator->collection));

		iterator->meta_backend->u.meta.get_by_value("items", query, &(iterator->cursor));
	}

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
	gboolean ret = FALSE;

	g_return_val_if_fail(iterator != NULL, FALSE);

	if (iterator->meta_backend != NULL)
	{
		ret = iterator->meta_backend->u.meta.iterate(iterator->cursor, &(iterator->current));
	}

	return ret;
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
