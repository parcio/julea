/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <bson.h>

#include <item/jcollection-iterator.h>

#include <item/jcollection.h>
#include <item/jcollection-internal.h>

#include <julea-kv.h>

/**
 * \defgroup JCollectionIterator Store Iterator
 *
 * Data structures and functions for iterating over stores.
 *
 * @{
 **/

struct JCollectionIterator
{
	JKVIterator* iterator;
};

/**
 * Creates a new JCollectionIterator.
 *
 * \param store A JStore.
 *
 * \return A new JCollectionIterator.
 **/
JCollectionIterator*
j_collection_iterator_new(void)
{
	JCollectionIterator* iterator;

	iterator = g_slice_new(JCollectionIterator);
	iterator->iterator = j_kv_iterator_new("collections", NULL);

	return iterator;
}

/**
 * Frees the memory allocated by the JCollectionIterator.
 *
 * \param iterator A JCollectionIterator.
 **/
void
j_collection_iterator_free(JCollectionIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	j_kv_iterator_free(iterator->iterator);

	g_slice_free(JCollectionIterator, iterator);
}

/**
 * Checks whether another collection is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return TRUE on success, FALSE if the end of the store is reached.
 **/
gboolean
j_collection_iterator_next(JCollectionIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return j_kv_iterator_next(iterator->iterator);
}

/**
 * Returns the current collection.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_collection_iterator_get(JCollectionIterator* iterator)
{
	JCollection* collection;
	bson_t tmp[1];
	gconstpointer value;
	guint32 len;

	g_return_val_if_fail(iterator != NULL, NULL);

	j_kv_iterator_get(iterator->iterator, &value, &len);
	bson_init_static(tmp, value, len);
	collection = j_collection_new_from_bson(tmp);

	return collection;
}

/**
 * @}
 **/
