/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include <item/jitem-iterator.h>

#include <item/jcollection.h>
#include <item/jcollection-internal.h>
#include <item/jitem.h>
#include <item/jitem-internal.h>

#include <julea-kv.h>

/**
 * \defgroup JItemIterator Collection Iterator
 *
 * Data structures and functions for iterating over collections.
 *
 * @{
 **/

struct JItemIterator
{
	JCollection* collection;
	JKVIterator* iterator;
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
	g_autofree gchar* prefix = NULL;

	g_return_val_if_fail(collection != NULL, NULL);

	prefix = g_strdup_printf("%s/", j_collection_get_name(collection));

	iterator = g_slice_new(JItemIterator);
	iterator->collection = j_collection_ref(collection);
	iterator->iterator = j_kv_iterator_new(0, "items", prefix);

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

	j_kv_iterator_free(iterator->iterator);
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

	return j_kv_iterator_next(iterator->iterator);
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
	bson_t const* value;

	g_return_val_if_fail(iterator != NULL, NULL);

	value = j_kv_iterator_get(iterator->iterator);
	item = j_item_new_from_bson(iterator->collection, value);

	return item;
}

/**
 * @}
 **/
