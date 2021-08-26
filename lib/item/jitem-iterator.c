/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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
 * \addtogroup JItemIterator
 *
 * @{
 **/

struct JItemIterator
{
	JCollection* collection;
	JKVIterator* iterator;
};

JItemIterator*
j_item_iterator_new(JCollection* collection)
{
	JItemIterator* iterator;
	g_autofree gchar* prefix = NULL;

	g_return_val_if_fail(collection != NULL, NULL);

	prefix = g_strdup_printf("%s/", j_collection_get_name(collection));

	iterator = g_slice_new(JItemIterator);
	iterator->collection = j_collection_ref(collection);
	iterator->iterator = j_kv_iterator_new("items", prefix);

	return iterator;
}

void
j_item_iterator_free(JItemIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	j_kv_iterator_free(iterator->iterator);
	j_collection_unref(iterator->collection);

	g_slice_free(JItemIterator, iterator);
}

gboolean
j_item_iterator_next(JItemIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return j_kv_iterator_next(iterator->iterator);
}

JItem*
j_item_iterator_get(JItemIterator* iterator)
{
	JItem* item;
	bson_t tmp[1];
	gconstpointer value;
	guint32 len;

	g_return_val_if_fail(iterator != NULL, NULL);

	j_kv_iterator_get(iterator->iterator, &value, &len);
	bson_init_static(tmp, value, len);
	item = j_item_new_from_bson(iterator->collection, tmp);

	return item;
}

/**
 * @}
 **/
