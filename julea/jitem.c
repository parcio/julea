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

#include <string.h>

#include "jitem.h"
#include "jitem-internal.h"

#include "jbson.h"
#include "jbson-iterator.h"
#include "jcommon.h"
#include "jcollection.h"
#include "jcollection-internal.h"
#include "jconnection-internal.h"
#include "jdistribution.h"
#include "jmessage.h"
#include "jsemantics.h"

/**
 * \defgroup JItem Item
 *
 * Data structures and functions for managing items.
 *
 * @{
 **/

/**
 * A JItem.
 **/
struct JItem
{
	gchar* name;

	JCollection* collection;
	JSemantics* semantics;

	guint ref_count;
};

JItem*
j_item_new (const gchar* name)
{
	JItem* item;

	g_return_val_if_fail(name != NULL, NULL);

	item = g_slice_new(JItem);
	item->name = g_strdup(name);
	item->collection = NULL;
	item->semantics = NULL;
	item->ref_count = 1;

	return item;
}

void
j_item_unref (JItem* item)
{
	g_return_if_fail(item != NULL);

	item->ref_count--;

	if (item->ref_count == 0)
	{
		if (item->collection != NULL)
		{
			j_collection_unref(item->collection);
		}

		if (item->semantics != NULL)
		{
			j_semantics_unref(item->semantics);
		}

		g_free(item->name);

		g_slice_free(JItem, item);
	}
}

const gchar*
j_item_name (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	return item->name;
}

JSemantics*
j_item_semantics (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	if (item->semantics != NULL)
	{
		return item->semantics;
	}

	return j_collection_semantics(item->collection);
}

void
j_item_set_semantics (JItem* item, JSemantics* semantics)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(semantics != NULL);

	if (item->semantics != NULL)
	{
		j_semantics_unref(item->semantics);
	}

	item->semantics = j_semantics_ref(semantics);
}

gboolean
j_item_write (JItem* item, gconstpointer data, guint64 length, guint64 offset)
{
	JDistribution* distribution;
	guint64 new_length;
	guint64 new_offset;
	guint index;
	gchar const* d;

	g_return_val_if_fail(item != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);

	distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, length, offset);
	d = data;

	while (j_distribution_iterate(distribution, &index, &new_length, &new_offset))
	{
		JMessage* message;
		gchar const* store;
		gchar const* collection;
		gsize store_len;
		gsize collection_len;
		gsize item_len;

		store = j_store_name(j_collection_store(item->collection));
		collection = j_collection_name(item->collection);

		store_len = strlen(store) + 1;
		collection_len = strlen(collection) + 1;
		item_len = strlen(item->name) + 1;

		message = j_message_new(store_len + collection_len + item_len + sizeof(guint64) + sizeof(guint64), J_MESSAGE_OP_WRITE);
		j_message_append_n(message, store, store_len);
		j_message_append_n(message, collection, collection_len);
		j_message_append_n(message, item->name, item_len);
		j_message_append_8(message, &new_length);
		j_message_append_8(message, &new_offset);

		j_connection_send(j_store_connection(j_collection_store(item->collection)), index, message, d, new_length);

		j_message_free(message);

		d += new_length;
	}

	j_distribution_free(distribution);

	return TRUE;
}

/* Internal */

JItem*
j_item_new_from_bson (JCollection* collection, JBSON* bson)
{
	JItem* item;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(bson != NULL, NULL);

	item = g_slice_new(JItem);
	item->name = NULL;
	item->collection = j_collection_ref(collection);
	item->semantics = NULL;
	item->ref_count = 1;

	j_item_deserialize(item, bson);

	return item;
}

/**
 * Associates a JItem with a JCollection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param item The JItem.
 * \param collection The JCollection.
 **/
void
j_item_associate (JItem* item, JCollection* collection)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(collection != NULL);

		/*
		IsInitialized(false);
		m_initialized = true;
		*/
	item->collection = j_collection_ref(collection);
}

JBSON*
j_item_serialize (JItem* item)
{
	JBSON* bson;

	g_return_val_if_fail(item != NULL, NULL);

	bson = j_bson_new();
	j_bson_append_new_object_id(bson, "_id");
	/* FIXME id */
	j_bson_append_string(bson, "Collection", j_collection_name(item->collection));
	j_bson_append_string(bson, "Name", item->name);

	return bson;
}

void
j_item_deserialize (JItem* item, JBSON* bson)
{
	JBSONIterator* iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(bson != NULL);

	iterator = j_bson_iterator_new(bson);

	/*
		m_id = o.getField("_id").OID();
	*/

	while (j_bson_iterator_next(iterator))
	{
		const gchar* key;

		key = j_bson_iterator_get_key(iterator);

		if (g_strcmp0(key, "Name") == 0)
		{
			g_free(item->name);
			item->name = g_strdup(j_bson_iterator_get_string(iterator));
		}
	}

	j_bson_iterator_free(iterator);
}

/*
#include "item.h"

#include "exception.h"

namespace JULEA
{
	void _Item::IsInitialized (bool check)
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Item not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Item already initialized.");
			}
		}
	}
}
*/

/**
 * @}
 **/
