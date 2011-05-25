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
#include "jobjectid.h"
#include "jsemantics.h"
#include "jtrace.h"

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
	JObjectID* id;
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

	j_trace_enter(j_trace(), G_STRFUNC);

	item = g_slice_new(JItem);
	item->id = j_object_id_new(TRUE);
	item->name = g_strdup(name);
	item->collection = NULL;
	item->semantics = NULL;
	item->ref_count = 1;

	j_trace_leave(j_trace(), G_STRFUNC);

	return item;
}

JItem*
j_item_ref (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	item->ref_count++;

	j_trace_leave(j_trace(), G_STRFUNC);

	return item;
}

void
j_item_unref (JItem* item)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

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

		if (item->id != NULL)
		{
			j_object_id_free(item->id);
		}

		g_free(item->name);

		g_slice_free(JItem, item);
	}

	j_trace_leave(j_trace(), G_STRFUNC);
}

const gchar*
j_item_name (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return item->name;
}

JSemantics*
j_item_semantics (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (item->semantics != NULL)
	{
		return item->semantics;
	}

	j_trace_leave(j_trace(), G_STRFUNC);

	return j_collection_semantics(item->collection);
}

void
j_item_set_semantics (JItem* item, JSemantics* semantics)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(semantics != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	if (item->semantics != NULL)
	{
		j_semantics_unref(item->semantics);
	}

	item->semantics = j_semantics_ref(semantics);

	j_trace_leave(j_trace(), G_STRFUNC);
}

gboolean
j_item_read (JItem* item, gpointer data, guint64 length, guint64 offset, guint64* bytes_read)
{
	JDistribution* distribution;
	gboolean ret = TRUE;
	gchar* d;
	guint64 new_length;
	guint64 new_offset;
	guint index;

	g_return_val_if_fail(item != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(bytes_read != NULL, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	*bytes_read = 0;

	if (length == 0)
	{
		ret = FALSE;
		goto end;
	}

	j_trace_file_begin(j_trace(), item->name, J_TRACE_FILE_READ);

	distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, length, offset);
	d = data;

	while (j_distribution_distribute(distribution, &index, &new_length, &new_offset))
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

		message = j_message_new(store_len + collection_len + item_len + sizeof(guint64) + sizeof(guint64), J_MESSAGE_OPERATION_READ, 1);
		j_message_append_n(message, store, store_len);
		j_message_append_n(message, collection, collection_len);
		j_message_append_n(message, item->name, item_len);
		j_message_append_8(message, &new_length);
		j_message_append_8(message, &new_offset);

		j_connection_send(j_store_connection(j_collection_store(item->collection)), index, message, NULL, 0);
		j_connection_receive(j_store_connection(j_collection_store(item->collection)), index, message, d, new_length);

		j_message_free(message);

		d += new_length;
		*bytes_read += new_length;
	}

	j_distribution_free(distribution);

	j_trace_file_end(j_trace(), item->name, J_TRACE_FILE_READ, length, offset);

end:
	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
}

gboolean
j_item_write (JItem* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written)
{
	JDistribution* distribution;
	gboolean ret = TRUE;
	guint64 new_length;
	guint64 new_offset;
	guint index;
	gchar const* d;

	g_return_val_if_fail(item != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(bytes_written != NULL, FALSE);

	j_trace_enter(j_trace(), G_STRFUNC);

	*bytes_written = 0;

	if (length == 0)
	{
		ret = FALSE;
		goto end;
	}

	j_trace_file_begin(j_trace(), item->name, J_TRACE_FILE_WRITE);

	distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN, length, offset);
	d = data;

	while (j_distribution_distribute(distribution, &index, &new_length, &new_offset))
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

		message = j_message_new(store_len + collection_len + item_len + sizeof(guint64) + sizeof(guint64), J_MESSAGE_OPERATION_WRITE, 1);
		j_message_append_n(message, store, store_len);
		j_message_append_n(message, collection, collection_len);
		j_message_append_n(message, item->name, item_len);
		j_message_append_8(message, &new_length);
		j_message_append_8(message, &new_offset);

		j_connection_send(j_store_connection(j_collection_store(item->collection)), index, message, d, new_length);

		j_message_free(message);

		d += new_length;
		*bytes_written += new_length;
	}

	j_distribution_free(distribution);

	j_trace_file_end(j_trace(), item->name, J_TRACE_FILE_WRITE, length, offset);

end:
	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
}

/* Internal */

JItem*
j_item_new_from_bson (JCollection* collection, JBSON* bson)
{
	JItem* item;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(bson != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	item = g_slice_new(JItem);
	item->id = NULL;
	item->name = NULL;
	item->collection = j_collection_ref(collection);
	item->semantics = NULL;
	item->ref_count = 1;

	j_item_deserialize(item, bson);

	j_trace_leave(j_trace(), G_STRFUNC);

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

	j_trace_enter(j_trace(), G_STRFUNC);

		/*
		IsInitialized(false);
		m_initialized = true;
		*/
	item->collection = j_collection_ref(collection);

	j_trace_leave(j_trace(), G_STRFUNC);
}

JBSON*
j_item_serialize (JItem* item)
{
	JBSON* bson;

	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	bson = j_bson_new();
	j_bson_append_object_id(bson, "_id", item->id);
	j_bson_append_object_id(bson, "Collection", j_collection_id(item->collection));
	j_bson_append_string(bson, "Name", item->name);

	j_trace_leave(j_trace(), G_STRFUNC);

	return bson;
}

void
j_item_deserialize (JItem* item, JBSON* bson)
{
	JBSONIterator* iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(bson != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	iterator = j_bson_iterator_new(bson);

	while (j_bson_iterator_next(iterator))
	{
		const gchar* key;

		key = j_bson_iterator_get_key(iterator);

		if (g_strcmp0(key, "_id") == 0)
		{
			if (item->id != NULL)
			{
				j_object_id_free(item->id);
			}

			item->id = j_bson_iterator_get_id(iterator);
		}
		else if (g_strcmp0(key, "Name") == 0)
		{
			g_free(item->name);
			item->name = g_strdup(j_bson_iterator_get_string(iterator));
		}
	}

	j_bson_iterator_free(iterator);

	j_trace_leave(j_trace(), G_STRFUNC);
}

JObjectID*
j_item_id (JItem* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return item->id;
}

/*
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
*/

/**
 * @}
 **/
