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

#include "jitem.h"
#include "jitem-internal.h"

#include "jcollection.h"
#include "jsemantics.h"

/**
 * \defgroup JItem Item
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

	item = g_new(JItem, 1);
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
		g_free(item);
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

/* Internal */

JItem*
j_item_new_from_bson (JCollection* collection, JBSON* jbson)
{
	JItem* item;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(jbson != NULL, NULL);

	item = g_new(JItem, 1);
	item->name = NULL;
	item->collection = j_collection_ref(collection);
	item->semantics = NULL;
	item->ref_count = 1;

	j_item_deserialize(item, jbson);

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
	JBSON* jbson;

	g_return_val_if_fail(item != NULL, NULL);

	jbson = j_bson_new();
	j_bson_append_new_id(jbson, "_id");
	/* FIXME id */
	j_bson_append_str(jbson, "Collection", j_collection_name(item->collection));
	j_bson_append_str(jbson, "Name", item->name);

	return jbson;
}

void
j_item_deserialize (JItem* item, JBSON* jbson)
{
	g_return_if_fail(item != NULL);
	g_return_if_fail(jbson != NULL);

	/*
		m_id = o.getField("_id").OID();
		m_name = o.getField("Name").String();
	*/

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
