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

#include "jbson-iterator.h"

#include "jbson.h"

/**
 * \defgroup JBSONIterator BSON Iterator
 *
 * Data structures and functions for iterating over binary JSON documents.
 *
 * @{
 **/

/**
 * A BSON iterator.
 **/
struct JBSONIterator
{
	/**
	 * The associated BSON document.
	 **/
	JBSON* bson;

	const gchar* data;
	gboolean first;
};

static const gchar*
j_bson_iterator_get_value (JBSONIterator* iterator)
{
	const gchar* key;
	gsize len;

	key = iterator->data + 1;
	len = strlen(key) + 1;

	return (key + len);
}

static gchar
j_bson_iterator_get_8 (gconstpointer data)
{
	return *((const gchar*)data);
}

static gint32
j_bson_iterator_get_32 (gconstpointer data)
{
	return GINT32_FROM_LE(*((const gint32*)data));
}

static gint64
j_bson_iterator_get_64 (gconstpointer data)
{
	return GINT64_FROM_LE(*((const gint64*)data));
}

/**
 * Creates a new BSON iterator.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 *
 * \return A new BSON iterator.
 **/
JBSONIterator*
j_bson_iterator_new (JBSON* bson)
{
	JBSONIterator* iterator;

	iterator = g_slice_new(JBSONIterator);
	iterator->bson = j_bson_ref(bson);
	iterator->data = j_bson_data(iterator->bson);
	iterator->first = TRUE;

	iterator->data += 4;

	return iterator;
}

/**
 * Frees the memory allocated for the BSON iterator.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A BSON iterator.
 **/
void
j_bson_iterator_free (JBSONIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	j_bson_unref(iterator->bson);

	g_slice_free(JBSONIterator, iterator);
}

gboolean
j_bson_iterator_find (JBSONIterator* iterator, const gchar* key)
{
	gboolean ret = FALSE;

	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	while (j_bson_iterator_next(iterator))
	{
		if (g_strcmp0(j_bson_iterator_get_key(iterator), key) == 0)
		{
			ret = TRUE;
		}
	}

	return ret;
}

gboolean
j_bson_iterator_next (JBSONIterator* iterator)
{
	JBSONType type;
	gsize offset = 0;

	g_return_val_if_fail(iterator != NULL, FALSE);

	type = j_bson_iterator_get_8(iterator->data);

	switch (type)
	{
		case J_BSON_TYPE_END:
			return FALSE;
		case J_BSON_TYPE_STRING:
			offset = 4 + j_bson_iterator_get_32(j_bson_iterator_get_value(iterator));
			break;
		case J_BSON_TYPE_DOCUMENT:
			offset = j_bson_iterator_get_32(j_bson_iterator_get_value(iterator));
			break;
		case J_BSON_TYPE_OBJECT_ID:
			offset = 12;
			break;
		case J_BSON_TYPE_BOOLEAN:
			offset = 1;
			break;
		case J_BSON_TYPE_INT32:
			offset = 4;
			break;
		case J_BSON_TYPE_INT64:
			offset = 8;
			break;
		default:
			g_warn_if_reached();
			return FALSE;
	}

	if (!iterator->first)
	{
		iterator->data = j_bson_iterator_get_value(iterator) + offset;
	}
	else
	{
		iterator->first = FALSE;
	}

	type = j_bson_iterator_get_8(iterator->data);

	return (type != J_BSON_TYPE_END);
}

const gchar*
j_bson_iterator_get_key (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return (iterator->data + 1);
}

JObjectID*
j_bson_iterator_get_id (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return j_object_id_new_for_data(j_bson_iterator_get_value(iterator));
}

gboolean
j_bson_iterator_get_boolean (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return j_bson_iterator_get_8(j_bson_iterator_get_value(iterator));
}

gint32
j_bson_iterator_get_int32 (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, -1);

	return j_bson_iterator_get_32(j_bson_iterator_get_value(iterator));
}

gint64
j_bson_iterator_get_int64 (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, -1);

	return j_bson_iterator_get_64(j_bson_iterator_get_value(iterator));
}

const gchar*
j_bson_iterator_get_string (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return (j_bson_iterator_get_value(iterator) + 4);
}

/**
 * @}
 **/
