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

#include "jbson.h"

#include "jobjectid.h"

/**
 * \defgroup JBSON BSON
 *
 * Data structures and functions for handling binary JSON documents.
 *
 * @{
 **/

/*
 * See http://bsonspec.org/.
 */

/**
 * A BSON document.
 **/
struct JBSON
{
	/**
	 * The data.
	 **/
	gchar* data;

	/**
	 * Current position within #data.
	 **/
	gchar* current;

	/**
	 * Size of #data.
	 **/
	gsize allocated_size;

	gboolean finalized;

	/**
	 * Reference count.
	 **/
	guint ref_count;
};

static void
j_bson_resize (JBSON* bson, gsize n)
{
	gsize pos = 4;

	if (bson->current != NULL)
	{
		pos = bson->current - bson->data;
	}

	bson->allocated_size += n;
	bson->data = g_renew(gchar, bson->data, bson->allocated_size);
	bson->current = bson->data + pos;
}

static void
j_bson_finalize (JBSON* bson)
{
	gint32 size;

	if (bson->finalized)
	{
		return;
	}

	size = bson->current - bson->data + 1;
	*((gint32*)(bson->data)) = GINT32_TO_LE(size);
	*(bson->current) = '\0';

	bson->finalized = TRUE;
}

static void
j_bson_append_8 (JBSON* bson, gconstpointer data)
{
	*(bson->current) = *((const gchar*)data);
	bson->current++;
}

static void
j_bson_append_32 (JBSON* bson, gconstpointer data)
{
	*((gint32*)(bson->current)) = GINT32_TO_LE(*((const gint32*)data));
	bson->current += 4;
}

static void
j_bson_append_64 (JBSON* bson, gconstpointer data)
{
	*((gint64*)(bson->current)) = GINT64_TO_LE(*((const gint64*)data));
	bson->current += 8;
}

static void
j_bson_append_n (JBSON* bson, gconstpointer data, gsize n)
{
	memcpy(bson->current, data, n);
	bson->current += n;
}

static void
j_bson_append_element (JBSON* bson, const gchar* name, gchar type, gsize n)
{
	gsize len;

	len = strlen(name) + 1;
	j_bson_resize(bson, 1 + len + n);

	j_bson_append_8(bson, &type);
	j_bson_append_n(bson, name, len);
}

/**
 * Creates a new BSON document.
 *
 * \author Michael Kuhn
 *
 * \code
 * JBSON* j;
 * j = j_bson_new();
 * \endcode
 *
 * \return A new BSON document.
 **/
JBSON*
j_bson_new (void)
{
	JBSON* bson;

	bson = g_slice_new(JBSON);
	bson->data = NULL;
	bson->current = NULL;
	bson->allocated_size = 0;
	bson->finalized = FALSE;
	bson->ref_count = 1;

	j_bson_resize(bson, 128);

	return bson;
}

/**
 * Creates a new empty BSON document.
 *
 * \author Michael Kuhn
 *
 * \code
 * JBSON* j;
 * j = j_bson_new_empty();
 * \endcode
 *
 * \return A new empty BSON document.
 **/
JBSON*
j_bson_new_empty (void)
{
	static JBSON* bson = NULL;

	if (G_UNLIKELY(bson == NULL))
	{
		bson = g_slice_new(JBSON);
		bson->data = NULL;
		bson->current = NULL;
		bson->allocated_size = 0;
		bson->finalized = FALSE;
		bson->ref_count = 1;

		j_bson_resize(bson, 5);
	}

	j_bson_ref(bson);

	return bson;
}

/**
 * Increases the BSON document's reference count.
 *
 * \author Michael Kuhn
 *
 * \param list A BSON document.
 *
 * \return The BSON document.
 **/
JBSON*
j_bson_ref (JBSON* bson)
{
	g_return_val_if_fail(bson != NULL, NULL);

	bson->ref_count++;

	return bson;
}

/**
 * Decreases the BSON document's reference count.
 * When the reference count reaches zero, frees the memory allocated for the BSON document.
 *
 * \author Michael Kuhn
 *
 * \code
 * JBSON* j;
 * j = j_bson_new();
 * j_bson_unref(j);
 * \endcode
 *
 * \param bson A BSON document.
 **/
void
j_bson_unref (JBSON* bson)
{
	g_return_if_fail(bson != NULL);

	bson->ref_count--;

	if (bson->ref_count == 0)
	{
		g_free(bson->data);

		g_slice_free(JBSON, bson);
	}
}

/**
 * Returns the BSON document's size.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 *
 * \return The size.
 **/
gint32
j_bson_size (JBSON* bson)
{
	g_return_val_if_fail(bson != NULL, -1);

	j_bson_finalize(bson);

	return GINT32_FROM_LE(*((const gint32*)(bson->data)));
}

/**
 * Appends a newly generated object ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The object ID's key.
 **/
void
j_bson_append_new_object_id (JBSON* bson, const gchar* key)
{
	JObjectID* id;

	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);

	id = j_object_id_new(TRUE);
	j_bson_append_object_id(bson, key, id);
	j_object_id_free(id);
}

/**
 * Appends an object ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The ID's key.
 * \param value The object ID.
 **/
void
j_bson_append_object_id (JBSON* bson, const gchar* key, JObjectID* value)
{
	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);

	j_bson_append_element(bson, key, J_BSON_TYPE_OBJECT_ID, 12);
	j_bson_append_n(bson, j_object_id_data(value), 12);
	bson->finalized = FALSE;
}

/**
 * Appends a boolean.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The boolean's key.
 * \param value The boolean.
 **/
void
j_bson_append_boolean (JBSON* bson, const gchar* key, gboolean value)
{
	gchar v;

	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);

	v = (value) ? 1 : 0;

	j_bson_append_element(bson, key, J_BSON_TYPE_BOOLEAN, 1);
	j_bson_append_8(bson, &value);
	bson->finalized = FALSE;
}

/**
 * Appends a 32 bit integer.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The integer's key.
 * \param value The integer.
 **/
void
j_bson_append_int32 (JBSON* bson, const gchar* key, gint32 value)
{
	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);

	j_bson_append_element(bson, key, J_BSON_TYPE_INT32, 4);
	j_bson_append_32(bson, &value);
	bson->finalized = FALSE;
}

/**
 * Appends a 64 bit integer.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The integer's key.
 * \param value The integer.
 **/
void
j_bson_append_int64 (JBSON* bson, const gchar* key, gint64 value)
{
	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);

	j_bson_append_element(bson, key, J_BSON_TYPE_INT64, 8);
	j_bson_append_64(bson, &value);
	bson->finalized = FALSE;
}

/**
 * Appends a string.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The string's key.
 * \param value The string.
 **/
void
j_bson_append_string (JBSON* bson, const gchar* key, const gchar* value)
{
	gsize len;

	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);
	g_return_if_fail(value != NULL);

	len = strlen(value) + 1;
	j_bson_append_element(bson, key, J_BSON_TYPE_STRING, len);
	j_bson_append_32(bson, &len);
	j_bson_append_n(bson, value, len);
	bson->finalized = FALSE;
}

/**
 * Appends a BSON document.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 * \param key The BSON document's key.
 * \param value The BSON document.
 **/
void
j_bson_append_document (JBSON* bson, const gchar* key, JBSON* value)
{
	gint32 size;

	g_return_if_fail(bson != NULL);
	g_return_if_fail(key != NULL);
	g_return_if_fail(value != NULL);
	g_return_if_fail(bson != value);

	size = j_bson_size(value);
	j_bson_append_element(bson, key, J_BSON_TYPE_DOCUMENT, size);
	j_bson_append_n(bson, j_bson_data(value), size);
	bson->finalized = FALSE;
}

/**
 * Constructs and returns the BSON document's data.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param bson A BSON document.
 *
 * \return The data.
 **/
gpointer
j_bson_data (JBSON* bson)
{
	g_return_val_if_fail(bson != NULL, NULL);

	j_bson_finalize(bson);

	return bson->data;
}

/**
 * @}
 **/
