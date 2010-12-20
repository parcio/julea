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

#include <bson.h>

#include "jbson.h"

/**
 * \defgroup JBSON BSON
 *
 * Data structures and functions for handling binary JSON objects.
 *
 * @{
 **/

/**
 * A JBSON object.
 * This is basically a wrapper around the bson and bson_buffer data types provided by MongoDB's C driver.
 **/
struct JBSON
{
	/**
	 * BSON object.
	 **/
	bson bson;
	/**
	 * BSON buffer used to build #bson.
	 **/
	bson_buffer buffer;

	/**
	 * Whether to destroy the internal objects.
	 **/
	struct
	{
		/**
		 * Whether to destroy #bson.
		 **/
		gboolean bson;
		/**
		 * Whether to destroy #buffer.
		 **/
		gboolean buffer;
	}
	destroy;
};

/**
 * Creates a new JBSON object.
 *
 * \author Michael Kuhn
 *
 * \code
 * JSBON* j;
 * j = j_bson_new();
 * \endcode
 *
 * \return A new JBSON object.
 **/
JBSON*
j_bson_new (void)
{
	JBSON* jbson;

	jbson = g_slice_new(JBSON);
	jbson->destroy.bson = FALSE;
	jbson->destroy.buffer = TRUE;

	bson_buffer_init(&(jbson->buffer));

	return jbson;
}

/**
 * Creates a new JBSON object from an existing bson object.
 *
 * \author Michael Kuhn
 *
 * \code
 * JBSON* j;
 * j = j_bson_new_from_bson(...);
 * \endcode
 *
 * \param bson_ The existing bson object.
 *
 * \return A new JBSON object.
 **/
JBSON*
j_bson_new_from_bson (bson* bson_)
{
	JBSON* jbson;

	g_return_val_if_fail(bson_ != NULL, NULL);

	jbson = g_slice_new(JBSON);
	jbson->destroy.bson = FALSE;
	jbson->destroy.buffer = FALSE;

	bson_init(&(jbson->bson), bson_->data, 0);

	return jbson;
}

/**
 * Creates a new empty JBSON object.
 *
 * \author Michael Kuhn
 *
 * \code
 * JBSON* j;
 * j = j_bson_new_empty();
 * \endcode
 *
 * \return A new empty JBSON object.
 **/
JBSON*
j_bson_new_empty (void)
{
	JBSON* jbson;

	jbson = g_slice_new(JBSON);
	jbson->destroy.bson = TRUE;
	jbson->destroy.buffer = FALSE;

	bson_empty(&(jbson->bson));

	return jbson;
}

/**
 * Frees the memory allocated for the JBSON object.
 *
 * \author Michael Kuhn
 *
 * \code
 * JBSON* j;
 * j = j_bson_new();
 * j_bson_free(j);
 * \endcode
 *
 * \param jbson A JBSON object.
 **/
void
j_bson_free (JBSON* jbson)
{
	g_return_if_fail(jbson != NULL);

	if (jbson->destroy.bson)
	{
		bson_destroy(&(jbson->bson));
	}

	if (jbson->destroy.buffer)
	{
		bson_buffer_destroy(&(jbson->buffer));
	}

	g_slice_free(JBSON, jbson);
}

/**
 * Starts a new sub-object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 * \param key The sub-object's key.
 **/
void
j_bson_append_object_start (JBSON* jbson, const gchar* key)
{
	g_return_if_fail(jbson != NULL);
	g_return_if_fail(jbson->destroy.buffer);

	bson_append_start_object(&(jbson->buffer), key);
}

/**
 * Ends the last sub-object started.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 **/
void
j_bson_append_object_end (JBSON* jbson)
{
	g_return_if_fail(jbson != NULL);
	g_return_if_fail(jbson->destroy.buffer);

	bson_append_finish_object(&(jbson->buffer));
}

/**
 * Appends a newly generated ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 * \param key The ID's key.
 **/
void
j_bson_append_new_id (JBSON* jbson, const gchar* key)
{
	g_return_if_fail(jbson != NULL);
	g_return_if_fail(jbson->destroy.buffer);

	bson_append_new_oid(&(jbson->buffer), key);
}

/**
 * Appends an ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 * \param key The ID's key.
 * \param value The ID.
 **/
void
j_bson_append_id (JBSON* jbson, const gchar* key, const bson_oid_t* value)
{
	g_return_if_fail(jbson != NULL);
	g_return_if_fail(jbson->destroy.buffer);

	bson_append_oid(&(jbson->buffer), key, value);
}

/**
 * Appends an integer.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 * \param key The integer's key.
 * \param value The integer.
 **/
void
j_bson_append_int (JBSON* jbson, const gchar* key, gint value)
{
	g_return_if_fail(jbson != NULL);
	g_return_if_fail(jbson->destroy.buffer);

	bson_append_int(&(jbson->buffer), key, value);
}

/**
 * Appends a string.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 * \param key The string's key.
 * \param value The string.
 **/
void
j_bson_append_str (JBSON* jbson, const gchar* key, const gchar* value)
{
	g_return_if_fail(jbson != NULL);
	g_return_if_fail(jbson->destroy.buffer);

	bson_append_string(&(jbson->buffer), key, value);
}

/**
 * Constructs and returns the BSON object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 *
 * \return A bson object.
 **/
bson*
j_bson_get (JBSON* jbson)
{
	g_return_val_if_fail(jbson != NULL, NULL);

	if (jbson->destroy.buffer)
	{
		jbson->destroy.bson = TRUE;
		jbson->destroy.buffer = FALSE;

		bson_from_buffer(&(jbson->bson), &(jbson->buffer));
	}

	return &(jbson->bson);
}

/**
 * @}
 **/
