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

#include "jbson-iterator.h"

#include "jbson.h"

/**
 * \defgroup JBSONIterator BSON Iterator
 *
 * Data structures and functions for iterating over binary JSON objects.
 *
 * @{
 **/

/**
 * A JBSON iterator.
 **/
struct JBSONIterator
{
	/**
	 * The associated JBSON object.
	 **/
	JBSON* jbson;

	/**
	 * BSON iterator.
	 **/
	bson_iterator iterator;
};

/**
 * Creates a new JBSON iterator.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param jbson A JBSON object.
 *
 * \return A new JBSONIterator.
 **/
JBSONIterator*
j_bson_iterator_new (JBSON* jbson)
{
	JBSONIterator* iterator;
	bson* bson_;

	iterator = g_slice_new(JBSONIterator);
	iterator->jbson = j_bson_ref(jbson);

	bson_ = j_bson_get(iterator->jbson);
	bson_iterator_init(&(iterator->iterator), bson_->data);

	return iterator;
}

/**
 * Frees the memory allocated for the JBSON iterator.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A JBSONIterator.
 **/
void
j_bson_iterator_free (JBSONIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	j_bson_unref(iterator->jbson);

	g_slice_free(JBSONIterator, iterator);
}

gboolean
j_bson_iterator_next (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	return (bson_iterator_next(&(iterator->iterator)) != bson_eoo);
}

const gchar*
j_bson_iterator_get_key (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return bson_iterator_key(&(iterator->iterator));
}

bson_oid_t*
j_bson_iterator_get_id (JBSONIterator* iterator)
{
	//g_return_val_if_fail(iterator != NULL, NULL);

	return bson_iterator_oid(&(iterator->iterator));
}

gint
j_bson_iterator_get_int (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, -1);

	return bson_iterator_int(&(iterator->iterator));
}

const gchar*
j_bson_iterator_get_string (JBSONIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return bson_iterator_string(&(iterator->iterator));
}

/**
 * @}
 **/
