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

#include "juri.h"

/**
 * \defgroup JURI URI
 *
 * @{
 **/

/**
 * A URI.
 **/
struct JURI
{
	gchar* store;
	gchar* collection;
	gchar* item;
};

static
gboolean
j_uri_parse (JURI* uri, gchar const* uri_)
{
	gchar** parts = NULL;
	guint parts_len;

	if (!g_str_has_prefix(uri_, "julea://"))
	{
		goto error;
	}

	parts = g_strsplit(uri_ + 8, "/", 0);
	parts_len = g_strv_length(parts);

	if (parts_len < 1 || parts_len > 3)
	{
		goto error;
	}

	if (parts_len >= 1)
	{
		uri->store = g_strdup(parts[0]);
	}

	if (parts_len >= 2)
	{
		uri->collection = g_strdup(parts[1]);
	}

	if (parts_len >= 3)
	{
		uri->item = g_strdup(parts[2]);
	}

	g_strfreev(parts);

	return TRUE;

error:
	g_strfreev(parts);

	return FALSE;
}

/**
 * Creates a new URI.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param uri_ A URI.
 *
 * \return A new URI. Should be freed with j_uri_free().
 **/
JURI*
j_uri_new (gchar const* uri_)
{
	JURI* uri;

	uri = g_slice_new(JURI);

	if (!j_uri_parse(uri, uri_))
	{
		g_slice_free(JURI, uri);

		return NULL;
	}

	return uri;
}

/**
 * Frees the memory allocated by a URI.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param uri A URI.
 **/
void
j_uri_free (JURI* uri)
{
	g_return_if_fail(uri != NULL);

	g_slice_free(JURI, uri);
}

gchar const*
j_uri_get_store (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->store;
}

gchar const*
j_uri_get_collection (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->collection;
}

gchar const*
j_uri_get_item (JURI* uri)
{
	g_return_val_if_fail(uri != NULL, NULL);

	return uri->item;
}

/**
 * @}
 **/
