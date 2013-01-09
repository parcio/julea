/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#ifndef H_URI
#define H_URI

/**
 * \addtogroup JURI
 *
 * @{
 **/

/**
 * A URI error domain.
 **/
#define J_URI_ERROR j_uri_error_quark()

/**
 * A URI error code.
 **/
typedef enum
{
	J_URI_ERROR_STORE_NOT_FOUND,
	J_URI_ERROR_COLLECTION_NOT_FOUND,
	J_URI_ERROR_ITEM_NOT_FOUND
}
JURIError;

struct JURI;

/**
 * A URI.
 **/
typedef struct JURI JURI;

#include <jcollection.h>
#include <jitem.h>
#include <jstore.h>

#include <glib.h>

GQuark j_uri_error_quark (void);

JURI* j_uri_new (gchar const*);
void j_uri_free (JURI*);

gchar const* j_uri_get_store_name (JURI*);
gchar const* j_uri_get_collection_name (JURI*);
gchar const* j_uri_get_item_name (JURI*);

gboolean j_uri_get (JURI*, GError**);

JStore* j_uri_get_store (JURI*);
JCollection* j_uri_get_collection (JURI*);
JItem* j_uri_get_item (JURI*);

/**
 * @}
 **/

#endif
