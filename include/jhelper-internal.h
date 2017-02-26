/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

#ifndef H_HELPER_INTERNAL
#define H_HELPER_INTERNAL

#include <glib.h>
#include <gio/gio.h>

#include <bson.h>
#include <mongoc.h>

#include <julea-internal.h>

enum JStoreCollection
{
	J_STORE_COLLECTION_COLLECTIONS,
	J_STORE_COLLECTION_ITEMS,
	J_STORE_COLLECTION_LOCKS
};

typedef enum JStoreCollection JStoreCollection;

#include <jsemantics.h>

#include <bson.h>
#include <mongoc.h>

J_GNUC_INTERNAL void j_helper_set_nodelay (GSocketConnection*, gboolean);
J_GNUC_INTERNAL void j_helper_set_cork (GSocketConnection*, gboolean);

J_GNUC_INTERNAL gboolean j_helper_insert_batch (mongoc_collection_t*, bson_t**, guint, mongoc_write_concern_t*);
J_GNUC_INTERNAL void j_helper_set_write_concern (mongoc_write_concern_t*, JSemantics*);

J_GNUC_INTERNAL void j_helper_get_number_string (gchar*, guint32, guint32);

J_GNUC_INTERNAL void j_helper_create_index (JStoreCollection, mongoc_client_t*, bson_t const*);

#endif
