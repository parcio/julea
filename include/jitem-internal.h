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

#ifndef H_ITEM_INTERNAL
#define H_ITEM_INTERNAL

#include <glib.h>

#include <julea-internal.h>

#include <jitem.h>

#include <jcollection.h>
#include <jcredentials-internal.h>
#include <jsemantics.h>

#include <bson.h>

J_GNUC_INTERNAL JItem* j_item_new (JCollection*, gchar const*, JDistribution*);
J_GNUC_INTERNAL JItem* j_item_new_from_bson (JCollection*, bson_t const*);

J_GNUC_INTERNAL JCollection* j_item_get_collection (JItem*);
J_GNUC_INTERNAL JCredentials* j_item_get_credentials (JItem*);

J_GNUC_INTERNAL bson_t* j_item_serialize (JItem*, JSemantics*);
J_GNUC_INTERNAL void j_item_deserialize (JItem*, bson_t const*);

J_GNUC_INTERNAL bson_oid_t const* j_item_get_id (JItem*);

J_GNUC_INTERNAL void j_item_set_modification_time (JItem*, gint64);
J_GNUC_INTERNAL void j_item_set_size (JItem*, guint64);

J_GNUC_INTERNAL gboolean j_item_read_internal (JBatch*, JList*);
J_GNUC_INTERNAL gboolean j_item_write_internal (JBatch*, JList*);

J_GNUC_INTERNAL gboolean j_item_get_status_internal (JBatch*, JList*);

#endif
