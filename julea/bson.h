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

#ifndef H_BSON
#define H_BSON

struct JBSON;

typedef struct JBSON JBSON;

#include <glib.h>

#include <bson.h>

JBSON* j_bson_new (void);
JBSON* j_bson_new_from_bson (bson*);
JBSON* j_bson_new_empty (void);
void j_bson_free (JBSON*);

void j_bson_append_object_start (JBSON*, const gchar*);
void j_bson_append_object_end (JBSON*);

void j_bson_append_new_id (JBSON*, const gchar*);
void j_bson_append_id (JBSON*, const gchar*, const bson_oid_t*);
void j_bson_append_int (JBSON*, const gchar*, gint);
void j_bson_append_str (JBSON*, const gchar*, const gchar*);

bson* j_bson_get (JBSON*);

#endif
