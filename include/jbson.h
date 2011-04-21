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

#ifndef H_BSON
#define H_BSON

struct JBSON;

typedef struct JBSON JBSON;

enum JBSONType
{
	J_BSON_TYPE_END = 0,
	J_BSON_TYPE_STRING = 2,
	J_BSON_TYPE_DOCUMENT = 3,
	J_BSON_TYPE_OBJECT_ID = 7,
	J_BSON_TYPE_BOOLEAN = 8,
	J_BSON_TYPE_INT32 = 16,
	J_BSON_TYPE_INT64 = 18
};

typedef enum JBSONType JBSONType;

#include <glib.h>

#include "jobjectid.h"

JBSON* j_bson_new (void);
JBSON* j_bson_new_for_data (gconstpointer);
JBSON* j_bson_new_empty (void);
JBSON* j_bson_ref (JBSON*);
void j_bson_unref (JBSON*);

gint32 j_bson_size (JBSON*);

void j_bson_append_new_object_id (JBSON*, const gchar*);
void j_bson_append_object_id (JBSON*, const gchar*, JObjectID*);
void j_bson_append_boolean (JBSON*, const gchar*, gboolean);
void j_bson_append_int32 (JBSON*, const gchar*, gint32);
void j_bson_append_int64 (JBSON*, const gchar*, gint64);
void j_bson_append_string (JBSON*, const gchar*, const gchar*);
void j_bson_append_document (JBSON*, const gchar*, JBSON*);

gpointer j_bson_data (JBSON*);

#endif
