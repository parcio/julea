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

#include <glib.h>

#include <bson.h>

#include "bson.h"

struct JBSON
{
	bson b;
	bson_buffer buf;
};

JBSON*
j_bson_new (void)
{
	JBSON* jbson;

	jbson = g_new(JBSON, 1);

	bson_buffer_init(&(jbson->buf));

	return jbson;
}

void
j_bson_free (JBSON* jbson)
{
	bson_destroy(&(jbson->b));
	/*
	bson_buffer_destroy(&(jbson->buf));
	*/

	g_free(jbson);
}

void
j_bson_append_int (JBSON* jbson, const gchar* key, gint value)
{
	bson_append_int(&(jbson->buf), key, value);
}

void
j_bson_append_str (JBSON* jbson, const gchar* key, const gchar* value)
{
	bson_append_string(&(jbson->buf), key, value);
}

bson*
j_bson_get (JBSON* jbson)
{
	bson_from_buffer(&(jbson->b), &(jbson->buf));

	return &(jbson->b);
}
