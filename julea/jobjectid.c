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
#include <sys/types.h>
#include <unistd.h>

#include "jobjectid.h"

/**
 * \defgroup JOBJECTID Object ID
 *
 * Data structures and functions for handling object IDs.
 *
 * @{
 **/

/**
 * An object ID.
 **/
struct JObjectID
{
	union
	{
		gint32 ints[3];
		gchar chars[12];
	}
	data;
};

/**
 * Creates a new object ID.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new object ID.
 **/
JObjectID*
j_object_id_new (gboolean initialize)
{
	JObjectID* id;

	id = g_slice_new(JObjectID);

	if (initialize)
	{
		j_object_id_init(id);
	}

	return id;
}

JObjectID*
j_object_id_new_for_data (gconstpointer data)
{
	JObjectID* id;

	id = g_slice_new(JObjectID);

	memcpy(&(id->data), data, 12);

	return id;
}

void
j_object_id_free (JObjectID* id)
{
	g_return_if_fail(id != NULL);

	g_slice_free(JObjectID, id);
}

void
j_object_id_init (JObjectID* id)
{
	static GPid pid = 0;
	static gint counter = 0;

	GTimeVal tv;

	g_return_if_fail(id != NULL);

	if (G_UNLIKELY(pid == 0))
	{
		pid = getpid();
	}

	g_get_current_time(&tv);

	id->data.ints[0] = GINT32_TO_BE(tv.tv_sec);
	id->data.ints[1] = pid;
	id->data.ints[2] = GINT32_TO_BE(g_atomic_int_get(&counter));

	g_atomic_int_inc(&counter);
}

gchar*
j_object_id_hex (JObjectID* id)
{
	gchar hex[16] = {
		'0', '1', '2', '3',
		'4', '5', '6', '7',
		'8', '9', 'a', 'b',
		'c', 'd', 'e', 'f'
	};
	gchar* hash;
	guint i;

	g_return_val_if_fail(id != NULL, NULL);

	hash = g_new(gchar, 25);

	for (i = 0; i < 12; i++)
	{
		hash[2 * i]     = hex[(id->data.chars[i] >> 4) & 0x0F];
		hash[2 * i + 1] = hex[ id->data.chars[i]       & 0x0F];
	}

	hash[24] = '\0';

	return hash;
}

gconstpointer
j_object_id_data (JObjectID* id)
{
	g_return_val_if_fail(id != NULL, NULL);

	return &(id->data);
}

/**
 * @}
 **/
