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

#include <julea-config.h>

#include <glib.h>

#include <sys/types.h>
#include <unistd.h>

#include <bson.h>

#include <jcredentials.h>

#include <jcommon-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JCredentials Credentials
 * @{
 **/

/**
 * A JCredentials.
 **/
struct JCredentials
{
	guint64 user;
	guint64 group;

	gint ref_count;
};

JCredentials*
j_credentials_new (void)
{
	JCredentials* credentials;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	credentials = g_slice_new(JCredentials);
	credentials->user = geteuid();
	credentials->group = getegid();
	credentials->ref_count = 1;

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return credentials;
}

JCredentials*
j_credentials_ref (JCredentials* credentials)
{
	g_return_val_if_fail(credentials != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	g_atomic_int_inc(&(credentials->ref_count));

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return credentials;
}

void
j_credentials_unref (JCredentials* credentials)
{
	g_return_if_fail(credentials != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(credentials->ref_count)))
	{
		g_slice_free(JCredentials, credentials);
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

guint64
j_credentials_get_user (JCredentials* credentials)
{
	guint64 ret;

	g_return_val_if_fail(credentials != NULL, 0);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	ret = credentials->user;
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

guint64
j_credentials_get_group (JCredentials* credentials)
{
	guint64 ret;

	g_return_val_if_fail(credentials != NULL, 0);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	ret = credentials->group;
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

/* Internal */

/**
 * Serializes credentials.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param credentials Credentials.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
bson*
j_credentials_serialize (JCredentials* credentials)
{
	bson* b;

	g_return_val_if_fail(credentials != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	b = g_slice_new(bson);
	bson_init(b);
	bson_append_long(b, "User", credentials->user);
	bson_append_long(b, "Group", credentials->group);
	bson_finish(b);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return b;
}

/**
 * Deserializes credentials.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param credentials credentials.
 * \param b           A BSON object.
 **/
void
j_credentials_deserialize (JCredentials* credentials, bson const* b)
{
	bson_iterator iterator;

	g_return_if_fail(credentials != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	bson_iterator_init(&iterator, b);

	while (bson_iterator_next(&iterator))
	{
		gchar const* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "User") == 0)
		{
			credentials->user = bson_iterator_long(&iterator);
		}
		else if (g_strcmp0(key, "Group") == 0)
		{
			credentials->group = bson_iterator_long(&iterator);
		}
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * @}
 **/
