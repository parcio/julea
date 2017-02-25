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

#include <julea-config.h>

#include <glib.h>

#include <sys/types.h>
#include <unistd.h>

#include <bson.h>

#include <jcredentials-internal.h>

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
	guint32 user;
	guint32 group;

	gint ref_count;
};

JCredentials*
j_credentials_new (void)
{
	JCredentials* credentials;

	j_trace_enter(G_STRFUNC);

	credentials = g_slice_new(JCredentials);
	credentials->user = geteuid();
	credentials->group = getegid();
	credentials->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return credentials;
}

JCredentials*
j_credentials_ref (JCredentials* credentials)
{
	g_return_val_if_fail(credentials != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	g_atomic_int_inc(&(credentials->ref_count));

	j_trace_leave(G_STRFUNC);

	return credentials;
}

void
j_credentials_unref (JCredentials* credentials)
{
	g_return_if_fail(credentials != NULL);

	j_trace_enter(G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(credentials->ref_count)))
	{
		g_slice_free(JCredentials, credentials);
	}

	j_trace_leave(G_STRFUNC);
}

guint32
j_credentials_get_user (JCredentials* credentials)
{
	g_return_val_if_fail(credentials != NULL, 0);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return credentials->user;
}

guint32
j_credentials_get_group (JCredentials* credentials)
{
	g_return_val_if_fail(credentials != NULL, 0);

	j_trace_enter(G_STRFUNC);
	j_trace_leave(G_STRFUNC);

	return credentials->group;
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
bson_t*
j_credentials_serialize (JCredentials* credentials)
{
	bson_t* b;

	g_return_val_if_fail(credentials != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	b = g_slice_new(bson_t);
	bson_init(b);
	bson_append_int32(b, "User", -1, credentials->user);
	bson_append_int32(b, "Group", -1, credentials->group);
	//bson_finish(b);

	j_trace_leave(G_STRFUNC);

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
j_credentials_deserialize (JCredentials* credentials, bson_t const* b)
{
	bson_iter_t iterator;

	g_return_if_fail(credentials != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "User") == 0)
		{
			credentials->user = bson_iter_int32(&iterator);
		}
		else if (g_strcmp0(key, "Group") == 0)
		{
			credentials->group = bson_iter_int32(&iterator);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
