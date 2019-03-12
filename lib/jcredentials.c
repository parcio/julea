/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

#include <jcommon.h>
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

	j_trace_enter(G_STRFUNC, NULL);

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

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(credentials->ref_count));

	j_trace_leave(G_STRFUNC);

	return credentials;
}

void
j_credentials_unref (JCredentials* credentials)
{
	g_return_if_fail(credentials != NULL);

	j_trace_enter(G_STRFUNC, NULL);

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

	j_trace_enter(G_STRFUNC, NULL);
	j_trace_leave(G_STRFUNC);

	return credentials->user;
}

guint32
j_credentials_get_group (JCredentials* credentials)
{
	g_return_val_if_fail(credentials != NULL, 0);

	j_trace_enter(G_STRFUNC, NULL);
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

	j_trace_enter(G_STRFUNC, NULL);

	b = bson_new();

	bson_append_int32(b, "user", -1, credentials->user);
	bson_append_int32(b, "group", -1, credentials->group);
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

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "user") == 0)
		{
			credentials->user = bson_iter_int32(&iterator);
		}
		else if (g_strcmp0(key, "group") == 0)
		{
			credentials->group = bson_iter_int32(&iterator);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
