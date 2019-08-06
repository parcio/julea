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
#include <jtrace.h>

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
	J_TRACE_FUNCTION(NULL);

	JCredentials* credentials;

	credentials = g_slice_new(JCredentials);
	credentials->user = geteuid();
	credentials->group = getegid();
	credentials->ref_count = 1;

	return credentials;
}

JCredentials*
j_credentials_ref (JCredentials* credentials)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(credentials != NULL, NULL);

	g_atomic_int_inc(&(credentials->ref_count));

	return credentials;
}

void
j_credentials_unref (JCredentials* credentials)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(credentials != NULL);

	if (g_atomic_int_dec_and_test(&(credentials->ref_count)))
	{
		g_slice_free(JCredentials, credentials);
	}
}

guint32
j_credentials_get_user (JCredentials* credentials)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(credentials != NULL, 0);

	return credentials->user;
}

guint32
j_credentials_get_group (JCredentials* credentials)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(credentials != NULL, 0);

	return credentials->group;
}

/* Internal */

/**
 * Serializes credentials.
 *
 * \private
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
	J_TRACE_FUNCTION(NULL);

	bson_t* b;

	g_return_val_if_fail(credentials != NULL, NULL);

	b = bson_new();

	bson_append_int32(b, "user", -1, credentials->user);
	bson_append_int32(b, "group", -1, credentials->group);
	//bson_finish(b);

	return b;
}

/**
 * Deserializes credentials.
 *
 * \private
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
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;

	g_return_if_fail(credentials != NULL);
	g_return_if_fail(b != NULL);

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
}

/**
 * @}
 **/
