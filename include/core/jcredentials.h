/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2024 Michael Kuhn
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

#ifndef JULEA_CREDENTIALS_H
#define JULEA_CREDENTIALS_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

G_BEGIN_DECLS

/**
 * \defgroup JCredentials Credentials
 *
 * @{
 **/

struct JCredentials;

typedef struct JCredentials JCredentials;

/**
 * Create new credential for the current user.
 *
 * \return A new JCredentials object. Should be freed with j_credentials_unref().
 **/
JCredentials* j_credentials_new(void);

/**
 * Increases the credential's reference count.
 *
 * \param credentials A credentials object.
 *
 * \return The credentials object.
 **/
JCredentials* j_credentials_ref(JCredentials* credentials);

/**
 * Decreases the credential's reference count.
 *
 * \param credentials A credentials object.
 *
 **/
void j_credentials_unref(JCredentials* credentials);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JCredentials, j_credentials_unref)

/**
 * Get the user id associated with the credentials
 *
 * \param credentials A credentials object.
 *
 * \return The user id.
 **/
guint32 j_credentials_get_user(JCredentials* credentials);

/**
 * Get the group id associated with the credentials
 *
 * \param credentials A credentials object.
 *
 * \return The group id.
 **/
guint32 j_credentials_get_group(JCredentials* credentials);

/**
 * Serializes credentials.
 *
 * \code
 * \endcode
 *
 * \param credentials Credentials.
 *
 * \return A new BSON object. Should be freed with bson_destroy().
 **/
bson_t* j_credentials_serialize(JCredentials* credentials);

/**
 * Deserializes credentials.
 *
 * \code
 * \endcode
 *
 * \param credentials credentials.
 * \param b           A BSON object.
 **/
void j_credentials_deserialize(JCredentials* credentials, bson_t const* b);

/**
 * @}
 **/

G_END_DECLS

#endif
