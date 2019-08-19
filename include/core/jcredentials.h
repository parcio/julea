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

#ifndef JULEA_CREDENTIALS_H
#define JULEA_CREDENTIALS_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

G_BEGIN_DECLS

struct JCredentials;

typedef struct JCredentials JCredentials;

JCredentials* j_credentials_new (void);
JCredentials* j_credentials_ref (JCredentials*);
void j_credentials_unref (JCredentials*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JCredentials, j_credentials_unref)

guint32 j_credentials_get_user (JCredentials*);
guint32 j_credentials_get_group (JCredentials*);

bson_t* j_credentials_serialize (JCredentials*);
void j_credentials_deserialize (JCredentials*, bson_t const*);

G_END_DECLS

#endif
