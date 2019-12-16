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

#ifndef JULEA_KV_KV_URI_H
#define JULEA_KV_KV_URI_H

#if !defined(JULEA_KV_H) && !defined(JULEA_KV_COMPILATION)
#error "Only <julea-kv.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \addtogroup JKVURI
 *
 * @{
 **/

enum JKVURIScheme
{
	// kv://index/namespace
	J_KV_URI_SCHEME_NAMESPACE,
	// kv://index/namespace/key
	J_KV_URI_SCHEME_KV
};

typedef enum JKVURIScheme JKVURIScheme;

struct JKVURI;

typedef struct JKVURI JKVURI;

G_END_DECLS

#include <kv/jkv.h>

G_BEGIN_DECLS

JKVURI* j_kv_uri_new(gchar const*, JKVURIScheme);
void j_kv_uri_free(JKVURI*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JKVURI, j_kv_uri_free)

guint32 j_kv_uri_get_index(JKVURI*);
gchar const* j_kv_uri_get_namespace(JKVURI*);
gchar const* j_kv_uri_get_name(JKVURI*);

JKV* j_kv_uri_get_kv(JKVURI*);

/**
 * @}
 **/

G_END_DECLS

#endif
