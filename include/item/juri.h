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

#ifndef JULEA_ITEM_URI_H
#define JULEA_ITEM_URI_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \addtogroup JURI
 *
 * @{
 **/

/**
 * A URI error domain.
 **/
#define J_URI_ERROR j_uri_error_quark()

/**
 * A URI error code.
 **/
typedef enum
{
	J_URI_ERROR_STORE_NOT_FOUND,
	J_URI_ERROR_COLLECTION_NOT_FOUND,
	J_URI_ERROR_ITEM_NOT_FOUND,
	J_URI_ERROR_STORE_EXISTS,
	J_URI_ERROR_COLLECTION_EXISTS,
	J_URI_ERROR_ITEM_EXISTS
} JURIError;

struct JURI;

/**
 * A URI.
 **/
typedef struct JURI JURI;

G_END_DECLS

#include <item/jcollection.h>
#include <item/jitem.h>

G_BEGIN_DECLS

GQuark j_uri_error_quark(void);

JURI* j_uri_new(gchar const*);
void j_uri_free(JURI*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JURI, j_uri_free)

gchar const* j_uri_get_collection_name(JURI*);
gchar const* j_uri_get_item_name(JURI*);

gboolean j_uri_get(JURI*, GError**);
gboolean j_uri_create(JURI*, gboolean, GError**);
gboolean j_uri_delete(JURI*, GError**);

JCollection* j_uri_get_collection(JURI*);
JItem* j_uri_get_item(JURI*);

/**
 * @}
 **/

G_END_DECLS

#endif
