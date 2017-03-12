/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#ifndef H_ITEM_URI
#define H_ITEM_URI

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
}
JURIError;

struct JURI;

/**
 * A URI.
 **/
typedef struct JURI JURI;

#include <item/jcollection.h>
#include <item/jitem.h>

#include <glib.h>

GQuark j_uri_error_quark (void);

JURI* j_uri_new (gchar const*);
void j_uri_free (JURI*);

gchar const* j_uri_get_collection_name (JURI*);
gchar const* j_uri_get_item_name (JURI*);

gboolean j_uri_get (JURI*, GError**);
gboolean j_uri_create (JURI*, gboolean, GError**);

JCollection* j_uri_get_collection (JURI*);
JItem* j_uri_get_item (JURI*);

/**
 * @}
 **/

#endif
