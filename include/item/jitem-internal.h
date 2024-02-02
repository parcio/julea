/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2023 Michael Kuhn
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

#ifndef JULEA_ITEM_ITEM_INTERNAL_H
#define JULEA_ITEM_ITEM_INTERNAL_H

#if !defined(JULEA_ITEM_H) && !defined(JULEA_ITEM_COMPILATION)
#error "Only <julea-item.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

#include <julea.h>

#include <item/jcollection.h>
#include <item/jitem.h>

G_BEGIN_DECLS

/**
 * \addtogroup JItem
 *
 * @{
 **/

/**
 * Creates a new item.
 *
 * \private
 * \code
 * JItem* i;
 *
 * i = j_item_new("JULEA");
 * \endcode
 *
 * \param collection   A collection.
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
G_GNUC_INTERNAL JItem* j_item_new(JCollection* collection, gchar const* name, JDistribution* distribution);

/**
 * Creates a new item from a BSON object.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param b          A BSON object.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
G_GNUC_INTERNAL JItem* j_item_new_from_bson(JCollection* collection, bson_t const* b);

/**
 * Returns an item's collection.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A collection.
 **/
G_GNUC_INTERNAL JCollection* j_item_get_collection(JItem* item);

/**
 * Serializes an item.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param semantics A semantics object.
 *
 * \return A new BSON object. Should be freed with bson_destroy().
 **/
G_GNUC_INTERNAL bson_t* j_item_serialize(JItem* item, JSemantics* semantics);

/**
 * Deserializes an item.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param b    A BSON object.
 **/
G_GNUC_INTERNAL void j_item_deserialize(JItem* item, bson_t const* b);

/**
 * Returns an item's ID.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return An ID.
 **/
G_GNUC_INTERNAL bson_oid_t const* j_item_get_id(JItem* item);

/**
 * Sets an item's modification time.
 *
 * \code
 * \endcode
 *
 * \param item              An item.
 * \param modification_time A modification time.
 **/
G_GNUC_INTERNAL void j_item_set_modification_time(JItem* item, gint64 modification_time);

/**
 * Sets an item's size.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param size A size.
 **/
G_GNUC_INTERNAL void j_item_set_size(JItem* item, guint64 size);

/**
 * @}
 **/

G_END_DECLS

#endif
