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

#ifndef JULEA_DISTRIBUTION_H
#define JULEA_DISTRIBUTION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <bson.h>

G_BEGIN_DECLS

/**
 * \defgroup JDistribution Distribution
 *
 * Data structures and functions for managing distributions.
 *
 * @{
 **/

enum JDistributionType
{
	J_DISTRIBUTION_ROUND_ROBIN,
	J_DISTRIBUTION_SINGLE_SERVER,
	J_DISTRIBUTION_WEIGHTED
};

typedef enum JDistributionType JDistributionType;

struct JDistribution;

typedef struct JDistribution JDistribution;

G_END_DECLS

#include <core/jconfiguration.h>

G_BEGIN_DECLS

/**
 * Creates a new distribution.
 *
 * \code
 * JDistribution* d;
 *
 * d = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
 * \endcode
 *
 * \param type   A distribution type.
 *
 * \return A new distribution. Should be freed with \ref j_distribution_unref().
 **/
JDistribution* j_distribution_new(JDistributionType type);

/**
 * Creates a new distribution for a given configuration.
 *
 * \code
 * JDistribution* d;
 *
 * d = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
 * \endcode
 *
 * \param type          A distribution type.
 * \param configuration A JConfiguration.
 *
 * \return A new distribution. Should be freed with \ref j_distribution_unref().
 **/
JDistribution* j_distribution_new_for_configuration(JDistributionType type, JConfiguration* configuration);

/**
 * Creates a new distribution from a BSON object.
 *
 * \code
 * \endcode
 *
 * \param b A BSON object.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
JDistribution* j_distribution_new_from_bson(bson_t const* b);

/**
 * Increases a distribution's reference count.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 **/
JDistribution* j_distribution_ref(JDistribution* distribution);

/**
 * Decreases a distribution's reference count.
 * When the reference count reaches zero, frees the memory allocated for the distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 **/
void j_distribution_unref(JDistribution* distribution);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JDistribution, j_distribution_unref)

/**
 * Serializes distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A JDistribution.
 *
 * \return A new BSON object. Should be freed with bson_destroy().
 **/
bson_t* j_distribution_serialize(JDistribution* distribution);

/**
 * Deserializes distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A JDistribution.
 * \param b            A BSON object.
 **/
void j_distribution_deserialize(JDistribution* distribution, bson_t const* b);

/**
 * Sets the block size for the distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param block_size   A block size.
 */
void j_distribution_set_block_size(JDistribution* distribution, guint64 block_size);

/**
 * Set a property of the distribution. Settings depend on the distribution type.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param key		   Name of the property to set.
 * \param value		   Value to be set for the property.
 */
void j_distribution_set(JDistribution* distribution, gchar const* key, guint64 value);

/**
 * Set a property of the distribution which needs to values. Settings depend on the distribution type.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param key		   Name of the property to set.
 * \param value1	   Value to be set for the property.
 * \param value2	   Value to be set for the property.
 */
void j_distribution_set2(JDistribution* distribution, gchar const* key, guint64 value1, guint64 value2);

/**
 * Resets a distribution.
 *
 * \code
 * JDistribution* d;
 *
 * j_distribution_reset(d, 0, 0);
 * \endcode
 *
 * \param distribution A JDistribution.
 * \param length 	   A length.
 * \param offset       An offset.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
void j_distribution_reset(JDistribution* distribution, guint64 length, guint64 offset);

/**
 * Calculates a new length and a new offset based on a distribution.
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param index        A server index.
 * \param new_length   A new length.
 * \param new_offset   A new offset.
 * \param block_id     A block ID.
 *
 * \return TRUE on success, FALSE if the distribution is finished.
 **/
gboolean j_distribution_distribute(JDistribution* distribution, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id);

/**
 * @}
 **/

G_END_DECLS

#endif
