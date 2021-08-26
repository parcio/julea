/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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

#ifndef JULEA_BATCH_INTERNAL_H
#define JULEA_BATCH_INTERNAL_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <core/jbatch.h>

G_BEGIN_DECLS

/**
 * \addtogroup JBatch Batch
 *
 * @{
 **/

/**
 * Copies a batch.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param old_batch A batch.
 *
 * \return A new batch.
 */
G_GNUC_INTERNAL JBatch* j_batch_new_from_batch(JBatch* batch);

/**
 * Returns a batch's parts.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return A list of parts.
 **/
G_GNUC_INTERNAL JList* j_batch_get_operations(JBatch* batch);

/**
 * Executes the batch.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
G_GNUC_INTERNAL gboolean j_batch_execute_internal(JBatch* batch);

/**
 * @}
 **/

G_END_DECLS

#endif
