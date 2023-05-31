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

#ifndef JULEA_BATCH_H
#define JULEA_BATCH_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JBatch Batch
 *
 * @{
 **/

struct JBatch;

typedef struct JBatch JBatch;

typedef void (*JBatchAsyncCallback)(JBatch*, gboolean, gpointer);

G_END_DECLS

#include <core/joperation.h>
#include <core/jsemantics.h>

G_BEGIN_DECLS

/**
 * Creates a new batch.
 *
 * \code
 * \endcode
 *
 * \param semantics A semantics object.
 *
 * \return A new batch. Should be freed with j_batch_unref().
 **/
JBatch* j_batch_new(JSemantics* semantics);

/**
 * Creates a new batch for a semantics template.
 *
 * \code
 * JBatch* batch;
 *
 * batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
 * \endcode
 *
 * \param template A semantics template.
 *
 * \return A new batch. Should be freed with j_batch_unref().
 **/
JBatch* j_batch_new_for_template(JSemanticsTemplate template);

/**
 * Increases the batch's reference count.
 *
 * \param batch A batch.
 *
 * \return The batch.
 **/
JBatch* j_batch_ref(JBatch* batch);

/**
 * Decreases the batch's reference count.
 * When the reference count reaches zero, frees the memory allocated for the batch.
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 **/
void j_batch_unref(JBatch* batch);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JBatch, j_batch_unref)

/**
 * Returns a batch's semantics.
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return A semantics object.
 **/
JSemantics* j_batch_get_semantics(JBatch* batch);

/**
 * Adds a new operation to the batch.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param batch     A batch.
 * \param operation An operation.
 **/
void j_batch_add(JBatch* batch, JOperation* operation);

/**
 * Executes the batch.
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean j_batch_execute(JBatch* batch) G_GNUC_WARN_UNUSED_RESULT;

/**
 * Executes the batch asynchronously.
 *
 *
 * \param batch     A batch.
 * \param callback  An async callback.
 * \param user_data Arguments specified by the user for the callback.
 *
 * \attention Consistency as defined by the batch's semantics is ignored by choosing explicit asynchronous execution.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
void j_batch_execute_async(JBatch* batch, JBatchAsyncCallback callback, gpointer user_data);

/**
 * Wait for an asynchronous batch to finish.
 *
 * \code
 * \endcode
 *
 * \param batch     A batch.
 *
 **/
void j_batch_wait(JBatch* batch);

/**
 * @}
 **/

G_END_DECLS

#endif
