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

#ifndef JULEA_BACKGROUND_OPERATION_H
#define JULEA_BACKGROUND_OPERATION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JBackgroundOperation Background Operation
 * @{
 **/

struct JBackgroundOperation;

typedef struct JBackgroundOperation JBackgroundOperation;

typedef gpointer (*JBackgroundOperationFunc)(gpointer);

/**
 * Creates a new background operation.
 *
 * \code
 * static
 * gpointer
 * background_func (gpointer data)
 * {
 *   return NULL;
 * }
 *
 * JBackgroundOperation* background_operation;
 *
 * background_operation = j_background_operation_new(background_func, NULL);
 * \endcode
 *
 * \param func A function to execute in the background.
 * \param data User data given to \p func.
 *
 * \return A new background operation. Should be freed with j_background_operation_unref().
 **/
JBackgroundOperation* j_background_operation_new(JBackgroundOperationFunc func, gpointer data);

/**
 * Increases a background operation's reference count.
 *
 * \code
 * JBackgroundOperation* background_operation;
 *
 * j_background_operation_ref(background_operation);
 * \endcode
 *
 * \param background_operation A background operation.
 *
 * \return \p background_operation.
 **/
JBackgroundOperation* j_background_operation_ref(JBackgroundOperation* background_operation);

/**
 * Decreases a background operation's reference count.
 * When the reference count reaches zero, frees the memory allocated for the background operation.
 *
 * \code
 * JBackgroundOperation* background_operation;
 *
 * j_background_operation_unref(background_operation);
 * \endcode
 *
 * \param background_operation A background operation.
 **/
void j_background_operation_unref(JBackgroundOperation* background_operation);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JBackgroundOperation, j_background_operation_unref)

/**
 * Waits for a background operation to finish.
 *
 * \code
 * JBackgroundOperation* background_operation;
 *
 * j_background_operation_wait(background_operation);
 * \endcode
 *
 * \param background_operation A background operation.
 *
 * \return The return value of the function given to j_background_operation_new().
 **/
gpointer j_background_operation_wait(JBackgroundOperation* background_operation);

/**
 * @}
 **/

G_END_DECLS

#endif
