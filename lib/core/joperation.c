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

#include <julea-config.h>

#include <glib.h>

#include <joperation-internal.h>

#include <jtrace.h>

/**
 * \defgroup JOperation Operation
 *
 * @{
 **/

/**
 * Creates a new operation.
 *
 * \code
 * \endcode
 *
 * \return A new operation. Should be freed with j_operation_free().
 **/
JOperation*
j_operation_new (void)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;

	operation = g_slice_new(JOperation);
	operation->key = NULL;
	operation->data = NULL;
	operation->exec_func = NULL;
	operation->free_func = NULL;

	return operation;
}

/**
 * Frees the memory allocated by an operation.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param data An operation.
 **/
/* FIXME */
void
j_operation_free (JOperation* operation)
{
	J_TRACE_FUNCTION(NULL);

	if (operation->free_func != NULL)
	{
		operation->free_func(operation->data);
	}

	g_slice_free(JOperation, operation);
}

/**
 * @}
 **/
