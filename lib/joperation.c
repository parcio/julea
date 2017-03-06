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

#include <julea-config.h>

#include <glib.h>

#include <joperation-internal.h>

/**
 * \defgroup JOperation Operation
 *
 * @{
 **/

/**
 * Creates a new operation.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new operation. Should be freed with j_operation_free().
 **/
JOperation*
j_operation_new (JOperationType type)
{
	JOperation* operation;

	operation = g_slice_new(JOperation);
	operation->type = type;
	operation->key = NULL;

	return operation;
}

/**
 * Frees the memory allocated by an operation.
 *
 * \private
 *
 * \author Michael Kuhn
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
	switch (operation->type)
	{
		case J_OPERATION_COLLECTION_CREATE:
			j_collection_unref(operation->u.collection_create.collection);
			break;
		case J_OPERATION_COLLECTION_DELETE:
			j_collection_unref(operation->u.collection_delete.collection);
			break;
		case J_OPERATION_COLLECTION_GET:
			g_free(operation->u.collection_get.name);
			break;
		case J_OPERATION_ITEM_CREATE:
			j_collection_unref(operation->u.item_create.collection);
			j_item_unref(operation->u.item_create.item);
			break;
		case J_OPERATION_ITEM_DELETE:
			j_collection_unref(operation->u.item_delete.collection);
			j_item_unref(operation->u.item_delete.item);
			break;
		case J_OPERATION_ITEM_GET:
			j_collection_unref(operation->u.item_get.collection);
			g_free(operation->u.item_get.name);
			break;
		case J_OPERATION_ITEM_GET_STATUS:
			j_item_unref(operation->u.item_get_status.item);
			break;
		case J_OPERATION_ITEM_READ:
			j_item_unref(operation->u.item_read.item);
			break;
		case J_OPERATION_ITEM_WRITE:
			j_item_unref(operation->u.item_write.item);
			break;
		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	g_slice_free(JOperation, operation);
}

/**
 * @}
 **/
