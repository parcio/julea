/*
 * Copyright (c) 2010-2011 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <glib.h>

#include "joperation.h"
#include "joperation-internal.h"

#include "jcollection-internal.h"
#include "jitem-internal.h"
#include "jlist.h"
#include "jlist-iterator.h"
#include "jstore-internal.h"

/**
 * \defgroup JOperation Operation
 *
 * @{
 **/

/**
 * An operation.
 **/
struct JOperation
{
	JList* list;
};

/**
 * Frees an operation part.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param data An operation part.
 **/
/* FIXME */
static
void
j_operation_part_free (gpointer data)
{
	JOperationPart* part = data;

	switch (part->type)
	{
		case J_OPERATION_COLLECTION_CREATE:
			j_collection_unref(part->u.collection_create.collection);
			break;
		case J_OPERATION_COLLECTION_GET:
			j_collection_unref(part->u.collection_get.collection);
			break;
		case J_OPERATION_ITEM_CREATE:
			j_item_unref(part->u.item_create.item);
			break;
		case J_OPERATION_ITEM_GET:
			j_item_unref(part->u.item_get.item);
			break;
		case J_OPERATION_STORE_CREATE:
			j_store_unref(part->u.store_create.store);
			break;
		case J_OPERATION_STORE_GET:
			j_store_unref(part->u.store_get.store);
			break;
		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	g_slice_free(JOperationPart, part);
}

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
j_operation_new (void)
{
	JOperation* operation;

	operation = g_slice_new(JOperation);
	operation->list = j_list_new(j_operation_part_free);

	return operation;
}

/**
 * Frees the memory allocated by the operation.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param operation An operation.
 **/
void
j_operation_free (JOperation* operation)
{
	g_return_if_fail(operation != NULL);

	j_list_unref(operation->list);

	g_slice_free(JOperation, operation);
}

/**
 * Executes the operation parts of a given operation type.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param type An operation type.
 * \param list A list of operation parts.
 **/
static
void
j_operation_execute_internal (JOperationType type, JList* list)
{
	switch (type)
	{
		case J_OPERATION_COLLECTION_CREATE:
			j_collection_create_internal(list);
			break;
		case J_OPERATION_COLLECTION_GET:
			j_collection_get_internal(list);
			break;
		case J_OPERATION_ITEM_CREATE:
			j_item_create_internal(list);
			break;
		case J_OPERATION_ITEM_GET:
			j_item_get_internal(list);
			break;
		case J_OPERATION_STORE_CREATE:
			//j_store_create_internal(list);
			break;
		case J_OPERATION_STORE_GET:
			j_store_get_internal(list);
			break;
		case J_OPERATION_NONE:
		default:
			g_return_if_reached();
	}

	j_list_delete_all(list);
}

/**
 * Executes the operation.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param operation An operation.
 *
 * \return TRUE on success, FALSE if an error occured.
 **/
gboolean
j_operation_execute (JOperation* operation)
{
	JList* same_list;
	JListIterator* iterator;
	JOperationType last_type;

	g_return_val_if_fail(operation != NULL, FALSE);

	if (j_list_length(operation->list) == 0)
	{
		return FALSE;
	}

	iterator = j_list_iterator_new(operation->list);
	same_list = j_list_new(NULL);
	last_type = J_OPERATION_NONE;

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);

		if (part->type != last_type && last_type != J_OPERATION_NONE)
		{
			j_operation_execute_internal(last_type, same_list);
		}

		last_type = part->type;
		j_list_append(same_list, part);
	}

	j_operation_execute_internal(last_type, same_list);

	j_list_unref(same_list);
	j_list_iterator_free(iterator);

	j_list_delete_all(operation->list);

	return TRUE;
}

/* Internal */

/**
 * Adds a new part to the operation.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param operation An operation.
 * \param part      An operation part.
 **/
void
j_operation_add (JOperation* operation, JOperationPart* part)
{
	g_return_if_fail(operation != NULL);
	g_return_if_fail(part != NULL);

	j_list_append(operation->list, part);
}

/**
 * @}
 **/
