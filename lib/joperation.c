/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
