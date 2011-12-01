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

#include <joperation.h>
#include <joperation-internal.h>

#include <jbackground-operation-internal.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
#include <jitem-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jsemantics.h>
#include <jstore-internal.h>

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
	/**
	 * The list of pending operation parts.
	 **/
	JList* list;

	JSemantics* semantics;

	JBackgroundOperation* background_operation;
};

struct JOperationAsync
{
	JOperation* operation;
	JOperationCompletedFunc func;
	gpointer user_data;
};

typedef struct JOperationAsync JOperationAsync;

static JSemantics* j_operation_default_semantics = NULL;

static
JSemantics*
j_operation_get_default_semantics (void)
{
	if (G_UNLIKELY(j_operation_default_semantics == NULL))
	{
		j_operation_default_semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	}

	return j_operation_default_semantics;
}

static
gpointer
j_operation_background_operation (gpointer data)
{
	JOperationAsync* async = data;

	j_operation_execute(async->operation);

	if (async->func != NULL)
	{
		(*async->func)(async->operation, async->user_data);
	}

	g_slice_free(JOperationAsync, async);

	return NULL;
}

/**
 * Frees the memory allocated by an operation part.
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
		case J_OPERATION_CREATE_STORE:
			j_connection_unref(part->u.create_store.connection);
			j_store_unref(part->u.create_store.store);
			break;
		case J_OPERATION_GET_STORE:
			j_connection_unref(part->u.get_store.connection);
			g_free(part->u.get_store.name);
			break;
		case J_OPERATION_DELETE_STORE:
			j_connection_unref(part->u.delete_store.connection);
			j_store_unref(part->u.delete_store.store);
			break;
		case J_OPERATION_STORE_CREATE_COLLECTION:
			j_store_unref(part->u.store_create_collection.store);
			j_collection_unref(part->u.store_create_collection.collection);
			break;
		case J_OPERATION_STORE_DELETE_COLLECTION:
			j_store_unref(part->u.store_delete_collection.store);
			j_collection_unref(part->u.store_delete_collection.collection);
			break;
		case J_OPERATION_STORE_GET_COLLECTION:
			j_store_unref(part->u.store_get_collection.store);
			g_free(part->u.store_get_collection.name);
			break;
		case J_OPERATION_COLLECTION_CREATE_ITEM:
			j_collection_unref(part->u.collection_create_item.collection);
			j_item_unref(part->u.collection_create_item.item);
			break;
		case J_OPERATION_COLLECTION_DELETE_ITEM:
			j_collection_unref(part->u.collection_delete_item.collection);
			j_item_unref(part->u.collection_delete_item.item);
			break;
		case J_OPERATION_COLLECTION_GET_ITEM:
			j_collection_unref(part->u.collection_get_item.collection);
			g_free(part->u.collection_get_item.name);
			break;
		case J_OPERATION_ITEM_GET_STATUS:
			j_item_unref(part->u.item_get_status.item);
			break;
		case J_OPERATION_ITEM_READ:
			j_item_unref(part->u.item_read.item);
			break;
		case J_OPERATION_ITEM_WRITE:
			j_item_unref(part->u.item_write.item);
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
j_operation_new (JSemantics* semantics)
{
	JOperation* operation;

	operation = g_slice_new(JOperation);
	operation->list = j_list_new(j_operation_part_free);
	operation->background_operation = NULL;

	if (semantics == NULL)
	{
		semantics = j_operation_get_default_semantics();
	}

	operation->semantics = j_semantics_ref(semantics);

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

	if (operation->semantics != NULL)
	{
		j_semantics_unref(operation->semantics);
	}

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
j_operation_execute_internal (JOperation* operation, JOperationType type, JList* list)
{
	switch (type)
	{
		case J_OPERATION_CREATE_STORE:
			j_create_store_internal(operation, list);
			break;
		case J_OPERATION_GET_STORE:
			j_get_store_internal(operation, list);
			break;
		case J_OPERATION_DELETE_STORE:
			j_delete_store_internal(operation, list);
			break;
		case J_OPERATION_STORE_CREATE_COLLECTION:
			j_store_create_collection_internal(operation, list);
			break;
		case J_OPERATION_STORE_DELETE_COLLECTION:
			j_store_delete_collection_internal(operation, list);
			break;
		case J_OPERATION_STORE_GET_COLLECTION:
			j_store_get_collection_internal(operation, list);
			break;
		case J_OPERATION_COLLECTION_CREATE_ITEM:
			j_collection_create_item_internal(operation, list);
			break;
		case J_OPERATION_COLLECTION_DELETE_ITEM:
			j_collection_delete_item_internal(operation, list);
			break;
		case J_OPERATION_COLLECTION_GET_ITEM:
			j_collection_get_item_internal(operation, list);
			break;
		case J_OPERATION_ITEM_GET_STATUS:
			j_item_get_status_internal(operation, list);
			break;
		case J_OPERATION_ITEM_READ:
			j_item_read_internal(operation, list);
			break;
		case J_OPERATION_ITEM_WRITE:
			j_item_write_internal(operation, list);
			break;
		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}
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
 * \return TRUE on success, FALSE if an error occurred.
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
			j_operation_execute_internal(operation, last_type, same_list);
			j_list_delete_all(same_list);
		}

		last_type = part->type;
		j_list_append(same_list, part);
	}

	j_operation_execute_internal(operation, last_type, same_list);

	j_list_unref(same_list);
	j_list_iterator_free(iterator);

	j_list_delete_all(operation->list);

	return TRUE;
}

/**
 * Executes the operation asynchronously.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param operation An operation.
 * \param func      A complete function.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
void
j_operation_execute_async (JOperation* operation, JOperationCompletedFunc func, gpointer user_data)
{
	JOperationAsync* async;

	g_return_if_fail(operation != NULL);

	async = g_slice_new(JOperationAsync);
	async->operation = operation;
	async->func = func;
	async->user_data = user_data;

	operation->background_operation = j_background_operation_new(j_operation_background_operation, async);
}

void
j_operation_wait (JOperation* operation)
{
	g_return_if_fail(operation != NULL);

	if (operation->background_operation != NULL)
	{
		j_background_operation_wait(operation->background_operation);
		j_background_operation_unref(operation->background_operation);
		operation->background_operation = NULL;
	}
}

/* Internal */

/**
 * Returns an operation's semantics.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param operation An operation.
 *
 * \return A semantics object.
 **/
JSemantics*
j_operation_get_semantics (JOperation* operation)
{
	JSemantics* ret;

	g_return_val_if_fail(operation != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);
	ret = operation->semantics;
	j_trace_leave(j_trace(), G_STRFUNC);

	return ret;
}

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
