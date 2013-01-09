/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <joperation.h>
#include <joperation-internal.h>

#include <jbackground-operation-internal.h>
#include <jcache-internal.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
#include <jitem-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation-cache-internal.h>
#include <joperation-part-internal.h>
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

	/**
	 * The semantics.
	 **/
	JSemantics* semantics;

	/**
	 * The background operation used for j_operation_execute_async().
	 **/
	JBackgroundOperation* background_operation;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

struct JOperationAsync
{
	JOperation* operation;
	JOperationCompletedFunc func;
	gpointer user_data;
};

typedef struct JOperationAsync JOperationAsync;

static
gpointer
j_operation_background_operation (gpointer data)
{
	JOperationAsync* async = data;
	gboolean ret;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	ret = j_operation_execute(async->operation);

	if (async->func != NULL)
	{
		(*async->func)(async->operation, ret, async->user_data);
	}

	j_operation_unref(async->operation);

	g_slice_free(JOperationAsync, async);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return NULL;
}

/**
 * Creates a new operation.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param semantics A semantics object.
 *
 * \return A new operation. Should be freed with j_operation_unref().
 **/
JOperation*
j_operation_new (JSemantics* semantics)
{
	JOperation* operation;

	g_return_val_if_fail(semantics != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	operation = g_slice_new(JOperation);
	operation->list = j_list_new((JListFreeFunc)j_operation_part_free);
	operation->semantics = j_semantics_ref(semantics);
	operation->background_operation = NULL;
	operation->ref_count = 1;

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return operation;
}

/**
 * Creates a new operation for a semantics template.
 *
 * \author Michael Kuhn
 *
 * \code
 * JOperation* operation;
 *
 * operation = j_operation_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
 * \endcode
 *
 * \param template A semantics template.
 *
 * \return A new operation. Should be freed with j_operation_unref().
 **/
JOperation*
j_operation_new_for_template (JSemanticsTemplate template)
{
	JOperation* operation;
	JSemantics* semantics;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	semantics = j_semantics_new(template);
	operation = j_operation_new(semantics);
	j_semantics_unref(semantics);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return operation;
}

/**
 * Increases the operation's reference count.
 *
 * \author Michael Kuhn
 *
 * \param list An operation.
 *
 * \return The operation.
 **/
JOperation*
j_operation_ref (JOperation* operation)
{
	g_return_val_if_fail(operation != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	g_atomic_int_inc(&(operation->ref_count));

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return operation;
}

/**
 * Decreases the operation's reference count.
 * When the reference count reaches zero, frees the memory allocated for the operation.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param operation An operation.
 **/
void
j_operation_unref (JOperation* operation)
{
	g_return_if_fail(operation != NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(operation->ref_count)))
	{
		if (operation->background_operation != NULL)
		{
			j_background_operation_unref(operation->background_operation);
		}

		if (operation->semantics != NULL)
		{
			j_semantics_unref(operation->semantics);
		}

		j_list_unref(operation->list);

		g_slice_free(JOperation, operation);
	}

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * Executes the operation parts of a given operation type.
 *
 * \private
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
gboolean
j_operation_execute_same (JOperation* operation, JList* list)
{
	JOperationPart* part;
	JOperationType type;
	gboolean ret = FALSE;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	part = j_list_get_first(list);

	if (part == NULL)
	{
		goto end;
	}

	type = part->type;

	switch (type)
	{
		case J_OPERATION_CREATE_STORE:
			ret = j_create_store_internal(operation, list);
			break;
		case J_OPERATION_DELETE_STORE:
			ret = j_delete_store_internal(operation, list);
			break;
		case J_OPERATION_GET_STORE:
			ret = j_get_store_internal(operation, list);
			break;
		case J_OPERATION_STORE_CREATE_COLLECTION:
			ret = j_store_create_collection_internal(operation, list);
			break;
		case J_OPERATION_STORE_DELETE_COLLECTION:
			ret = j_store_delete_collection_internal(operation, list);
			break;
		case J_OPERATION_STORE_GET_COLLECTION:
			ret = j_store_get_collection_internal(operation, list);
			break;
		case J_OPERATION_COLLECTION_CREATE_ITEM:
			ret = j_collection_create_item_internal(operation, list);
			break;
		case J_OPERATION_COLLECTION_DELETE_ITEM:
			ret = j_collection_delete_item_internal(operation, list);
			break;
		case J_OPERATION_COLLECTION_GET_ITEM:
			ret = j_collection_get_item_internal(operation, list);
			break;
		case J_OPERATION_ITEM_GET_STATUS:
			ret = j_item_get_status_internal(operation, list);
			break;
		case J_OPERATION_ITEM_READ:
			ret = j_item_read_internal(operation, list);
			break;
		case J_OPERATION_ITEM_WRITE:
			ret = j_item_write_internal(operation, list);
			break;
		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	j_list_delete_all(list);

end:
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
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
	gboolean ret;

	g_return_val_if_fail(operation != NULL, FALSE);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	if (j_list_length(operation->list) == 0)
	{
		ret = FALSE;
		goto end;
	}

	if (j_semantics_get(operation->semantics, J_SEMANTICS_CONSISTENCY) == J_SEMANTICS_CONSISTENCY_EVENTUAL
	    && j_operation_cache_add(operation))
	{
		ret = TRUE;
		goto end;
	}

	j_operation_cache_flush();

	ret = j_operation_execute_internal(operation);

end:
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
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
	g_return_if_fail(operation->background_operation == NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	async = g_slice_new(JOperationAsync);
	async->operation = j_operation_ref(operation);
	async->func = func;
	async->user_data = user_data;

	operation->background_operation = j_background_operation_new(j_operation_background_operation, async);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
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

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/* Internal */

/**
 * Returns an operation's parts.
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
 * \return A list of parts.
 **/
JList*
j_operation_get_parts (JOperation* operation)
{
	JList* ret;

	g_return_val_if_fail(operation != NULL, NULL);

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	ret = operation->list;
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);
	ret = operation->semantics;
	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

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

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	j_list_append(operation->list, part);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);
}

/**
 * Executes the operation.
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
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_operation_execute_internal (JOperation* operation)
{
	JList* same_list;
	JListIterator* iterator;
	JOperationType last_type;
	gpointer last_key;
	gboolean ret = TRUE;

	j_trace_enter(j_trace_get_thread_default(), G_STRFUNC);

	iterator = j_list_iterator_new(operation->list);
	same_list = j_list_new(NULL);
	last_key = NULL;
	last_type = J_OPERATION_NONE;

	while (j_list_iterator_next(iterator))
	{
		JOperationPart* part = j_list_iterator_get(iterator);

		if ((part->type != last_type || part->key != last_key) && last_type != J_OPERATION_NONE)
		{
			ret = j_operation_execute_same(operation, same_list) && ret;
		}

		last_key = part->key;
		last_type = part->type;
		j_list_append(same_list, part);
	}

	ret = j_operation_execute_same(operation, same_list) && ret;

	j_list_unref(same_list);
	j_list_iterator_free(iterator);

	j_list_delete_all(operation->list);

	j_trace_leave(j_trace_get_thread_default(), G_STRFUNC);

	return ret;
}

/**
 * @}
 **/
