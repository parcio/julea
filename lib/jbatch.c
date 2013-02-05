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

#include <jbatch.h>
#include <jbatch-internal.h>

#include <jbackground-operation-internal.h>
#include <jcache-internal.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
#include <jitem-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation-cache-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>
#include <jstore-internal.h>

/**
 * \defgroup JBatch Batch
 *
 * @{
 **/

/**
 * An operation.
 **/
struct JBatch
{
	/**
	 * The list of pending operations.
	 **/
	JList* list;

	/**
	 * The semantics.
	 **/
	JSemantics* semantics;

	/**
	 * The background operation used for j_batch_execute_async().
	 **/
	JBackgroundOperation* background_operation;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

struct JOperationAsync
{
	JBatch* batch;
	JOperationCompletedFunc func;
	gpointer user_data;
};

typedef struct JOperationAsync JOperationAsync;

static
gpointer
j_batch_background_operation (gpointer data)
{
	JOperationAsync* async = data;
	gboolean ret;

	j_trace_enter(G_STRFUNC);

	ret = j_batch_execute(async->batch);

	if (async->func != NULL)
	{
		(*async->func)(async->batch, ret, async->user_data);
	}

	j_batch_unref(async->batch);

	g_slice_free(JOperationAsync, async);

	j_trace_leave(G_STRFUNC);

	return NULL;
}

/**
 * Creates a new batch.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param semantics A semantics object.
 *
 * \return A new batch. Should be freed with j_batch_unref().
 **/
JBatch*
j_batch_new (JSemantics* semantics)
{
	JBatch* batch;

	g_return_val_if_fail(semantics != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	batch = g_slice_new(JBatch);
	batch->list = j_list_new((JListFreeFunc)j_operation_free);
	batch->semantics = j_semantics_ref(semantics);
	batch->background_operation = NULL;
	batch->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return batch;
}

/**
 * Creates a new batch for a semantics template.
 *
 * \author Michael Kuhn
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
JBatch*
j_batch_new_for_template (JSemanticsTemplate template)
{
	JBatch* batch;
	JSemantics* semantics;

	j_trace_enter(G_STRFUNC);

	semantics = j_semantics_new(template);
	batch = j_batch_new(semantics);
	j_semantics_unref(semantics);

	j_trace_leave(G_STRFUNC);

	return batch;
}

/**
 * Increases the batch's reference count.
 *
 * \author Michael Kuhn
 *
 * \param list A batch.
 *
 * \return The batch.
 **/
JBatch*
j_batch_ref (JBatch* batch)
{
	g_return_val_if_fail(batch != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	g_atomic_int_inc(&(batch->ref_count));

	j_trace_leave(G_STRFUNC);

	return batch;
}

/**
 * Decreases the batch's reference count.
 * When the reference count reaches zero, frees the memory allocated for the batch.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 **/
void
j_batch_unref (JBatch* batch)
{
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC);

	if (g_atomic_int_dec_and_test(&(batch->ref_count)))
	{
		if (batch->background_operation != NULL)
		{
			j_background_operation_unref(batch->background_operation);
		}

		if (batch->semantics != NULL)
		{
			j_semantics_unref(batch->semantics);
		}

		j_list_unref(batch->list);

		g_slice_free(JBatch, batch);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Executes the batch parts of a given batch type.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param type An operation type.
 * \param list A list of batch parts.
 **/
static
gboolean
j_batch_execute_same (JBatch* batch, JList* list)
{
	JOperation* operation;
	JOperationType type;
	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC);

	operation = j_list_get_first(list);

	if (operation == NULL)
	{
		goto end;
	}

	type = operation->type;

	switch (type)
	{
		case J_OPERATION_CREATE_STORE:
			ret = j_create_store_internal(batch, list);
			break;
		case J_OPERATION_DELETE_STORE:
			ret = j_delete_store_internal(batch, list);
			break;
		case J_OPERATION_GET_STORE:
			ret = j_get_store_internal(batch, list);
			break;
		case J_OPERATION_STORE_CREATE_COLLECTION:
			ret = j_store_create_collection_internal(batch, list);
			break;
		case J_OPERATION_STORE_DELETE_COLLECTION:
			ret = j_store_delete_collection_internal(batch, list);
			break;
		case J_OPERATION_STORE_GET_COLLECTION:
			ret = j_store_get_collection_internal(batch, list);
			break;
		case J_OPERATION_COLLECTION_CREATE_ITEM:
			ret = j_collection_create_item_internal(batch, list);
			break;
		case J_OPERATION_COLLECTION_DELETE_ITEM:
			ret = j_collection_delete_item_internal(batch, list);
			break;
		case J_OPERATION_COLLECTION_GET_ITEM:
			ret = j_collection_get_item_internal(batch, list);
			break;
		case J_OPERATION_ITEM_GET_STATUS:
			ret = j_item_get_status_internal(batch, list);
			break;
		case J_OPERATION_ITEM_READ:
			ret = j_item_read_internal(batch, list);
			break;
		case J_OPERATION_ITEM_WRITE:
			ret = j_item_write_internal(batch, list);
			break;
		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	j_list_delete_all(list);

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Executes the batch.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_batch_execute (JBatch* batch)
{
	gboolean ret;

	g_return_val_if_fail(batch != NULL, FALSE);

	j_trace_enter(G_STRFUNC);

	if (j_list_length(batch->list) == 0)
	{
		ret = FALSE;
		goto end;
	}

	if (j_semantics_get(batch->semantics, J_SEMANTICS_CONSISTENCY) == J_SEMANTICS_CONSISTENCY_EVENTUAL
	    && j_operation_cache_add(batch))
	{
		batch->list = j_list_new((JListFreeFunc)j_operation_free);
		ret = TRUE;
		goto end;
	}

	j_operation_cache_flush();

	ret = j_batch_execute_internal(batch);

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Executes the batch asynchronously.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch     A batch.
 * \param func      A complete function.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
void
j_batch_execute_async (JBatch* batch, JOperationCompletedFunc func, gpointer user_data)
{
	JOperationAsync* async;

	g_return_if_fail(batch != NULL);
	g_return_if_fail(batch->background_operation == NULL);

	j_trace_enter(G_STRFUNC);

	async = g_slice_new(JOperationAsync);
	async->batch = j_batch_ref(batch);
	async->func = func;
	async->user_data = user_data;

	batch->background_operation = j_background_operation_new(j_batch_background_operation, async);

	j_trace_leave(G_STRFUNC);
}

void
j_batch_wait (JBatch* batch)
{
	g_return_if_fail(batch != NULL);

	if (batch->background_operation != NULL)
	{
		j_background_operation_wait(batch->background_operation);
		j_background_operation_unref(batch->background_operation);
		batch->background_operation = NULL;
	}

	j_trace_leave(G_STRFUNC);
}

/* Internal */

/**
 * Copies a batch.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param old_batch A batch.
 *
 * \return A new batch.
 */
JBatch*
j_batch_copy (JBatch* old_batch)
{
	JBatch* batch;

	g_return_val_if_fail(old_batch != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	batch = g_slice_new(JBatch);
	batch->list = old_batch->list;
	batch->semantics = j_semantics_ref(old_batch->semantics);
	batch->background_operation = NULL;
	batch->ref_count = 1;

	j_trace_leave(G_STRFUNC);

	return batch;
}

/**
 * Returns a batch's parts.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return A list of parts.
 **/
JList*
j_batch_get_operations (JBatch* batch)
{
	JList* ret;

	g_return_val_if_fail(batch != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	ret = batch->list;
	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Returns a batch's semantics.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return A semantics object.
 **/
JSemantics*
j_batch_get_semantics (JBatch* batch)
{
	JSemantics* ret;

	g_return_val_if_fail(batch != NULL, NULL);

	j_trace_enter(G_STRFUNC);
	ret = batch->semantics;
	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * Adds a new operation to the batch.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch     A batch.
 * \param operation An operation.
 **/
void
j_batch_add (JBatch* batch, JOperation* operation)
{
	g_return_if_fail(batch != NULL);
	g_return_if_fail(operation != NULL);

	j_trace_enter(G_STRFUNC);

	j_list_append(batch->list, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * Executes the batch.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
gboolean
j_batch_execute_internal (JBatch* batch)
{
	JList* same_list;
	JListIterator* iterator;
	JOperationType last_type;
	gpointer last_key;
	gboolean ret = TRUE;

	j_trace_enter(G_STRFUNC);

	iterator = j_list_iterator_new(batch->list);
	same_list = j_list_new(NULL);
	last_key = NULL;
	last_type = J_OPERATION_NONE;

	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);

		if ((operation->type != last_type || operation->key != last_key) && last_type != J_OPERATION_NONE)
		{
			ret = j_batch_execute_same(batch, same_list) && ret;
		}

		last_key = operation->key;
		last_type = operation->type;
		j_list_append(same_list, operation);
	}

	ret = j_batch_execute_same(batch, same_list) && ret;

	j_list_unref(same_list);
	j_list_iterator_free(iterator);

	j_list_delete_all(batch->list);

	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * @}
 **/
