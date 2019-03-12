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

#include <jbatch.h>
#include <jbatch-internal.h>

#include <jbackground-operation.h>
#include <jcache.h>
#include <jcommon.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation-cache-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

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

	j_trace_enter(G_STRFUNC, NULL);

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

	j_trace_enter(G_STRFUNC, NULL);

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
	g_autoptr(JSemantics) semantics = NULL;

	j_trace_enter(G_STRFUNC, NULL);

	semantics = j_semantics_new(template);
	batch = j_batch_new(semantics);

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

	j_trace_enter(G_STRFUNC, NULL);

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

	j_trace_enter(G_STRFUNC, NULL);

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
j_batch_execute_same (JBatch* batch, JOperationExecFunc exec_func, JList* list)
{
	JOperation* operation;
	gboolean ret = FALSE;

	j_trace_enter(G_STRFUNC, NULL);

	operation = j_list_get_first(list);

	if (operation == NULL)
	{
		goto end;
	}

	if (exec_func != NULL)
	{
		ret = exec_func(list, batch->semantics);
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

	j_trace_enter(G_STRFUNC, NULL);

	if (j_list_length(batch->list) == 0)
	{
		ret = FALSE;
		goto end;
	}

	if (j_semantics_get(batch->semantics, J_SEMANTICS_PERSISTENCY) == J_SEMANTICS_PERSISTENCY_EVENTUAL
	    && j_operation_cache_add(batch))
	{
		ret = TRUE;
		goto end;
	}

	j_operation_cache_flush();

	ret = j_batch_execute_internal(batch);
	j_list_delete_all(batch->list);

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

	j_trace_enter(G_STRFUNC, NULL);

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
j_batch_new_from_batch (JBatch* old_batch)
{
	JBatch* batch;

	g_return_val_if_fail(old_batch != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	batch = g_slice_new(JBatch);
	batch->list = old_batch->list;
	batch->semantics = j_semantics_ref(old_batch->semantics);
	batch->background_operation = NULL;
	batch->ref_count = 1;

	old_batch->list = j_list_new((JListFreeFunc)j_operation_free);

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

	j_trace_enter(G_STRFUNC, NULL);
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

	j_trace_enter(G_STRFUNC, NULL);
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

	j_trace_enter(G_STRFUNC, NULL);

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
	g_autoptr(JList) same_list = NULL;
	g_autoptr(JListIterator) iterator = NULL;
	JOperationExecFunc last_exec_func;
	gpointer last_key;
	gboolean ret = TRUE;

	j_trace_enter(G_STRFUNC, NULL);

	iterator = j_list_iterator_new(batch->list);
	same_list = j_list_new(NULL);
	last_key = NULL;
	last_exec_func = NULL;

	if (j_semantics_get(batch->semantics, J_SEMANTICS_ORDERING) == J_SEMANTICS_ORDERING_RELAXED)
	{
		/* FIXME: perform some optimizations */
		/**
		 * It is important to consider dependencies:
		 * - Operations have to be performed before their dependent ones.
		 *   For example, a collection has to be created before items in it can be created.
		 */
	}

	/**
	 * Try to combine as many operations of the same type as possible.
	 * These are temporarily stored in same_list.
	 */
	while (j_list_iterator_next(iterator))
	{
		JOperation* operation = j_list_iterator_get(iterator);

		/* We only combine operations with the same type and the same key. */
		if ((operation->exec_func != last_exec_func || operation->key != last_key) && last_exec_func != NULL)
		{
			ret = j_batch_execute_same(batch, last_exec_func, same_list) && ret;
		}

		last_key = operation->key;
		last_exec_func = operation->exec_func;
		j_list_append(same_list, operation->data);
	}

	ret = j_batch_execute_same(batch, last_exec_func, same_list) && ret;

	j_trace_leave(G_STRFUNC);

	return ret;
}

/**
 * @}
 **/
