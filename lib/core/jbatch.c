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

#include <julea-config.h>

#include <glib.h>

#include <jbatch.h>
#include <jbatch-internal.h>

#include <jbackground-operation.h>
#include <jcache.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation-cache-internal.h>
#include <joperation.h>
#include <jsemantics.h>
#include <jtrace.h>

/**
 * \addtogroup JBatch Batch
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

struct JBatchAsync
{
	JBatch* batch;
	JBatchAsyncCallback callback;
	gpointer user_data;
};

typedef struct JBatchAsync JBatchAsync;

static gpointer
j_batch_background_operation(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JBatchAsync* async = data;
	gboolean ret;

	ret = j_batch_execute(async->batch);

	if (async->callback != NULL)
	{
		(*async->callback)(async->batch, ret, async->user_data);
	}

	j_batch_unref(async->batch);

	g_slice_free(JBatchAsync, async);

	return NULL;
}

JBatch*
j_batch_new(JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	JBatch* batch;

	g_return_val_if_fail(semantics != NULL, NULL);

	batch = g_slice_new(JBatch);
	batch->list = j_list_new((JListFreeFunc)j_operation_free);
	batch->semantics = j_semantics_ref(semantics);
	batch->background_operation = NULL;
	batch->ref_count = 1;

	return batch;
}

JBatch*
j_batch_new_for_template(JSemanticsTemplate template)
{
	J_TRACE_FUNCTION(NULL);

	JBatch* batch;
	g_autoptr(JSemantics) semantics = NULL;

	semantics = j_semantics_new(template);
	batch = j_batch_new(semantics);

	return batch;
}

JBatch*
j_batch_ref(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(batch != NULL, NULL);

	g_atomic_int_inc(&(batch->ref_count));

	return batch;
}

void
j_batch_unref(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(batch != NULL);

	if (g_atomic_int_dec_and_test(&(batch->ref_count)))
	{
		gboolean is_session;

		is_session = j_semantics_get(batch->semantics, J_SEMANTICS_CONSISTENCY) == J_SEMANTICS_CONSISTENCY_SESSION;

		if (is_session)
		{
			// Freeing the batch ends the current session
			j_batch_execute_internal(batch);
		}

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
}

/**
 * Executes the batch parts of a given batch type.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param batch A batch.
 * \param exec_func A function which executes the batch part.
 * \param list A list of batch parts.
 *
 * \return TRUE on success, FALSE if an error occurred.
 **/
static gboolean
j_batch_execute_same(JBatch* batch, JOperationExecFunc exec_func, JList* list)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;
	gboolean ret = FALSE;

	operation = j_list_get_first(list);

	if (operation == NULL)
	{
		return FALSE;
	}

	if (exec_func != NULL)
	{
		ret = exec_func(list, batch->semantics);
	}

	j_list_delete_all(list);

	return ret;
}

gboolean
j_batch_execute(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;
	JSemanticsConsistency consistency;

	g_return_val_if_fail(batch != NULL, FALSE);

	consistency = j_semantics_get(batch->semantics, J_SEMANTICS_CONSISTENCY);

	if (j_list_length(batch->list) == 0)
	{
		return ret;
	}

	switch (consistency)
	{
		case J_SEMANTICS_CONSISTENCY_EVENTUAL:
			// Try to cache the operation
			if (!j_operation_cache_add(batch))
			{
				// Fallback for operations that cannot be cached
				// Necessary flushes were handled in j_operation_cache_test
				ret = j_batch_execute_internal(batch);
				j_list_delete_all(batch->list);
			}
			else
			{
				ret = TRUE;
			}
			break;

		case J_SEMANTICS_CONSISTENCY_SESSION:
			// Execute when batch gets freed
			ret = TRUE;
			break;

		case J_SEMANTICS_CONSISTENCY_IMMEDIATE:
			// Sync point for eventual batches
			j_operation_cache_flush();

			ret = j_batch_execute_internal(batch);
			j_list_delete_all(batch->list);

			break;

		default:
			g_warn_if_reached();
	}

	return ret;
}

void
j_batch_execute_async(JBatch* batch, JBatchAsyncCallback callback, gpointer user_data)
{
	J_TRACE_FUNCTION(NULL);

	JBatchAsync* async;

	g_return_if_fail(batch != NULL);
	g_return_if_fail(batch->background_operation == NULL);

	async = g_slice_new(JBatchAsync);
	async->batch = j_batch_ref(batch);
	async->callback = callback;
	async->user_data = user_data;

	batch->background_operation = j_background_operation_new(j_batch_background_operation, async);
}

void
j_batch_wait(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(batch != NULL);

	if (batch->background_operation != NULL)
	{
		j_background_operation_wait(batch->background_operation);
		j_background_operation_unref(batch->background_operation);
		batch->background_operation = NULL;
	}
}

JSemantics*
j_batch_get_semantics(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(batch != NULL, NULL);

	return batch->semantics;
}

void
j_batch_add(JBatch* batch, JOperation* operation)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(batch != NULL);
	g_return_if_fail(operation != NULL);

	j_list_append(batch->list, operation);
}

/* Internal */

JBatch*
j_batch_new_from_batch(JBatch* old_batch)
{
	J_TRACE_FUNCTION(NULL);

	JBatch* batch;

	g_return_val_if_fail(old_batch != NULL, NULL);

	batch = g_slice_new(JBatch);
	batch->list = old_batch->list;
	batch->semantics = j_semantics_ref(old_batch->semantics);
	batch->background_operation = NULL;
	batch->ref_count = 1;

	old_batch->list = j_list_new((JListFreeFunc)j_operation_free);

	return batch;
}

JList*
j_batch_get_operations(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(batch != NULL, NULL);

	return batch->list;
}

gboolean
j_batch_execute_internal(JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JList) same_list = NULL;
	g_autoptr(JListIterator) iterator = NULL;
	JOperationExecFunc last_exec_func;
	gconstpointer last_key;
	gboolean ret = TRUE;

	iterator = j_list_iterator_new(batch->list);
	same_list = j_list_new(NULL);
	last_key = NULL;
	last_exec_func = NULL;

	/** \todo Perform some (reordering) optimizations
		* It is important to consider dependencies:
		* - Operations have to be performed before their dependent ones.
		*   For example, a collection has to be created before items in it can be created.
		*/

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

	return ret;
}

/**
 * @}
 **/
