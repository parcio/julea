/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2022 Timm Leon Erxleben
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

#include "sql-generic-internal.h"

gboolean
_backend_batch_start(gpointer backend_data, JSqlBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	g_return_val_if_fail(!batch->open, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!specs->func.transaction_start(thread_variables->db_connection, error))
	{
		goto _error;
	}

	batch->open = TRUE;
	batch->aborted = FALSE;

	return TRUE;

_error:
	return FALSE;
}

gboolean
_backend_batch_execute(gpointer backend_data, JSqlBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	g_return_val_if_fail(batch->open || (!batch->open && batch->aborted), FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!specs->func.transaction_commit(thread_variables->db_connection, error))
	{
		goto _error;
	}

	batch->open = FALSE;

	return TRUE;

_error:
	return FALSE;
}

gboolean
_backend_batch_abort(gpointer backend_data, JSqlBatch* batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JThreadVariables* thread_variables = NULL;

	g_return_val_if_fail(batch->open, FALSE);

	if (G_UNLIKELY(!(thread_variables = thread_variables_get(backend_data, error))))
	{
		goto _error;
	}

	if (!specs->func.transaction_abort(thread_variables->db_connection, error))
	{
		goto _error;
	}

	batch->open = FALSE;
	batch->aborted = TRUE;

	return TRUE;

_error:
	return FALSE;
}

gboolean
sql_generic_batch_start(gpointer backend_data, gchar const* namespace, JSemantics* semantics, gpointer* _batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);
	g_return_val_if_fail(_batch != NULL, FALSE);

	if (specs->single_threaded)
	{
		G_LOCK(sql_backend_lock);
	}

	batch = *_batch = g_new(JSqlBatch, 1);
	batch->namespace = namespace;
	batch->semantics = j_semantics_ref(semantics);
	batch->open = FALSE;
	batch->aborted = FALSE;

	if (G_UNLIKELY(!_backend_batch_start(backend_data, batch, error)))
	{
		goto _error;
	}

	return TRUE;

_error:
	j_semantics_unref(batch->semantics);
	g_free(batch);

	if (specs->single_threaded)
		G_UNLOCK(sql_backend_lock);

	return FALSE;
}

gboolean
sql_generic_batch_execute(gpointer backend_data, gpointer _batch, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JSqlBatch* batch = _batch;

	g_return_val_if_fail(batch != NULL, FALSE);

	if (G_UNLIKELY(!_backend_batch_execute(backend_data, batch, error)))
	{
		goto _error;
	}

	j_semantics_unref(batch->semantics);
	g_free(batch);

	if (specs->single_threaded)
		G_UNLOCK(sql_backend_lock);

	return TRUE;

_error:
	j_semantics_unref(batch->semantics);
	g_free(batch);

	if (specs->single_threaded)
		G_UNLOCK(sql_backend_lock);

	return FALSE;
}
