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

#include <bson.h>
#include <mongo.h>

#include <jcommon.h>
#include <jcommon-internal.h>

#include <jbackground-operation-internal.h>
#include <jconfiguration-internal.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation.h>
#include <joperation-internal.h>
#include <jstore.h>
#include <jstore-internal.h>
#include <jtrace.h>

/**
 * \defgroup JCommon Common
 * @{
 **/

struct JCommon
{
	JConfiguration* configuration;
	JConnection* connection;
	JTrace* trace;
};

static JCommon* j_common = NULL;

gboolean
j_init (gint* argc, gchar*** argv)
{
	JCommon* common;
	gchar* basename;

	g_return_val_if_fail(!j_is_initialized(), FALSE);

	common = g_slice_new(JCommon);
	common->configuration = NULL;
	common->connection = NULL;
	common->trace = NULL;

	g_type_init();

	basename = g_path_get_basename((*argv)[0]);
	j_trace_init(basename);
	g_free(basename);

	common->trace = j_trace_thread_enter(NULL, G_STRFUNC);
	common->configuration = j_configuration_new();

	if (common->configuration == NULL)
	{
		goto error;
	}

	common->connection = j_connection_new(common->configuration);

	if (!j_connection_connect(common->connection))
	{
		goto error;
	}

	j_background_operation_init();

	g_atomic_pointer_set(&j_common, common);

	return TRUE;

error:
	if (common->connection != NULL)
	{
		j_connection_unref(common->connection);
	}

	if (common->configuration != NULL)
	{
		j_configuration_unref(common->configuration);
	}

	j_trace_thread_leave(common->trace);

	j_trace_fini();

	g_slice_free(JCommon, common);

	return FALSE;
}

void
j_fini (void)
{
	JCommon* common;

	g_return_if_fail(j_is_initialized());

	common = g_atomic_pointer_get(&j_common);
	g_atomic_pointer_set(&j_common, NULL);

	j_background_operation_fini();

	j_connection_disconnect(common->connection);

	j_connection_unref(common->connection);
	j_configuration_unref(common->configuration);
	j_trace_thread_leave(common->trace);

	j_trace_fini();

	g_slice_free(JCommon, common);
}

gboolean
j_is_initialized (void)
{
	JCommon* p;

	p = g_atomic_pointer_get(&j_common);

	return (p != NULL);
}

JConfiguration*
j_configuration (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->configuration;
}

JTrace*
j_trace (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->trace;
}

void
j_create_store (JStore* store, JOperation* operation)
{
	JCommon* common;
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	common = g_atomic_pointer_get(&j_common);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_CREATE_STORE;
	part->u.create_store.connection = j_connection_ref(common->connection);
	part->u.create_store.store = j_store_ref(store);

	j_operation_add(operation, part);
}

void
j_delete_store (JStore* store, JOperation* operation)
{
	JCommon* common;
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	common = g_atomic_pointer_get(&j_common);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_DELETE_STORE;
	part->u.delete_store.connection = j_connection_ref(common->connection);
	part->u.delete_store.store = j_store_ref(store);

	j_operation_add(operation, part);
}

void
j_get_store (JStore** store, gchar const* name, JOperation* operation)
{
	JCommon* common;
	JOperationPart* part;

	g_return_if_fail(store != NULL);
	g_return_if_fail(name != NULL);

	common = g_atomic_pointer_get(&j_common);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_GET_STORE;
	part->u.get_store.connection = j_connection_ref(common->connection);
	part->u.get_store.store = store;
	part->u.get_store.name = g_strdup(name);

	j_operation_add(operation, part);
}

/* Internal */

void
j_create_store_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_if_fail(operation != NULL);
	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JConnection* connection = part->u.delete_store.connection;
		JStore* store = part->u.create_store.store;

		j_store_set_connection(store, connection);
		//store = j_store_new();
	}

	j_list_iterator_free(it);
}

void
j_delete_store_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_if_fail(operation != NULL);
	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JConnection* connection = part->u.delete_store.connection;
		JStore* store = part->u.delete_store.store;

		mongo_cmd_drop_db(j_connection_get_connection(connection), j_store_get_name(store));
	}

	j_list_iterator_free(it);
}

void
j_get_store_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_if_fail(operation != NULL);
	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JConnection* connection = part->u.delete_store.connection;
		JStore** store = part->u.get_store.store;
		gchar const* name = part->u.get_store.name;

		*store = j_store_new(name);
		j_store_set_connection(*store, connection);
	}

	j_list_iterator_free(it);
}

/**
 * @}
 **/
