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

#include "jcommon.h"
#include "jcommon-internal.h"

#include "jconfiguration.h"
#include "jconnection.h"
#include "jconnection-internal.h"
#include "jlist.h"
#include "jlist-iterator.h"
#include "joperation.h"
#include "joperation-internal.h"
#include "jstore.h"
#include "jstore-internal.h"
#include "jtrace.h"

/**
 * \defgroup JCommon Common
 * @{
 **/

static JConfiguration* j_configuration_global = NULL;
static JTrace* j_trace_global = NULL;

gboolean
j_init (void)
{
	g_return_val_if_fail(!j_is_initialized(), FALSE);

	return j_init_for_data(NULL);
}

gboolean
j_init_for_data (GKeyFile* key_file)
{
	JConfiguration* conf;

	g_return_val_if_fail(!j_is_initialized(), FALSE);

	j_trace_init("JULEA");

	g_atomic_pointer_set(&j_trace_global, j_trace_thread_enter(NULL, G_STRFUNC));

	if (key_file != NULL)
	{
		conf = j_configuration_new_for_data(key_file);
	}
	else
	{
		conf = j_configuration_new();
	}

	g_atomic_pointer_set(&j_configuration_global, conf);

	return j_is_initialized();
}

gboolean
j_fini (void)
{
	JConfiguration* conf;
	JTrace* trace;

	g_return_val_if_fail(j_is_initialized(), FALSE);

	conf = g_atomic_pointer_get(&j_configuration_global);
	g_atomic_pointer_set(&j_configuration_global, NULL);

	trace = g_atomic_pointer_get(&j_trace_global);
	g_atomic_pointer_set(&j_trace_global, NULL);

	j_configuration_free(conf);
	j_trace_thread_leave(trace);

	j_trace_fini();

	return TRUE;
}

gboolean
j_is_initialized (void)
{
	JConfiguration* p;

	p = g_atomic_pointer_get(&j_configuration_global);

	return (p != NULL);
}

JConfiguration*
j_configuration (void)
{
	JConfiguration* p;

	g_return_val_if_fail(j_is_initialized(), NULL);

	p = g_atomic_pointer_get(&j_configuration_global);

	return p;
}

JTrace*
j_trace (void)
{
	JTrace* p;

	g_return_val_if_fail(j_is_initialized(), NULL);

	p = g_atomic_pointer_get(&j_trace_global);

	return p;
}

void
j_add_store (JConnection* connection, JStore* store, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_ADD_STORE;
	part->u.add_store.connection = j_connection_ref(connection);
	part->u.add_store.store = j_store_ref(store);

	j_operation_add(operation, part);
}

void
j_delete_store (JConnection* connection, JStore* store, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_DELETE_STORE;
	part->u.delete_store.connection = j_connection_ref(connection);
	part->u.delete_store.store = j_store_ref(store);

	j_operation_add(operation, part);
}

void
j_get_store (JConnection* connection, JStore** store, gchar const* name, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	part = g_slice_new(JOperationPart);
	part->type = J_OPERATION_GET_STORE;
	part->u.get_store.connection = j_connection_ref(connection);
	part->u.get_store.store = store;
	part->u.get_store.name = g_strdup(name);

	j_operation_add(operation, part);
}

/* Internal */

void
j_add_store_internal (JList* parts)
{
	JListIterator* it;

	g_return_if_fail(parts != NULL);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JConnection* connection = part->u.delete_store.connection;
		JStore* store = part->u.add_store.store;

		j_store_set_connection(store, connection);
		//store = j_store_new();
	}

	j_list_iterator_free(it);
}

void
j_delete_store_internal (JList* parts)
{
	JListIterator* it;

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
j_get_store_internal (JList* parts)
{
	JListIterator* it;

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
