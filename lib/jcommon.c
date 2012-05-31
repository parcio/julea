/*
 * Copyright (c) 2010-2012 Michael Kuhn
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
#include <jconfiguration.h>
#include <jconnection.h>
#include <jconnection-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <joperation.h>
#include <joperation-internal.h>
#include <joperation-cache-internal.h>
#include <joperation-part-internal.h>
#include <jstore.h>
#include <jstore-internal.h>
#include <jthread-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JCommon Common
 * @{
 **/

/**
 * Common structure.
 */
struct JCommon
{
	/**
	 * The configuration.
	 */
	JConfiguration* configuration;

	/**
	 * The connection.
	 */
	JConnection* connection;
};

static JCommon* j_common = NULL;
static JThread* j_thread_global = NULL;

/**
 * Returns whether JULEA has been initialized.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \return TRUE if JULEA has been initialized, FALSE otherwise.
 */
static
gboolean
j_is_initialized (void)
{
	JCommon* p;

	p = g_atomic_pointer_get(&j_common);

	return (p != NULL);
}

/**
 * Initializes JULEA.
 *
 * \author Michael Kuhn
 *
 * \param argc A pointer to \c argc.
 * \param argv A pointer to \c argv.
 */
void
j_init (gint* argc, gchar*** argv)
{
	JCommon* common;
	JThread* thread;
	gchar* basename;

	g_return_if_fail(!j_is_initialized());

	common = g_slice_new(JCommon);
	common->configuration = NULL;
	common->connection = NULL;

	g_type_init();

	basename = g_path_get_basename((*argv)[0]);
	j_trace_init(basename);
	g_free(basename);

	thread = j_thread_new(NULL, "main");
	g_atomic_pointer_set(&j_thread_global, thread);

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
	j_operation_cache_init();

	g_atomic_pointer_set(&j_common, common);

	return;

error:
	if (common->connection != NULL)
	{
		j_connection_unref(common->connection);
	}

	if (common->configuration != NULL)
	{
		j_configuration_unref(common->configuration);
	}

	g_atomic_pointer_set(&j_thread_global, NULL);

	j_thread_free(thread);

	j_trace_fini();

	g_slice_free(JCommon, common);

	g_error("%s: Failed to initialize JULEA.", G_STRLOC);
}

/**
 * Shuts down JULEA.
 *
 * \author Michael Kuhn
 */
void
j_fini (void)
{
	JCommon* common;
	JThread* thread;

	g_return_if_fail(j_is_initialized());

	common = g_atomic_pointer_get(&j_common);
	g_atomic_pointer_set(&j_common, NULL);

	j_operation_cache_fini();
	j_background_operation_fini();

	j_connection_disconnect(common->connection);

	j_connection_unref(common->connection);
	j_configuration_unref(common->configuration);

	thread = g_atomic_pointer_get(&j_thread_global);
	g_atomic_pointer_set(&j_thread_global, NULL);

	j_thread_free(thread);

	j_trace_fini();

	g_slice_free(JCommon, common);
}

/**
 * Creates a store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store      A store.
 * \param operation  An operation.
 **/
void
j_create_store (JStore* store, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	part = j_operation_part_new(J_OPERATION_CREATE_STORE);
	part->key = NULL;
	part->u.create_store.store = j_store_ref(store);

	j_operation_add(operation, part);
}

/**
 * Deletes a store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store      A store.
 * \param operation  An operation.
 **/
void
j_delete_store (JStore* store, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);

	part = j_operation_part_new(J_OPERATION_DELETE_STORE);
	part->key = NULL;
	part->u.delete_store.store = j_store_ref(store);

	j_operation_add(operation, part);
}

/**
 * Gets a store.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param store     A pointer to a store.
 * \param name      A name.
 * \param operation An operation.
 **/
void
j_get_store (JStore** store, gchar const* name, JOperation* operation)
{
	JOperationPart* part;

	g_return_if_fail(store != NULL);
	g_return_if_fail(name != NULL);

	part = j_operation_part_new(J_OPERATION_GET_STORE);
	part->key = NULL;
	part->u.get_store.store = store;
	part->u.get_store.name = g_strdup(name);

	j_operation_add(operation, part);
}

/* Internal */

/**
 * Returns the configuration.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \return The configuration.
 */
JConfiguration*
j_configuration (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->configuration;
}

/**
 * Returns the connection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \return The connection.
 */
JConnection*
j_connection (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->connection;
}

/**
 * Creates stores.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param operation An operation.
 * \param parts     A list of operation parts.
 *
 * \return TRUE.
 */
gboolean
j_create_store_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JStore* store = part->u.create_store.store;

		(void)store;
		//store = j_store_new();
	}

	j_list_iterator_free(it);

	return TRUE;
}

/**
 * Deletes stores.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param operation An operation.
 * \param parts     A list of operation parts.
 *
 * \return TRUE.
 */
gboolean
j_delete_store_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JStore* store = part->u.delete_store.store;

		mongo_cmd_drop_db(j_connection_get_connection(j_store_get_connection(store)), j_store_get_name(store));
	}

	j_list_iterator_free(it);

	return TRUE;
}

/**
 * Gets stores.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param operation An operation.
 * \param parts     A list of operation parts.
 *
 * \return TRUE.
 */
gboolean
j_get_store_internal (JOperation* operation, JList* parts)
{
	JListIterator* it;

	g_return_val_if_fail(operation != NULL, FALSE);
	g_return_val_if_fail(parts != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(parts);

	while (j_list_iterator_next(it))
	{
		JOperationPart* part = j_list_iterator_get(it);
		JStore** store = part->u.get_store.store;
		gchar const* name = part->u.get_store.name;

		*store = j_store_new(name);
	}

	j_list_iterator_free(it);

	return TRUE;
}

/**
 * @}
 **/
