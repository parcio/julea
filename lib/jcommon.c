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

#include <bson.h>
#include <mongoc.h>

#include <jcommon.h>
#include <jcommon-internal.h>

#include <jbackground-operation-internal.h>
#include <jconfiguration-internal.h>
#include <jconnection-pool-internal.h>
#include <jdistribution-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-cache-internal.h>
#include <joperation-internal.h>
#include <jstore.h>
#include <jstore-internal.h>
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
};

static JCommon* j_common = NULL;

/*
static
gint
j_common_oid_fuzz (void)
{
	static GPid pid = 0;

	if (G_UNLIKELY(pid == 0))
	{
		pid = getpid();
	}

	return pid;
}

static
gint
j_common_oid_inc (void)
{
	static gint32 counter = 0;

	return g_atomic_int_add(&counter, 1);
}
*/

static
void
j_common_mongoc_log_handler (mongoc_log_level_t log_level, gchar const* log_domain, gchar const* message, gpointer data)
{
	return;
}

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
 * Returns the program name.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param default_name Default name
 *
 * \return The progran name if it can be determined, default_name otherwise.
 */
static
gchar*
j_get_program_name (gchar const* default_name)
{
	gchar* program_name;

	if ((program_name = g_file_read_link("/proc/self/exe", NULL)) != NULL)
	{
		gchar* basename;

		basename = g_path_get_basename(program_name);
		g_free(program_name);
		program_name = basename;
	}

	if (program_name == NULL)
	{
		program_name = g_strdup(default_name);
	}

	return program_name;
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
j_init (void)
{
	JCommon* common;
	gchar* basename;

	g_return_if_fail(!j_is_initialized());

	common = g_slice_new(JCommon);
	common->configuration = NULL;

	mongoc_init();

	basename = j_get_program_name("julea");
	j_trace_init(basename);
	g_free(basename);

	j_trace_enter(G_STRFUNC);

	common->configuration = j_configuration_new();

	if (common->configuration == NULL)
	{
		goto error;
	}

	j_connection_pool_init(common->configuration);
	j_distribution_init();
	j_background_operation_init(0);
	j_operation_cache_init();

	//bson_set_oid_fuzz(j_common_oid_fuzz);
	//bson_set_oid_inc(j_common_oid_inc);

	/* We do not want the mongoc output. */
	mongoc_log_set_handler(j_common_mongoc_log_handler, NULL);

	g_atomic_pointer_set(&j_common, common);

	j_trace_leave(G_STRFUNC);

	return;

error:
	if (common->configuration != NULL)
	{
		j_configuration_unref(common->configuration);
	}

	j_trace_leave(G_STRFUNC);

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

	g_return_if_fail(j_is_initialized());

	j_trace_enter(G_STRFUNC);

	j_operation_cache_fini();
	j_background_operation_fini();
	j_connection_pool_fini();

	common = g_atomic_pointer_get(&j_common);
	g_atomic_pointer_set(&j_common, NULL);

	j_configuration_unref(common->configuration);

	mongoc_cleanup();

	j_trace_leave(G_STRFUNC);

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
 * \param batch      A batch.
 **/
JStore*
j_create_store (gchar const* name, JBatch* batch)
{
	JOperation* operation;
	JStore* store;

	if ((store = j_store_new(name)) == NULL)
	{
		goto end;
	}

	operation = j_operation_new(J_OPERATION_CREATE_STORE);
	operation->key = NULL;
	operation->u.create_store.store = j_store_ref(store);

	j_batch_add(batch, operation);

end:
	return store;
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
 * \param batch      A batch.
 **/
void
j_delete_store (JStore* store, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(store != NULL);

	operation = j_operation_new(J_OPERATION_DELETE_STORE);
	operation->key = NULL;
	operation->u.delete_store.store = j_store_ref(store);

	j_batch_add(batch, operation);
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
 * \param batch     A batch.
 **/
void
j_get_store (JStore** store, gchar const* name, JBatch* batch)
{
	JOperation* operation;

	g_return_if_fail(store != NULL);
	g_return_if_fail(name != NULL);

	operation = j_operation_new(J_OPERATION_GET_STORE);
	operation->key = NULL;
	operation->u.get_store.store = store;
	operation->u.get_store.name = g_strdup(name);

	j_batch_add(batch, operation);
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
 * Creates stores.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_create_store_internal (JBatch* batch, JList* operations)
{
	JListIterator* it;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(operations);

	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JStore* store = operation->u.create_store.store;

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
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_delete_store_internal (JBatch* batch, JList* operations)
{
	JListIterator* it;
	mongoc_client_t* mongo_connection;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(operations);
	mongo_connection = j_connection_pool_pop_meta(0);

	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JStore* store = operation->u.delete_store.store;

		mongoc_database_t* m_database;

		m_database = mongoc_client_get_database(mongo_connection, j_store_get_name(store));
		mongoc_database_drop(m_database, NULL);
	}

	j_connection_pool_push_meta(0, mongo_connection);
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
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_get_store_internal (JBatch* batch, JList* operations)
{
	JListIterator* it;

	g_return_val_if_fail(batch != NULL, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);

	/*
		IsInitialized(true);
	*/

	it = j_list_iterator_new(operations);

	while (j_list_iterator_next(it))
	{
		JOperation* operation = j_list_iterator_get(it);
		JStore** store = operation->u.get_store.store;
		gchar const* name = operation->u.get_store.name;

		*store = j_store_new(name);
	}

	j_list_iterator_free(it);

	return TRUE;
}

/**
 * @}
 **/
