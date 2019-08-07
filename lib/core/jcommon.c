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

#include <jcommon.h>

#include <jbackend.h>
#include <jbackground-operation-internal.h>
#include <jconfiguration.h>
#include <jconnection-pool-internal.h>
#include <jdistribution-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-cache-internal.h>
#include <joperation-internal.h>
#include <jtrace.h>

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

	JBackend* object_backend;
	JBackend* kv_backend;
	JBackend* db_backend;

	GModule* object_module;
	GModule* kv_module;
	GModule* db_module;
};

static JCommon* j_common = NULL;

/**
 * Returns whether JULEA has been initialized.
 *
 * \private
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
 * \param argc A pointer to \c argc.
 * \param argv A pointer to \c argv.
 */
void
j_init (void)
{
	JCommon* common;
	JTrace* trace;
	g_autofree gchar* basename = NULL;
	gchar const* object_backend;
	gchar const* object_component;
	gchar const* object_path;
	gchar const* kv_backend;
	gchar const* kv_component;
	gchar const* kv_path;
	gchar const* db_backend;
	gchar const* db_component;
	gchar const* db_path;

	if (j_is_initialized())
	{
		return;
	}

	basename = j_get_program_name("libjulea");

	if (g_strcmp0(basename, "julea-server") == 0)
	{
		return;
	}

	common = g_slice_new(JCommon);
	common->configuration = NULL;

	j_trace_init(basename);
	trace = j_trace_enter(G_STRFUNC, NULL);

	common->configuration = j_configuration_new();

	if (common->configuration == NULL)
	{
		goto error;
	}

	object_backend = j_configuration_get_object_backend(common->configuration);
	object_component = j_configuration_get_object_component(common->configuration);
	object_path = j_configuration_get_object_path(common->configuration);

	kv_backend = j_configuration_get_kv_backend(common->configuration);
	kv_component = j_configuration_get_kv_component(common->configuration);
	kv_path = j_configuration_get_kv_path(common->configuration);

	db_backend = j_configuration_get_db_backend(common->configuration);
	db_component = j_configuration_get_db_component(common->configuration);
	db_path = j_configuration_get_db_path(common->configuration);

	if (j_backend_load_client(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &(common->object_module), &(common->object_backend)))
	{
		if (common->object_backend == NULL || !j_backend_object_init(common->object_backend, object_path))
		{
			g_critical("Could not initialize object backend %s.\n", object_backend);
			goto error;
		}
	}

	if (j_backend_load_client(kv_backend, kv_component, J_BACKEND_TYPE_KV, &(common->kv_module), &(common->kv_backend)))
	{
		if (common->kv_backend == NULL || !j_backend_kv_init(common->kv_backend, kv_path))
		{
			g_critical("Could not initialize kv backend %s.\n", kv_backend);
			goto error;
		}
	}

	if (j_backend_load_client(db_backend, db_component, J_BACKEND_TYPE_DB, &(common->db_module), &(common->db_backend)))
	{
		if (common->db_backend == NULL || !j_backend_db_init(common->db_backend, db_path))
		{
			g_critical("Could not initialize db backend %s.\n", db_backend);
			goto error;
		}
	}

	j_connection_pool_init(common->configuration);
	j_distribution_init();
	j_background_operation_init(0);
	j_operation_cache_init();

	g_atomic_pointer_set(&j_common, common);

	j_trace_leave(trace);

	return;

error:
	if (common->configuration != NULL)
	{
		j_configuration_unref(common->configuration);
	}

	j_trace_leave(trace);

	j_trace_fini();

	g_slice_free(JCommon, common);

	g_error("%s: Failed to initialize JULEA.", G_STRLOC);
}

/**
 * Shuts down JULEA.
 */
void
j_fini (void)
{
	JCommon* common;
	JTrace* trace;

	if (!j_is_initialized())
	{
		return;
	}

	trace = j_trace_enter(G_STRFUNC, NULL);

	j_operation_cache_fini();
	j_background_operation_fini();
	j_connection_pool_fini();

	common = g_atomic_pointer_get(&j_common);
	g_atomic_pointer_set(&j_common, NULL);

	if (common->db_backend != NULL)
	{
		j_backend_db_fini(common->db_backend);
	}

	if (common->kv_backend != NULL)
	{
		j_backend_kv_fini(common->kv_backend);
	}

	if (common->object_backend != NULL)
	{
		j_backend_object_fini(common->object_backend);
	}

	if (common->db_module)
	{
		g_module_close(common->db_module);
	}

	if (common->kv_module)
	{
		g_module_close(common->kv_module);
	}

	if (common->object_module)
	{
		g_module_close(common->object_module);
	}

	j_configuration_unref(common->configuration);

	j_trace_leave(trace);

	j_trace_fini();

	g_slice_free(JCommon, common);
}

/* Internal */

/**
 * Returns the configuration.
 *
 * \private
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
 * Returns the object backend.
 *
 * \private
 *
 * \return The object backend.
 */
JBackend*
j_object_backend (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->object_backend;
}

/**
 * Returns the kv backend.
 *
 * \private
 *
 * \return The kv backend.
 */
JBackend*
j_kv_backend (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->kv_backend;
}

/**
 * Returns the db backend.
 *
 * \private
 *
 * \return The db backend.
 */
JBackend*
j_db_backend (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->db_backend;
}

/**
 * @}
 **/
