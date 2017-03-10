/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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
#include <jcommon-internal.h>

#include <jbackend.h>
#include <jbackend-internal.h>
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

	JBackend* data_backend;
	JBackend* meta_backend;

	GModule* data_module;
	GModule* meta_module;
};

static JCommon* j_common = NULL;

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

	basename = j_get_program_name("julea");
	j_trace_init(basename);
	g_free(basename);

	j_trace_enter(G_STRFUNC, NULL);

	common->configuration = j_configuration_new();

	if (common->configuration == NULL)
	{
		goto error;
	}

	common->data_module = j_backend_load_client(j_configuration_get_data_backend(common->configuration), J_BACKEND_TYPE_DATA, &(common->data_backend));
	common->meta_module = j_backend_load_client(j_configuration_get_metadata_backend(common->configuration), J_BACKEND_TYPE_META, &(common->meta_backend));

	if (common->data_backend != NULL)
	{
		j_backend_data_init(common->data_backend, j_configuration_get_data_path(common->configuration));
	}

	if (common->meta_backend != NULL)
	{
		j_backend_meta_init(common->meta_backend, j_configuration_get_metadata_path(common->configuration));
	}

	j_connection_pool_init(common->configuration);
	j_distribution_init();
	j_background_operation_init(0);
	j_operation_cache_init();

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

	j_trace_enter(G_STRFUNC, NULL);

	j_operation_cache_fini();
	j_background_operation_fini();
	j_connection_pool_fini();

	common = g_atomic_pointer_get(&j_common);
	g_atomic_pointer_set(&j_common, NULL);

	if (common->meta_backend != NULL)
	{
		j_backend_meta_fini(common->meta_backend);
	}

	if (common->data_backend != NULL)
	{
		j_backend_data_fini(common->data_backend);
	}

	if (common->meta_module)
	{
		g_module_close(common->meta_module);
	}

	if (common->data_module)
	{
		g_module_close(common->data_module);
	}

	j_configuration_unref(common->configuration);

	j_trace_leave(G_STRFUNC);

	j_trace_fini();

	g_slice_free(JCommon, common);
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
 * Returns the data backend.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \return The data backend.
 */
JBackend*
j_data_backend (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->data_backend;
}

/**
 * Returns the data backend.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \return The data backend.
 */
JBackend*
j_metadata_backend (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->meta_backend;
}

/**
 * @}
 **/
