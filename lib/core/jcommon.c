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

static gboolean j_inited = FALSE;

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

// FIXME copy and use GLib's G_DEFINE_CONSTRUCTOR/DESTRUCTOR
static void __attribute__((constructor)) j_init (void);
static void __attribute__((destructor)) j_fini (void);

/**
 * Initializes JULEA.
 */
static
void
j_init (void)
{
	JTrace* trace;
	g_autofree gchar* basename = NULL;

	if (j_inited)
	{
		return;
	}

	basename = j_get_program_name("libjulea");

	if (g_strcmp0(basename, "julea-server") == 0)
	{
		return;
	}

	j_trace_init(basename);
	trace = j_trace_enter(G_STRFUNC, NULL);

	if (j_configuration() == NULL)
	{
		goto error;
	}

	j_connection_pool_init(j_configuration());
	j_distribution_init();
	j_background_operation_init(0);
	j_operation_cache_init();

	j_inited = TRUE;

	j_trace_leave(trace);

	return;

error:
	j_trace_leave(trace);

	j_trace_fini();

	g_error("%s: Failed to initialize JULEA.", G_STRLOC);
}

/**
 * Shuts down JULEA.
 */
static
void
j_fini (void)
{
	JTrace* trace;

	if (!j_inited)
	{
		return;
	}

	trace = j_trace_enter(G_STRFUNC, NULL);

	j_operation_cache_fini();
	j_background_operation_fini();
	j_connection_pool_fini();

	j_inited = FALSE;

	j_trace_leave(trace);

	j_trace_fini();
}

/**
 * @}
 **/
