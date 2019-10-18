/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Michael Kuhn
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

#include <db/jdb-internal.h>

#include <julea.h>

/**
 * \defgroup JDB DB
 *
 * Data structures and functions for managing databases.
 *
 * @{
 **/

static JBackend* j_db_backend = NULL;
static GModule* j_db_module = NULL;

// FIXME copy and use GLib's G_DEFINE_CONSTRUCTOR/DESTRUCTOR
static void __attribute__((constructor)) j_db_init (void);
static void __attribute__((destructor)) j_db_fini (void);

/**
 * Initializes the db client.
 */
static
void
j_db_init (void)
{
	gchar const* db_backend;
	gchar const* db_component;
	gchar const* db_path;

	if (j_db_backend != NULL && j_db_module != NULL)
	{
		return;
	}

	db_backend = j_configuration_get_backend(j_configuration(), J_BACKEND_TYPE_DB);
	db_component = j_configuration_get_backend_component(j_configuration(), J_BACKEND_TYPE_DB);
	db_path = j_configuration_get_backend_path(j_configuration(), J_BACKEND_TYPE_DB);

	if (j_backend_load_client(db_backend, db_component, J_BACKEND_TYPE_DB, &j_db_module, &j_db_backend))
	{
		if (j_db_backend == NULL || !j_backend_db_init(j_db_backend, db_path))
		{
			g_critical("Could not initialize db backend %s.\n", db_backend);
		}
	}
}

/**
 * Shuts down the db client.
 */
static
void
j_db_fini (void)
{
	if (j_db_backend == NULL && j_db_module == NULL)
	{
		return;
	}

	if (j_db_backend != NULL)
	{
		j_backend_db_fini(j_db_backend);
	}

	if (j_db_module)
	{
		g_module_close(j_db_module);
	}
}

/**
 * Returns the db backend.
 *
 * \return The db backend.
 */
JBackend*
j_db_get_backend (void)
{
	return j_db_backend;
}

/**
 * @}
 **/
