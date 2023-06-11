/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2023-2023 Julian Benda
 * Copyright (C) 2023 Timm Leon Erxleben
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

#include <julea-config.h>

#include <glib.h>
#include <locale.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <julea.h>

JBackend* object_backend = NULL;
JBackend* kv_backend = NULL;
JBackend* db_backend = NULL;

static gboolean
setup_backend(JConfiguration* configuration, JBackendType type, gchar const* port_str, GModule** module, JBackend** j_backend)
{
	gchar const* backend;
	gchar const* component;
	g_autofree gchar* path;
	gboolean load_module = FALSE;

	backend = j_configuration_get_backend(configuration, type);
	component = j_configuration_get_backend_component(configuration, type);
	path = j_helper_str_replace(j_configuration_get_backend_path(configuration, type), "{PORT}", port_str);

	if (strcmp(component, "server") == 0)
	{
		load_module = j_backend_load_server(backend, component, type, module, j_backend);
	}
	else
	{
		load_module = j_backend_load_client(backend, component, type, module, j_backend);
	}

	if (load_module)
	{
		gboolean res = TRUE;

		if (j_backend == NULL)
		{
			g_warning("Failed to load component: %s.", backend);
			return FALSE;
		}

		switch (type)
		{
			case J_BACKEND_TYPE_OBJECT:
				res = j_backend_object_init(*j_backend, path);
				break;
			case J_BACKEND_TYPE_KV:
				res = j_backend_kv_init(*j_backend, path);
				break;
			case J_BACKEND_TYPE_DB:
				res = j_backend_db_init(*j_backend, path);
				break;
			default:
				g_warning("unknown backend type: (%d), unable to setup backend!", type);
				res = FALSE;
		}

		if (!res)
		{
			g_warning("Failed to initalize backend: %s", backend);
			return FALSE;
		}

		return TRUE;
	}

	return FALSE;
}

int
main(int argc, char** argv)
{
	JConfiguration* configuration = NULL;
	GModule* db_module = NULL;
	GModule* kv_module = NULL;
	GModule* object_module = NULL;
	g_autofree gchar* port_str = NULL;
	gint res = 1;
	(void)argc;
	(void)argv;

	setlocale(LC_ALL, "C.UTF-8");

	configuration = j_configuration();

	if (configuration == NULL)
	{
		g_warning("unable to read config");
		goto end;
	}

	port_str = g_strdup_printf("%d", j_configuration_get_port(configuration));
	if (!setup_backend(configuration, J_BACKEND_TYPE_OBJECT, port_str, &object_module, &object_backend))
	{
		g_warning("failed to initealize object backend");
		goto end;
	}
	if (!setup_backend(configuration, J_BACKEND_TYPE_KV, port_str, &kv_module, &kv_backend))
	{
		g_warning("failed to initealize kv backend");
		goto end;
	}
	if (!setup_backend(configuration, J_BACKEND_TYPE_DB, port_str, &db_module, &db_backend))
	{
		g_warning("failed to initealize db backend");
		goto end;
	}

	if (!j_backend_db_clean(db_backend))
	{
		g_warning("Could not clean db backend");
		goto end;
	}

	if (!j_backend_kv_clean(kv_backend))
	{
		g_warning("Could not clean kv backend");
		goto end;
	}

	if (!j_backend_object_clean(object_backend))
	{
		g_warning("Could not clean object backend");
		goto end;
	}

	res = 0;
end:
	if (db_backend != NULL)
	{
		j_backend_db_fini(db_backend);
	}
	if (kv_backend != NULL)
	{
		j_backend_kv_fini(kv_backend);
	}
	if (object_backend != NULL)
	{
		j_backend_object_fini(object_backend);
	}
	if (db_module != NULL)
	{
		g_module_close(db_module);
	}
	if (kv_module != NULL)
	{
		g_module_close(kv_module);
	}
	if (object_module != NULL)
	{
		g_module_close(object_module);
	}
	j_configuration_unref(configuration);

	return res;
}
