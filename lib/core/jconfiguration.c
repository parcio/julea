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

#include <string.h>

#include <jconfiguration.h>

#include <jbackend.h>
#include <jtrace.h>

/**
 * \defgroup JConfiguration Configuration
 *
 * @{
 **/

/**
 * A configuration.
 */
struct JConfiguration
{
	struct
	{
		/**
		 * The object servers.
		 */
		gchar** object;

		/**
		 * The kv servers.
		 */
		gchar** kv;

		/**
		 * The db servers.
		 */
		gchar** db;

		/**
		 * The number of object servers.
		 */
		guint32 object_len;

		/**
		 * The number of kv servers.
		 */
		guint32 kv_len;

		/**
		 * The number of db servers.
		 */
		guint32 db_len;
	}
	servers;

	/**
	 * The object configuration.
	 */
	struct
	{
		/**
		 * The backend.
		 */
		gchar* backend;

		/**
		 * The component.
		 */
		gchar* component;

		/**
		 * The path.
		 */
		gchar* path;
	}
	object;

	/**
	 * The kv configuration.
	 */
	struct
	{
		/**
		 * The backend.
		 */
		gchar* backend;

		/**
		 * The component.
		 */
		gchar* component;

		/**
		 * The path.
		 */
		gchar* path;
	}
	kv;

	/**
	 * The db configuration.
	 */
	struct
	{
		/**
		 * The backend.
		 */
		gchar* backend;

		/**
		 * The component.
		 */
		gchar* component;

		/**
		 * The path.
		 */
		gchar* path;
	} db;

	guint64 max_operation_size;
	guint32 max_connections;
	guint64 stripe_size;

	/**
	 * The reference count.
	 */
	gint ref_count;
};

/**
 * Creates a new configuration.
 *
 * \code
 * \endcode
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new (void)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration = NULL;
	GKeyFile* key_file;
	gchar* config_name = NULL;
	gchar const* env_path;
	gchar* path = NULL;
	gchar const* const* dirs;

	key_file = g_key_file_new();

	if ((env_path = g_getenv("JULEA_CONFIG")) != NULL)
	{
		if (g_path_is_absolute(env_path))
		{
			if (g_key_file_load_from_file(key_file, env_path, G_KEY_FILE_NONE, NULL))
			{
				configuration = j_configuration_new_for_data(key_file);
			}
			else
			{
				g_critical("Can not open configuration file %s.", env_path);
			}

			/* If we do not find the configuration file, stop searching. */
			goto out;
		}
		else
		{
			config_name = g_path_get_basename(env_path);
		}
	}

	if (config_name == NULL)
	{
		config_name = g_strdup("julea");
	}

	path = g_build_filename(g_get_user_config_dir(), "julea", config_name, NULL);

	if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
	{
		configuration = j_configuration_new_for_data(key_file);

		goto out;
	}

	g_free(path);

	dirs = g_get_system_config_dirs();

	for (guint i = 0; dirs[i] != NULL; i++)
	{
		path = g_build_filename(dirs[i], "julea", config_name, NULL);

		if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
		{
			configuration = j_configuration_new_for_data(key_file);

			goto out;
		}

		g_free(path);
	}

	path = NULL;

out:
	g_key_file_free(key_file);

	g_free(path);
	g_free(config_name);

	return configuration;
}

/**
 * Creates a new configuration for the given configuration data.
 *
 * \code
 * \endcode
 *
 * \param key_file The configuration data.
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new_for_data (GKeyFile* key_file)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration;
	gchar** servers_object;
	gchar** servers_kv;
	gchar** servers_db;
	gchar* object_backend;
	gchar* object_component;
	gchar* object_path;
	gchar* kv_backend;
	gchar* kv_component;
	gchar* kv_path;
	gchar* db_backend;
	gchar* db_component;
	gchar* db_path;
	guint64 max_operation_size;
	guint32 max_connections;
	guint64 stripe_size;

	g_return_val_if_fail(key_file != NULL, FALSE);

	max_operation_size = g_key_file_get_uint64(key_file, "core", "max-operation-size", NULL);
	max_connections = g_key_file_get_integer(key_file, "clients", "max-connections", NULL);
	stripe_size = g_key_file_get_uint64(key_file, "clients", "stripe-size", NULL);
	servers_object = g_key_file_get_string_list(key_file, "servers", "object", NULL, NULL);
	servers_kv = g_key_file_get_string_list(key_file, "servers", "kv", NULL, NULL);
	servers_db = g_key_file_get_string_list(key_file, "servers", "db", NULL, NULL);
	object_backend = g_key_file_get_string(key_file, "object", "backend", NULL);
	object_component = g_key_file_get_string(key_file, "object", "component", NULL);
	object_path = g_key_file_get_string(key_file, "object", "path", NULL);
	kv_backend = g_key_file_get_string(key_file, "kv", "backend", NULL);
	kv_component = g_key_file_get_string(key_file, "kv", "component", NULL);
	kv_path = g_key_file_get_string(key_file, "kv", "path", NULL);
	db_backend = g_key_file_get_string(key_file, "db", "backend", NULL);
	db_component = g_key_file_get_string(key_file, "db", "component", NULL);
	db_path = g_key_file_get_string(key_file, "db", "path", NULL);

	if (servers_object == NULL || servers_object[0] == NULL
	    || servers_kv == NULL || servers_kv[0] == NULL
	    || servers_db == NULL || servers_db[0] == NULL
	    || object_backend == NULL
	    || object_component == NULL
	    || object_path == NULL
	    || kv_backend == NULL
	    || kv_component == NULL
	    || kv_path == NULL
	    || db_backend == NULL
	    || db_component == NULL
	    || db_path == NULL)
	{
		g_free(db_backend);
		g_free(db_component);
		g_free(db_path);
		g_free(kv_backend);
		g_free(kv_component);
		g_free(kv_path);
		g_free(object_backend);
		g_free(object_component);
		g_free(object_path);
		g_strfreev(servers_object);
		g_strfreev(servers_kv);
		g_strfreev(servers_db);

		return NULL;
	}

	configuration = g_slice_new(JConfiguration);
	configuration->servers.object = servers_object;
	configuration->servers.kv = servers_kv;
	configuration->servers.db = servers_db;
	configuration->servers.object_len = g_strv_length(servers_object);
	configuration->servers.kv_len = g_strv_length(servers_kv);
	configuration->servers.db_len = g_strv_length(servers_db);
	configuration->object.backend = object_backend;
	configuration->object.component = object_component;
	configuration->object.path = object_path;
	configuration->kv.backend = kv_backend;
	configuration->kv.component = kv_component;
	configuration->kv.path = kv_path;
	configuration->db.backend = db_backend;
	configuration->db.component = db_component;
	configuration->db.path = db_path;
	configuration->max_operation_size = max_operation_size;
	configuration->max_connections = max_connections;
	configuration->stripe_size = stripe_size;
	configuration->ref_count = 1;

	if (configuration->max_operation_size == 0)
	{
		configuration->max_operation_size = 8 * 1024 * 1024;
	}

	if (configuration->max_connections == 0)
	{
		configuration->max_connections = g_get_num_processors();
	}

	if (configuration->stripe_size == 0)
	{
		configuration->stripe_size = 4 * 1024 * 1024;
	}

	return configuration;
}

/**
 * Increases a configuration's reference count.
 *
 * \code
 * JConfiguration* c;
 *
 * j_configuration_ref(c);
 * \endcode
 *
 * \param configuration A configuration.
 *
 * \return #configuration.
 **/
JConfiguration*
j_configuration_ref (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	g_atomic_int_inc(&(configuration->ref_count));

	return configuration;
}

/**
 * Decreases a configuration's reference count.
 * When the reference count reaches zero, frees the memory allocated for the configuration.
 *
 * \code
 * \endcode
 *
 * \param configuration A configuration.
 **/
void
j_configuration_unref (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(configuration != NULL);

	if (g_atomic_int_dec_and_test(&(configuration->ref_count)))
	{
		g_free(configuration->db.backend);
		g_free(configuration->db.component);
		g_free(configuration->db.path);

		g_free(configuration->kv.backend);
		g_free(configuration->kv.component);
		g_free(configuration->kv.path);

		g_free(configuration->object.backend);
		g_free(configuration->object.component);
		g_free(configuration->object.path);

		g_strfreev(configuration->servers.object);
		g_strfreev(configuration->servers.kv);
		g_strfreev(configuration->servers.db);

		g_slice_free(JConfiguration, configuration);
	}
}

gchar const*
j_configuration_get_server (JConfiguration* configuration, JBackendType backend, guint32 index)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			g_return_val_if_fail(index < configuration->servers.object_len, NULL);
			return configuration->servers.object[index];
		case J_BACKEND_TYPE_KV:
			g_return_val_if_fail(index < configuration->servers.kv_len, NULL);
			return configuration->servers.kv[index];
		case J_BACKEND_TYPE_DB:
			g_return_val_if_fail(index < configuration->servers.db_len, NULL);
			return configuration->servers.db[index];
		default:
			g_assert_not_reached();
	}

	return NULL;
}

guint32
j_configuration_get_server_count (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->servers.object_len;
		case J_BACKEND_TYPE_KV:
			return configuration->servers.kv_len;
		case J_BACKEND_TYPE_DB:
			return configuration->servers.db_len;
		default:
			g_assert_not_reached();
	}

	return 0;
}

gchar const*
j_configuration_get_backend (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->object.backend;
		case J_BACKEND_TYPE_KV:
			return configuration->kv.backend;
		case J_BACKEND_TYPE_DB:
			return configuration->db.backend;
		default:
			g_assert_not_reached();
	}

	return NULL;
}

gchar const*
j_configuration_get_backend_component (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->object.component;
		case J_BACKEND_TYPE_KV:
			return configuration->kv.component;
		case J_BACKEND_TYPE_DB:
			return configuration->db.component;
		default:
			g_assert_not_reached();
	}

	return NULL;
}

gchar const*
j_configuration_get_backend_path (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->object.path;
		case J_BACKEND_TYPE_KV:
			return configuration->kv.path;
		case J_BACKEND_TYPE_DB:
			return configuration->db.path;
		default:
			g_assert_not_reached();
	}

	return NULL;
}

guint64
j_configuration_get_max_operation_size (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_operation_size;
}

guint32
j_configuration_get_max_connections (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_connections;
}

guint64
j_configuration_get_stripe_size (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->stripe_size;
}

/**
 * @}
 **/
