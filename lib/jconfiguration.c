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

#include <string.h>

#include <jconfiguration-internal.h>

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
		 * The data servers.
		 */
		gchar** data;

		/**
		 * The metadata servers.
		 */
		gchar** metadata;

		/**
		 * The number of data servers.
		 */
		guint32 data_len;

		/**
		 * The number of metadata servers.
		 */
		guint32 metadata_len;
	}
	servers;

	/**
	 * The data configuration.
	 */
	struct
	{
		/**
		 * The backend.
		 */
		gchar* backend;

		/**
		 * The path.
		 */
		gchar* path;
	}
	data;

	/**
	 * The metadata configuration.
	 */
	struct
	{
		/**
		 * The backend.
		 */
		gchar* backend;

		/**
		 * The path.
		 */
		gchar* path;
	}
	meta;

	guint32 max_connections;

	/**
	 * The reference count.
	 */
	gint ref_count;
};

/**
 * Creates a new configuration.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new (void)
{
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
				J_CRITICAL("Can not open configuration file %s.", env_path);
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
 * \author Michael Kuhn
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
	JConfiguration* configuration;
	gchar** servers_data;
	gchar** servers_meta;
	gchar* data_backend;
	gchar* data_path;
	gchar* meta_backend;
	gchar* meta_path;
	guint32 max_connections;

	g_return_val_if_fail(key_file != NULL, FALSE);

	max_connections = g_key_file_get_integer(key_file, "clients", "max-connections", NULL);
	servers_data = g_key_file_get_string_list(key_file, "servers", "data", NULL, NULL);
	servers_meta = g_key_file_get_string_list(key_file, "servers", "metadata", NULL, NULL);
	data_backend = g_key_file_get_string(key_file, "data", "backend", NULL);
	data_path = g_key_file_get_string(key_file, "data", "path", NULL);
	meta_backend = g_key_file_get_string(key_file, "metadata", "backend", NULL);
	meta_path = g_key_file_get_string(key_file, "metadata", "path", NULL);

	if (servers_data == NULL || servers_data[0] == NULL
	    || servers_meta == NULL || servers_meta[0] == NULL
	    || data_backend == NULL
	    || data_path == NULL
	    || meta_backend == NULL
	    || meta_path == NULL)
	{
		g_free(meta_backend);
		g_free(meta_path);
		g_free(data_backend);
		g_free(data_path);
		g_strfreev(servers_data);
		g_strfreev(servers_meta);

		return NULL;
	}

	configuration = g_slice_new(JConfiguration);
	configuration->servers.data = servers_data;
	configuration->servers.metadata = servers_meta;
	configuration->servers.data_len = g_strv_length(servers_data);
	configuration->servers.metadata_len = g_strv_length(servers_meta);
	configuration->data.backend = data_backend;
	configuration->data.path = data_path;
	configuration->meta.backend = meta_backend;
	configuration->meta.path = meta_path;
	configuration->max_connections = max_connections;
	configuration->ref_count = 1;

	return configuration;
}

/**
 * Increases a configuration's reference count.
 *
 * \author Michael Kuhn
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
	g_return_val_if_fail(configuration != NULL, NULL);

	g_atomic_int_inc(&(configuration->ref_count));

	return configuration;
}

/**
 * Decreases a configuration's reference count.
 * When the reference count reaches zero, frees the memory allocated for the configuration.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param configuration A configuration.
 **/
void
j_configuration_unref (JConfiguration* configuration)
{
	if (g_atomic_int_dec_and_test(&(configuration->ref_count)))
	{
		g_free(configuration->meta.backend);
		g_free(configuration->meta.path);

		g_free(configuration->data.backend);
		g_free(configuration->data.path);

		g_strfreev(configuration->servers.data);
		g_strfreev(configuration->servers.metadata);

		g_slice_free(JConfiguration, configuration);
	}
}

gchar const*
j_configuration_get_data_server (JConfiguration* configuration, guint32 index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.data_len, NULL);

	return configuration->servers.data[index];
}

gchar const*
j_configuration_get_metadata_server (JConfiguration* configuration, guint32 index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.metadata_len, NULL);

	return configuration->servers.metadata[index];
}

guint32
j_configuration_get_data_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->servers.data_len;
}

guint32
j_configuration_get_metadata_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->servers.metadata_len;
}

gchar const*
j_configuration_get_data_backend (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->data.backend;
}

gchar const*
j_configuration_get_data_path (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->data.path;
}

gchar const*
j_configuration_get_metadata_backend (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->meta.backend;
}

gchar const*
j_configuration_get_metadata_path (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->meta.path;
}

guint32
j_configuration_get_max_connections (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_connections;
}

/**
 * @}
 **/
