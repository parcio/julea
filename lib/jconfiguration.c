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
		guint data_len;

		/**
		 * The number of metadata servers.
		 */
		guint metadata_len;
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

	guint max_connections;

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
	gchar const* env_path;
	gchar* path = NULL;
	gchar const* const* dirs;
	guint i;

	key_file = g_key_file_new();

	if ((env_path = g_getenv("JULEA_CONFIG")) != NULL)
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

	path = g_build_filename(g_get_user_config_dir(), "julea", "julea", NULL);

	if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
	{
		configuration = j_configuration_new_for_data(key_file);

		goto out;
	}

	g_free(path);

	dirs = g_get_system_config_dirs();

	for (i = 0; dirs[i] != NULL; i++)
	{
		path = g_build_filename(dirs[i], "julea", "julea", NULL);

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
	guint max_connections;

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
j_configuration_get_data_server (JConfiguration* configuration, guint index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.data_len, NULL);

	return configuration->servers.data[index];
}

gchar const*
j_configuration_get_metadata_server (JConfiguration* configuration, guint index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.metadata_len, NULL);

	return configuration->servers.metadata[index];
}

guint
j_configuration_get_data_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->servers.data_len;
}

guint
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

guint
j_configuration_get_max_connections (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_connections;
}

/**
 * @}
 **/
