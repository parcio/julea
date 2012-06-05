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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <jconfiguration.h>
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

	/**
	 * The storage configuration.
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
	storage;

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
	gchar* path;
	gchar const* const* dirs;
	guint i;

	key_file = g_key_file_new();
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
	gchar** data;
	gchar** metadata;
	gchar* storage_backend;
	gchar* storage_path;

	g_return_val_if_fail(key_file != NULL, FALSE);

	data = g_key_file_get_string_list(key_file, "servers", "data", NULL, NULL);
	metadata = g_key_file_get_string_list(key_file, "servers", "metadata", NULL, NULL);
	storage_backend = g_key_file_get_string(key_file, "storage", "backend", NULL);
	storage_path = g_key_file_get_string(key_file, "storage", "path", NULL);

	if (data == NULL || data[0] == NULL
	    || metadata == NULL || metadata[0] == NULL
	    || storage_backend == NULL
	    || storage_path == NULL)
	{
		g_free(storage_backend);
		g_free(storage_path);
		g_strfreev(data);
		g_strfreev(metadata);

		return NULL;
	}

	configuration = g_slice_new(JConfiguration);
	configuration->data = data;
	configuration->metadata = metadata;
	configuration->data_len = g_strv_length(data);
	configuration->metadata_len = g_strv_length(metadata);
	configuration->storage.backend = storage_backend;
	configuration->storage.path = storage_path;
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
		g_free(configuration->storage.backend);
		g_free(configuration->storage.path);

		g_strfreev(configuration->data);
		g_strfreev(configuration->metadata);

		g_slice_free(JConfiguration, configuration);
	}
}

gchar const*
j_configuration_get_data_server (JConfiguration* configuration, guint index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->data_len, NULL);

	return configuration->data[index];
}

gchar const*
j_configuration_get_metadata_server (JConfiguration* configuration, guint index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->metadata_len, NULL);

	return configuration->metadata[index];
}

guint
j_configuration_get_data_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->data_len;
}

guint
j_configuration_get_metadata_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->metadata_len;
}

gchar const*
j_configuration_get_storage_backend (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->storage.backend;
}

gchar const*
j_configuration_get_storage_path (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->storage.path;
}

/**
 * @}
 **/
