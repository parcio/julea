/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <glib.h>

#include <string.h>

#include "jconfiguration.h"

/**
 * \defgroup JConfiguration Configuration
 *
 * @{
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

	return configuration;
}

void
j_configuration_free (JConfiguration* configuration)
{
	g_free(configuration->storage.backend);
	g_free(configuration->storage.path);

	g_strfreev(configuration->data);
	g_strfreev(configuration->metadata);

	g_slice_free(JConfiguration, configuration);
}

/**
 * @}
 **/
