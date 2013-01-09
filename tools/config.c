/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include <string.h>

static gboolean opt_local = FALSE;
static gboolean opt_global = FALSE;
static gchar const* opt_data = NULL;
static gchar const* opt_metadata = NULL;
static gchar const* opt_storage_backend = "gio";
static gchar const* opt_storage_path = NULL;

static
gchar**
string_split (gchar const* string)
{
	guint i;
	guint len;
	gchar** arr;

	arr = g_strsplit(string, ",", 0);
	len = g_strv_length(arr);

	for (i = 0; i < len; i++)
	{
		g_strstrip(arr[i]);
	}

	return arr;
}

static
gboolean
write_config (gchar* path)
{
	GKeyFile* key_file;
	gboolean ret = TRUE;
	gsize key_file_data_len;
	gchar* key_file_data;
	gchar** data;
	gchar** metadata;

	data = string_split(opt_data);
	metadata = string_split(opt_metadata);

	if (opt_storage_path == NULL)
	{
		if (strcmp(opt_storage_backend, "null") == 0)
		{
			opt_storage_path = "";
		}
		else if (strcmp(opt_storage_backend, "gio") == 0)
		{
			opt_storage_path = "/tmp/julea";
		}
		else if (strcmp(opt_storage_backend, "posix") == 0)
		{
			opt_storage_path = "/tmp/julea";
		}
	}

	key_file = g_key_file_new();
	g_key_file_set_string_list(key_file, "servers", "data", (gchar const* const*)data, g_strv_length(data));
	g_key_file_set_string_list(key_file, "servers", "metadata", (gchar const* const*)metadata, g_strv_length(metadata));
	g_key_file_set_string(key_file, "storage", "backend", opt_storage_backend);
	g_key_file_set_string(key_file, "storage", "path", opt_storage_path);
	key_file_data = g_key_file_to_data(key_file, &key_file_data_len, NULL);

	if (path != NULL)
	{
		GFile* file;
		GFile* parent;

		file = g_file_new_for_commandline_arg(path);
		parent = g_file_get_parent(file);
		g_file_make_directory_with_parents(parent, NULL, NULL);
		ret = g_file_replace_contents(file, key_file_data, key_file_data_len, NULL, FALSE, G_FILE_CREATE_NONE, NULL, NULL, NULL);

		g_object_unref(file);
		g_object_unref(parent);

		g_free(path);
	}
	else
	{
		g_print("%s", key_file_data);
	}

	g_free(key_file_data);
	g_key_file_free(key_file);

	g_strfreev(data);
	g_strfreev(metadata);

	return ret;
}

gint
main (gint argc, gchar** argv)
{
	GError* error = NULL;
	GOptionContext* context;
	gchar* path = NULL;

	GOptionEntry entries[] = {
		{ "local", 'l', 0, G_OPTION_ARG_NONE, &opt_local, "Write local configuration", NULL },
		{ "global", 'g', 0, G_OPTION_ARG_NONE, &opt_global, "Write global configuration", NULL },
		{ "data", 'd', 0, G_OPTION_ARG_STRING, &opt_data, "Data servers to use", "host1,host2" },
		{ "metadata", 'm', 0, G_OPTION_ARG_STRING, &opt_metadata, "Metadata servers to use", "host1,host2" },
		{ "storage-backend", 'b', 0, G_OPTION_ARG_STRING, &opt_storage_backend, "Storage backend to use", "null|gio|posix" },
		{ "storage-path", 'p', 0, G_OPTION_ARG_STRING, &opt_storage_path, "Storage path to use", "/tmp/julea" },
		{ NULL }
	};

	g_type_init();

	context = g_option_context_new("[FILE]");
	g_option_context_add_main_entries(context, entries, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		g_option_context_free(context);

		if (error)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	if (opt_data == NULL
	    || opt_metadata == NULL
	    || (opt_local && opt_global)
	    || (strcmp(opt_storage_backend, "null") != 0 && strcmp(opt_storage_backend, "gio") != 0 && strcmp(opt_storage_backend, "posix") != 0)
	)
	{
		gchar* help;

		help = g_option_context_get_help(context, TRUE, NULL);
		g_option_context_free(context);

		g_print("%s", help);
		g_free(help);

		return 1;
	}

	g_option_context_free(context);

	if (opt_local)
	{
		path = g_build_filename(g_get_user_config_dir(), "julea", "julea", NULL);
	}
	else if (opt_global)
	{
		path = g_build_filename(g_get_system_config_dirs()[0], "julea", "julea", NULL);
	}
	else if (argc > 1)
	{
		path = g_strdup(argv[1]);
	}

	if (!write_config(path))
	{
		return 1;
	}

	return 0;
}
