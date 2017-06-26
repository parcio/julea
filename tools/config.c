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

#include <julea-config.h>

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include <string.h>

static gboolean opt_user = FALSE;
static gboolean opt_system = FALSE;
static gboolean opt_read = FALSE;
static gchar const* opt_name = "julea";
static gchar const* opt_servers_data = NULL;
static gchar const* opt_servers_meta = NULL;
static gchar const* opt_data_backend = NULL;
static gchar const* opt_data_component = NULL;
static gchar const* opt_data_path = NULL;
static gchar const* opt_meta_backend = NULL;
static gchar const* opt_meta_component = NULL;
static gchar const* opt_meta_path = NULL;
static gint opt_max_connections = 0;

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
read_config (gchar* path)
{
	gboolean ret = TRUE;
	g_autoptr(GFile) file = NULL;
	g_autofree gchar* buf = NULL;

	if (path == NULL)
	{
		ret = FALSE;
		goto end;
	}

	file = g_file_new_for_commandline_arg(path);
	ret = g_file_load_contents(file, NULL, &buf, NULL, NULL, NULL);

	if (ret)
	{
		g_print("%s", buf);
	}

end:
	return ret;
}

static
gboolean
write_config (gchar* path)
{
	g_autoptr(GKeyFile) key_file = NULL;
	gboolean ret = TRUE;
	gsize key_file_data_len;
	g_autofree gchar* key_file_data = NULL;
	g_auto(GStrv) servers_data = NULL;
	g_auto(GStrv) servers_meta = NULL;

	servers_data = string_split(opt_servers_data);
	servers_meta = string_split(opt_servers_meta);

	key_file = g_key_file_new();
	g_key_file_set_integer(key_file, "clients", "max-connections", opt_max_connections);
	g_key_file_set_string_list(key_file, "servers", "data", (gchar const* const*)servers_data, g_strv_length(servers_data));
	g_key_file_set_string_list(key_file, "servers", "metadata", (gchar const* const*)servers_meta, g_strv_length(servers_meta));
	g_key_file_set_string(key_file, "data", "backend", opt_data_backend);
	g_key_file_set_string(key_file, "data", "component", opt_data_component);
	g_key_file_set_string(key_file, "data", "path", opt_data_path);
	g_key_file_set_string(key_file, "metadata", "backend", opt_meta_backend);
	g_key_file_set_string(key_file, "metadata", "component", opt_meta_component);
	g_key_file_set_string(key_file, "metadata", "path", opt_meta_path);
	key_file_data = g_key_file_to_data(key_file, &key_file_data_len, NULL);

	if (path != NULL)
	{
		g_autoptr(GFile) file = NULL;
		g_autoptr(GFile) parent = NULL;

		file = g_file_new_for_commandline_arg(path);
		parent = g_file_get_parent(file);
		g_file_make_directory_with_parents(parent, NULL, NULL);
		ret = g_file_replace_contents(file, key_file_data, key_file_data_len, NULL, FALSE, G_FILE_CREATE_NONE, NULL, NULL, NULL);
	}
	else
	{
		g_print("%s", key_file_data);
	}

	return ret;
}

gint
main (gint argc, gchar** argv)
{
	GError* error = NULL;
	g_autoptr(GOptionContext) context = NULL;
	gboolean ret;
	g_autofree gchar* path = NULL;

	GOptionEntry entries[] = {
		{ "user", 0, 0, G_OPTION_ARG_NONE, &opt_user, "Write user configuration", NULL },
		{ "system", 0, 0, G_OPTION_ARG_NONE, &opt_system, "Write system configuration", NULL },
		{ "read", 0, 0, G_OPTION_ARG_NONE, &opt_read, "Read configuration", NULL },
		{ "name", 0, 0, G_OPTION_ARG_STRING, &opt_name, "Configuration name", "julea" },
		{ "data-servers", 0, 0, G_OPTION_ARG_STRING, &opt_servers_data, "Data servers to use", "host1,host2" },
		{ "metadata-servers", 0, 0, G_OPTION_ARG_STRING, &opt_servers_meta, "Metadata servers to use", "host1,host2" },
		{ "data-backend", 0, 0, G_OPTION_ARG_STRING, &opt_data_backend, "Data backend to use", "posix|null|gio|…" },
		{ "data-component", 0, 0, G_OPTION_ARG_STRING, &opt_data_component, "Data component to use", "client|server" },
		{ "data-path", 0, 0, G_OPTION_ARG_STRING, &opt_data_path, "Data path to use", "/path/to/storage" },
		{ "metadata-backend", 0, 0, G_OPTION_ARG_STRING, &opt_meta_backend, "Metadata backend to use", "posix|null|gio|…" },
		{ "meta-component", 0, 0, G_OPTION_ARG_STRING, &opt_meta_component, "Metadata component to use", "client|server" },
		{ "metadata-path", 0, 0, G_OPTION_ARG_STRING, &opt_meta_path, "Metadata path to use", "/path/to/storage" },
		{ "max-connections", 0, 0, G_OPTION_ARG_INT, &opt_max_connections, "Maximum number of connections", "0" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		if (error)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	if ((opt_user && opt_system)
	    || (opt_read && (opt_servers_data != NULL || opt_servers_meta != NULL || opt_data_backend != NULL || opt_data_component != NULL || opt_data_path != NULL || opt_meta_backend != NULL || opt_meta_component != NULL || opt_meta_path != NULL))
	    || (opt_read && !opt_user && !opt_system)
	    || (!opt_read && (opt_servers_data == NULL || opt_servers_meta == NULL || opt_data_backend == NULL || opt_data_component == NULL || opt_data_path == NULL || opt_meta_backend == NULL || opt_meta_component == NULL || opt_meta_path == NULL))
	    || opt_max_connections < 0
	)
	{
		g_autofree gchar* help = NULL;

		help = g_option_context_get_help(context, TRUE, NULL);

		g_print("%s", help);

		return 1;
	}

	if (opt_user)
	{
		path = g_build_filename(g_get_user_config_dir(), "julea", opt_name, NULL);
	}
	else if (opt_system)
	{
		path = g_build_filename(g_get_system_config_dirs()[0], "julea", opt_name, NULL);
	}
	else
	{
		path = NULL;
	}

	if (opt_read)
	{
		ret = read_config(path);
	}
	else
	{
		ret = write_config(path);
	}

	return (ret) ? 0 : 1;
}
