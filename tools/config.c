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

#include <julea-config.h>

#include <glib.h>
#include <glib-object.h>
#include <gio/gio.h>

#include <string.h>

static gboolean opt_user = FALSE;
static gboolean opt_system = FALSE;
static gboolean opt_read = FALSE;
static gchar const* opt_name = "julea";
static gchar const* opt_servers_object = NULL;
static gchar const* opt_servers_kv = NULL;
static gchar const* opt_servers_db = NULL;
static gchar const* opt_object_backend = NULL;
static gchar const* opt_object_component = NULL;
static gchar const* opt_object_path = NULL;
static gchar const* opt_kv_backend = NULL;
static gchar const* opt_kv_component = NULL;
static gchar const* opt_kv_path = NULL;
static gchar const* opt_db_backend = NULL;
static gchar const* opt_db_component = NULL;
static gchar const* opt_db_path = NULL;
static gint64 opt_max_operation_size = 0;
static gint opt_max_connections = 0;
static gint64 opt_stripe_size = 0;

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
	g_auto(GStrv) servers_object = NULL;
	g_auto(GStrv) servers_kv = NULL;
	g_auto(GStrv) servers_db = NULL;

	servers_object = string_split(opt_servers_object);
	servers_kv = string_split(opt_servers_kv);
	servers_db = string_split(opt_servers_db);

	key_file = g_key_file_new();
	g_key_file_set_int64(key_file, "core", "max-operation-size", opt_stripe_size);
	g_key_file_set_integer(key_file, "clients", "max-connections", opt_max_connections);
	g_key_file_set_int64(key_file, "clients", "stripe-size", opt_stripe_size);
	g_key_file_set_string_list(key_file, "servers", "object", (gchar const* const*)servers_object, g_strv_length(servers_object));
	g_key_file_set_string_list(key_file, "servers", "kv", (gchar const* const*)servers_kv, g_strv_length(servers_kv));
	g_key_file_set_string_list(key_file, "servers", "db", (gchar const* const*)servers_db, g_strv_length(servers_db));
	g_key_file_set_string(key_file, "object", "backend", opt_object_backend);
	g_key_file_set_string(key_file, "object", "component", opt_object_component);
	g_key_file_set_string(key_file, "object", "path", opt_object_path);
	g_key_file_set_string(key_file, "kv", "backend", opt_kv_backend);
	g_key_file_set_string(key_file, "kv", "component", opt_kv_component);
	g_key_file_set_string(key_file, "kv", "path", opt_kv_path);
	g_key_file_set_string(key_file, "db", "backend", opt_db_backend);
	g_key_file_set_string(key_file, "db", "component", opt_db_component);
	g_key_file_set_string(key_file, "db", "path", opt_db_path);
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
		{ "object-servers", 0, 0, G_OPTION_ARG_STRING, &opt_servers_object, "Object servers to use", "host1,host2:port" },
		{ "kv-servers", 0, 0, G_OPTION_ARG_STRING, &opt_servers_kv, "Key-value servers to use", "host1,host2:port" },
		{ "db-servers", 0, 0, G_OPTION_ARG_STRING, &opt_servers_db, "Key-value servers to use", "host1,host2:port" },
		{ "object-backend", 0, 0, G_OPTION_ARG_STRING, &opt_object_backend, "Object backend to use", "posix|null|gio|…" },
		{ "object-component", 0, 0, G_OPTION_ARG_STRING, &opt_object_component, "Object component to use", "client|server" },
		{ "object-path", 0, 0, G_OPTION_ARG_STRING, &opt_object_path, "Object path to use", "/path/to/storage" },
		{ "kv-backend", 0, 0, G_OPTION_ARG_STRING, &opt_kv_backend, "Key-value backend to use", "posix|null|gio|…" },
		{ "kv-component", 0, 0, G_OPTION_ARG_STRING, &opt_kv_component, "Key-value component to use", "client|server" },
		{ "kv-path", 0, 0, G_OPTION_ARG_STRING, &opt_kv_path, "Key-value path to use", "/path/to/storage" },
		{ "db-backend", 0, 0, G_OPTION_ARG_STRING, &opt_db_backend, "Database backend to use", "sqlite|null|…" },
		{ "db-component", 0, 0, G_OPTION_ARG_STRING, &opt_db_component, "Key-value component to use", "client|server" },
		{ "db-path", 0, 0, G_OPTION_ARG_STRING, &opt_db_path, "Key-value path to use", "/path/to/storage" },
		{ "max-operation-size", 0, 0, G_OPTION_ARG_INT64, &opt_max_operation_size, "Maximum size of an operation", "0" },
		{ "max-connections", 0, 0, G_OPTION_ARG_INT, &opt_max_connections, "Maximum number of connections", "0" },
		{ "stripe-size", 0, 0, G_OPTION_ARG_INT64, &opt_stripe_size, "Default stripe size", "0" },
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
	    || (opt_read && (opt_servers_object != NULL || opt_servers_kv != NULL || opt_servers_db != NULL || opt_object_backend != NULL || opt_object_component != NULL || opt_object_path != NULL || opt_kv_backend != NULL || opt_kv_component != NULL || opt_kv_path != NULL || opt_db_backend != NULL || opt_db_component != NULL || opt_db_path != NULL))
	    || (opt_read && !opt_user && !opt_system)
	    || (!opt_read && (opt_servers_object == NULL || opt_servers_kv == NULL || opt_servers_db == NULL || opt_object_backend == NULL || opt_object_component == NULL || opt_object_path == NULL || opt_kv_backend == NULL || opt_kv_component == NULL || opt_kv_path == NULL || opt_db_backend == NULL || opt_db_component == NULL || opt_db_path == NULL))
	    || opt_max_operation_size < 0
	    || opt_max_connections < 0
	    || opt_stripe_size < 0
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
