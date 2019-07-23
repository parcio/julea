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

#include <julea.h>

#include <jconfiguration.h>

#include "test.h"

static
void
test_configuration_new_ref_unref (void)
{
	JConfiguration* configuration;

	configuration = j_configuration_new();
	g_assert(configuration != NULL);
	j_configuration_ref(configuration);
	j_configuration_unref(configuration);
	j_configuration_unref(configuration);
}

static
void
test_configuration_new_for_data (void)
{
	JConfiguration* configuration;
	GKeyFile* key_file;
	gchar const* servers[] = { "localhost", NULL };

	key_file = g_key_file_new();
	g_key_file_set_string_list(key_file, "servers", "object", servers, 1);
	g_key_file_set_string_list(key_file, "servers", "kv", servers, 1);
	g_key_file_set_string_list(key_file, "servers", "db", servers, 1);
	g_key_file_set_string(key_file, "object", "backend", "null");
	g_key_file_set_string(key_file, "object", "component", "server");
	g_key_file_set_string(key_file, "object", "path", "");
	g_key_file_set_string(key_file, "kv", "backend", "null");
	g_key_file_set_string(key_file, "kv", "component", "server");
	g_key_file_set_string(key_file, "kv", "path", "");
	g_key_file_set_string(key_file, "db", "backend", "null");
	g_key_file_set_string(key_file, "db", "component", "server");
	g_key_file_set_string(key_file, "db", "path", "");

	configuration = j_configuration_new_for_data(key_file);
	g_assert(configuration != NULL);
	j_configuration_unref(configuration);

	g_key_file_free(key_file);
}

static
void
test_configuration_get (void)
{
	JConfiguration* configuration;
	GKeyFile* key_file;
	gchar const* object_servers[] = { "localhost", "local.host", NULL };
	gchar const* kv_servers[] = { "localhost", NULL };
	gchar const* db_servers[] = { "localhost", "host.local", NULL };

	key_file = g_key_file_new();
	g_key_file_set_string_list(key_file, "servers", "object", object_servers, 2);
	g_key_file_set_string_list(key_file, "servers", "kv", kv_servers, 1);
	g_key_file_set_string_list(key_file, "servers", "db", db_servers, 2);
	g_key_file_set_string(key_file, "object", "backend", "null");
	g_key_file_set_string(key_file, "object", "component", "server");
	g_key_file_set_string(key_file, "object", "path", "NULL");
	g_key_file_set_string(key_file, "kv", "backend", "null2");
	g_key_file_set_string(key_file, "kv", "component", "client");
	g_key_file_set_string(key_file, "kv", "path", "NULL2");
	g_key_file_set_string(key_file, "db", "backend", "null3");
	g_key_file_set_string(key_file, "db", "component", "client");
	g_key_file_set_string(key_file, "db", "path", "NULL3");

	configuration = j_configuration_new_for_data(key_file);
	g_assert(configuration != NULL);

	g_assert_cmpstr(j_configuration_get_object_server(configuration, 0), ==, "localhost");
	g_assert_cmpstr(j_configuration_get_object_server(configuration, 1), ==, "local.host");
	g_assert_cmpuint(j_configuration_get_object_server_count(configuration), ==, 2);

	g_assert_cmpstr(j_configuration_get_kv_server(configuration, 0), ==, "localhost");
	g_assert_cmpuint(j_configuration_get_kv_server_count(configuration), ==, 1);

	g_assert_cmpstr(j_configuration_get_db_server(configuration, 0), ==, "localhost");
	g_assert_cmpstr(j_configuration_get_db_server(configuration, 1), ==, "host.local");
	g_assert_cmpuint(j_configuration_get_db_server_count(configuration), ==, 2);

	g_assert_cmpstr(j_configuration_get_object_backend(configuration), ==, "null");
	g_assert_cmpstr(j_configuration_get_object_component(configuration), ==, "server");
	g_assert_cmpstr(j_configuration_get_object_path(configuration), ==, "NULL");

	g_assert_cmpstr(j_configuration_get_kv_backend(configuration), ==, "null2");
	g_assert_cmpstr(j_configuration_get_kv_component(configuration), ==, "client");
	g_assert_cmpstr(j_configuration_get_kv_path(configuration), ==, "NULL2");

	g_assert_cmpstr(j_configuration_get_db_backend(configuration), ==, "null3");
	g_assert_cmpstr(j_configuration_get_db_component(configuration), ==, "client");
	g_assert_cmpstr(j_configuration_get_db_path(configuration), ==, "NULL3");

	j_configuration_unref(configuration);

	g_key_file_free(key_file);
}

void
test_configuration (void)
{
	g_test_add_func("/configuration/new_ref_unref", test_configuration_new_ref_unref);
	g_test_add_func("/configuration/new_for_data", test_configuration_new_for_data);
	g_test_add_func("/configuration/get", test_configuration_get);
}
