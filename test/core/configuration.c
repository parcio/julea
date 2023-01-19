/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
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

static void
test_configuration_new_ref_unref(void)
{
	JConfiguration* configuration;

	J_TEST_TRAP_START;
	configuration = j_configuration_new();
	g_assert_true(configuration != NULL);
	j_configuration_ref(configuration);
	j_configuration_unref(configuration);
	j_configuration_unref(configuration);
	J_TEST_TRAP_END;
}

static void
test_configuration_new_for_data(void)
{
	JConfiguration* configuration;
	GKeyFile* key_file;
	gchar const* servers[] = { "localhost", NULL };

	J_TEST_TRAP_START;
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
	g_key_file_set_string(key_file, "object.hsm-policy", "kv_backend", "null");
	g_key_file_set_string(key_file, "object.hsm-policy", "kv_path", "null");
	g_key_file_set_string(key_file, "object.hsm-policy", "policy", "null");

	configuration = j_configuration_new_for_data(key_file);
	g_assert_true(configuration != NULL);
	j_configuration_unref(configuration);

	g_key_file_free(key_file);
	J_TEST_TRAP_END;
}

static void
test_configuration_get(void)
{
	JConfiguration* configuration;
	GKeyFile* key_file;
	gchar const* object_servers[] = { "localhost", "local.host", NULL };
	gchar const* kv_servers[] = { "localhost", NULL };
	gchar const* db_servers[] = { "localhost", "host.local", NULL };

	J_TEST_TRAP_START;
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
	g_key_file_set_string(key_file, "object.hsm-policy", "kv_backend", "NULL4");
	g_key_file_set_string(key_file, "object.hsm-policy", "kv_path", "NULL5");
	g_key_file_set_string(key_file, "object.hsm-policy", "policy", "NULL6");

	configuration = j_configuration_new_for_data(key_file);
	g_assert_true(configuration != NULL);

	g_assert_cmpstr(j_configuration_get_server(configuration, J_BACKEND_TYPE_OBJECT, 0), ==, "localhost");
	g_assert_cmpstr(j_configuration_get_server(configuration, J_BACKEND_TYPE_OBJECT, 1), ==, "local.host");
	g_assert_cmpuint(j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT), ==, 2);

	g_assert_cmpstr(j_configuration_get_server(configuration, J_BACKEND_TYPE_KV, 0), ==, "localhost");
	g_assert_cmpuint(j_configuration_get_server_count(configuration, J_BACKEND_TYPE_KV), ==, 1);

	g_assert_cmpstr(j_configuration_get_server(configuration, J_BACKEND_TYPE_DB, 0), ==, "localhost");
	g_assert_cmpstr(j_configuration_get_server(configuration, J_BACKEND_TYPE_DB, 1), ==, "host.local");
	g_assert_cmpuint(j_configuration_get_server_count(configuration, J_BACKEND_TYPE_DB), ==, 2);

	g_assert_cmpstr(j_configuration_get_backend(configuration, J_BACKEND_TYPE_OBJECT), ==, "null");
	g_assert_cmpstr(j_configuration_get_backend_component(configuration, J_BACKEND_TYPE_OBJECT), ==, "server");
	g_assert_cmpstr(j_configuration_get_backend_path(configuration, J_BACKEND_TYPE_OBJECT), ==, "NULL");

	g_assert_cmpstr(j_configuration_get_backend(configuration, J_BACKEND_TYPE_KV), ==, "null2");
	g_assert_cmpstr(j_configuration_get_backend_component(configuration, J_BACKEND_TYPE_KV), ==, "client");
	g_assert_cmpstr(j_configuration_get_backend_path(configuration, J_BACKEND_TYPE_KV), ==, "NULL2");

	g_assert_cmpstr(j_configuration_get_backend(configuration, J_BACKEND_TYPE_DB), ==, "null3");
	g_assert_cmpstr(j_configuration_get_backend_component(configuration, J_BACKEND_TYPE_DB), ==, "client");
	g_assert_cmpstr(j_configuration_get_backend_path(configuration, J_BACKEND_TYPE_DB), ==, "NULL3");

	g_assert_cmpstr(j_configuration_get_object_policy_kv_backend(configuration), ==, "NULL4");
	g_assert_cmpstr(j_configuration_get_object_policy_kv_path(configuration), ==, "NULL5");
	g_assert_cmpstr(j_configuration_get_object_policy(configuration), ==, "NULL6");

	j_configuration_unref(configuration);

	g_key_file_free(key_file);
	J_TEST_TRAP_END;
}

void
test_core_configuration(void)
{
	g_test_add_func("/core/configuration/new_ref_unref", test_configuration_new_ref_unref);
	g_test_add_func("/core/configuration/new_for_data", test_configuration_new_for_data);
	g_test_add_func("/core/configuration/get", test_configuration_get);
}
