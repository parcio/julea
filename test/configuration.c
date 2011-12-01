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

#include <glib.h>

#include <julea.h>

#include <jconfiguration-internal.h>

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
	g_key_file_set_string_list(key_file, "servers", "data", servers, 1);
	g_key_file_set_string_list(key_file, "servers", "metadata", servers, 1);
	g_key_file_set_string(key_file, "storage", "backend", "null");
	g_key_file_set_string(key_file, "storage", "path", "");

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
	gchar const* data_servers[] = { "localhost", "local.host", NULL };
	gchar const* metadata_servers[] = { "localhost", NULL };

	key_file = g_key_file_new();
	g_key_file_set_string_list(key_file, "servers", "data", data_servers, 2);
	g_key_file_set_string_list(key_file, "servers", "metadata", metadata_servers, 1);
	g_key_file_set_string(key_file, "storage", "backend", "null");
	g_key_file_set_string(key_file, "storage", "path", "NULL");

	configuration = j_configuration_new_for_data(key_file);
	g_assert(configuration != NULL);

	g_assert_cmpstr(j_configuration_get_data_server(configuration, 0), ==, "localhost");
	g_assert_cmpstr(j_configuration_get_data_server(configuration, 1), ==, "local.host");
	g_assert_cmpuint(j_configuration_get_data_server_count(configuration), ==, 2);

	g_assert_cmpstr(j_configuration_get_metadata_server(configuration, 0), ==, "localhost");
	g_assert_cmpuint(j_configuration_get_metadata_server_count(configuration), ==, 1);

	g_assert_cmpstr(j_configuration_get_storage_backend(configuration), ==, "null");
	g_assert_cmpstr(j_configuration_get_storage_path(configuration), ==, "NULL");

	j_configuration_unref(configuration);

	g_key_file_free(key_file);
}

void
test_configuration (void)
{
	g_test_add_func("/julea/configuration/new_ref_unref", test_configuration_new_ref_unref);
	g_test_add_func("/julea/configuration/new_for_data", test_configuration_new_for_data);
	g_test_add_func("/julea/configuration/get", test_configuration_get);
}
