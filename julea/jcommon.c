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
#include <glib-object.h>
#include <gio/gio.h>

#include "jcommon.h"
#include "jcommon-internal.h"

/**
 * \defgroup JCommon Common
 * @{
 **/

static JCommon* j_common_global = NULL;

gboolean
j_init (void)
{
	GKeyFile* key_file;
	gchar* path;
	gchar const* const* dirs;
	guint i;

	g_return_val_if_fail(!j_is_initialized(), FALSE);

	key_file = g_key_file_new();
	path = g_build_filename(g_get_user_config_dir(), "julea", "julea", NULL);

	if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
	{
		j_init_for_data(key_file);

		goto out;
	}

	g_free(path);

	dirs = g_get_system_config_dirs();

	for (i = 0; dirs[i] != NULL; i++)
	{
		path = g_build_filename(dirs[i], "julea", "julea", NULL);

		if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
		{
			j_init_for_data(key_file);

			goto out;
		}

		g_free(path);
	}

	g_key_file_free(key_file);

	return FALSE;

out:
	g_key_file_free(key_file);
	g_free(path);

	return j_is_initialized();
}

gboolean
j_deinit (void)
{
	JCommon* p;

	g_return_val_if_fail(j_is_initialized(), FALSE);

	p = g_atomic_pointer_get(&j_common_global);
	g_atomic_pointer_set(&j_common_global, NULL);

	g_strfreev(p->data);
	g_strfreev(p->metadata);

	g_slice_free(JCommon, p);

	return TRUE;
}

gboolean
j_is_initialized (void)
{
	JCommon* p;

	p = g_atomic_pointer_get(&j_common_global);

	return (p != NULL);
}

JCommon*
j_common (void)
{
	JCommon* p;

	g_return_val_if_fail(j_is_initialized(), NULL);

	p = g_atomic_pointer_get(&j_common_global);

	return p;
}

/* Internal */

gboolean
j_init_for_data (GKeyFile* key_file)
{
	JCommon* common;
	gchar** data;
	gchar** metadata;

	g_return_val_if_fail(!j_is_initialized(), FALSE);
	g_return_val_if_fail(key_file != NULL, FALSE);

	data = g_key_file_get_string_list(key_file, "servers", "data", NULL, NULL);
	metadata = g_key_file_get_string_list(key_file, "servers", "metadata", NULL, NULL);

	if (data == NULL || data[0] == NULL || metadata == NULL || metadata[0] == NULL)
	{
		g_strfreev(data);
		g_strfreev(metadata);

		return FALSE;
	}

	common = g_slice_new(JCommon);
	common->data = data;
	common->metadata = metadata;
	common->data_len = g_strv_length(data);
	common->metadata_len = g_strv_length(metadata);

	g_atomic_pointer_set(&j_common_global, common);

	return TRUE;
}

/**
 * @}
 **/
