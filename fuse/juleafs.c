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

#include "juleafs.h"

#include <glib.h>
#include <glib-object.h>

#include <string.h>

JConnection* jfs_connection = NULL;

struct fuse_operations jfs_vtable = {
	.access   = jfs_access,
	.chmod    = jfs_chmod,
	.chown    = jfs_chown,
	.create   = jfs_create,
	.destroy  = jfs_destroy,
	.getattr  = jfs_getattr,
	.init     = jfs_init,
	.mkdir    = jfs_mkdir,
	.read     = jfs_read,
	.readdir  = jfs_readdir,
	.rmdir    = jfs_rmdir,
	.truncate = jfs_truncate,
	.unlink   = jfs_unlink,
	.utimens  = jfs_utimens,
	.write    = jfs_write,
};

gchar**
jfs_path_components (gchar const* path)
{
	return g_strsplit(path + 1, "/", 0);
}

guint
jfs_path_depth (gchar const* path)
{
	gchar const* c;
	guint depth = 0;

	if (strcmp(path, "/") == 0)
	{
		return 0;
	}

	for (c = path; *c != '\0'; c++)
	{
		if (*c == '/')
		{
			depth++;
		}
	}

	return depth;
}

guint
jfs_path_parse (gchar const* path, JStore** store, JCollection** collection, JItem** item)
{
	JOperation* operation;
	gchar** components;
	guint depth;

	*store = NULL;
	*collection = NULL;
	*item = NULL;

	operation = j_operation_new();
	depth = jfs_path_depth(path);
	components = jfs_path_components(path);

	if (depth > 0)
	{
		/* FIXME */
		if (components[0][0] == '.')
		{
			goto end;
		}

		*store = j_store_new(jfs_connection, components[0]);
		j_store_get(*store, operation);
		j_operation_execute(operation);

		if (*store == NULL)
		{
			goto end;
		}
	}

	if (depth > 1)
	{
		/* FIXME */
		if (components[1][0] == '.')
		{
			goto end;
		}

		j_store_get_collection(*store, collection, components[1], operation);
		j_operation_execute(operation);

		if (*collection == NULL)
		{
			goto end;
		}
	}

	if (depth > 2)
	{
		/* FIXME */
		if (components[2][0] == '.')
		{
			goto end;
		}

		j_collection_get_item(*collection, item, components[2], operation);
		j_operation_execute(operation);

		if (*item == NULL)
		{
			goto end;
		}
	}

end:
	g_strfreev(components);

	return depth;
}

int
main (int argc, char** argv)
{
	gint ret;

	g_type_init();

	if (!j_init())
	{
		g_printerr("Could not initialize.\n");
		return 1;
	}

	jfs_connection = j_connection_new();

	if (!j_connection_connect(jfs_connection))
	{
		g_printerr("Could not connect.\n");
		j_connection_unref(jfs_connection);

		return 1;
	}

	ret = fuse_main(argc, argv, &jfs_vtable, NULL);

	j_connection_disconnect(jfs_connection);
	j_connection_unref(jfs_connection);

	j_fini();

	return ret;
}
