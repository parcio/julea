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

#include "juleafs.h"

#include <glib.h>

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

JURI*
jfs_get_uri (gchar const* path)
{
	JURI* uri;
	gchar* new_path;

	new_path = g_strconcat("julea://", path + 1, NULL);
	uri = j_uri_new(new_path);
	g_free(new_path);

	return uri;
}

gboolean
jfs_uri_last (JURI* uri)
{
	gboolean ret = FALSE;

	if (j_uri_get_store(uri) == NULL && j_uri_get_store_name(uri) != NULL)
	{
		ret = (j_uri_get_collection_name(uri) == NULL && j_uri_get_item_name(uri) == NULL);
	}
	else if (j_uri_get_collection(uri) == NULL && j_uri_get_collection_name(uri) != NULL)
	{
		ret = (j_uri_get_item_name(uri) == NULL);
	}
	else if (j_uri_get_item(uri) == NULL && j_uri_get_item_name(uri) != NULL)
	{
		ret = TRUE;
	}

	return ret;
}

int
main (int argc, char** argv)
{
	gint ret;

	j_init();

	ret = fuse_main(argc, argv, &jfs_vtable, NULL);

	j_fini();

	return ret;
}
