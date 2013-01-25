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

#include <errno.h>

int jfs_create (char const* path, mode_t mode, struct fuse_file_info* fi)
{
	JBatch* operation;
	JURI* uri;
	int ret = -ENOENT;

	if ((uri = jfs_get_uri(path)) == NULL)
	{
		goto end;
	}

	if (j_uri_get(uri, NULL))
	{
		goto end;
	}
	else if (!jfs_uri_last(uri))
	{
		goto end;
	}

	operation = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (j_uri_get_collection(uri) != NULL)
	{
		JItem* item;

		item = j_item_new(j_uri_get_item_name(uri));
		j_collection_create_item(j_uri_get_collection(uri), item, operation);
		j_batch_execute(operation);

		ret = 0;
	}

	j_batch_unref(operation);

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
