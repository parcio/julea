/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

int
jfs_read (char const* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi)
{
	JURI* uri;
	guint64 bytes_read;
	int ret = -ENOENT;

	if ((uri = jfs_get_uri(path)) == NULL)
	{
		goto end;
	}

	if (!j_uri_get(uri, NULL))
	{
		goto end;
	}

	if (j_uri_get_item(uri) != NULL)
	{
		JBatch* batch;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_item_read(j_uri_get_item(uri), buf, size, offset, &bytes_read, batch);
		j_batch_execute(batch);
		j_batch_unref(batch);

		ret = bytes_read;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
