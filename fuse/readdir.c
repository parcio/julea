/*
 * Copyright (c) 2010-2012 Michael Kuhn
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
#include <string.h>

int
jfs_readdir (char const* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi)
{
	JURI* uri;
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
	}
	else if (j_uri_get_collection(uri) != NULL)
	{
		JCollectionIterator* citerator;

		citerator = j_collection_iterator_new(j_uri_get_collection(uri), J_ITEM_STATUS_NONE);

		while (j_collection_iterator_next(citerator))
		{
			JItem* i = j_collection_iterator_get(citerator);

			filler(buf, j_item_get_name(i), NULL, 0);
			j_item_unref(i);
		}

		j_collection_iterator_free(citerator);

		ret = 0;
	}
	else if (j_uri_get_store(uri) != NULL)
	{
		JStoreIterator* siterator;

		siterator = j_store_iterator_new(j_uri_get_store(uri));

		while (j_store_iterator_next(siterator))
		{
			JCollection* c = j_store_iterator_get(siterator);

			filler(buf, j_collection_get_name(c), NULL, 0);
			j_collection_unref(c);
		}

		j_store_iterator_free(siterator);

		ret = 0;
	}
	else
	{
		filler(buf, "JULEA", NULL, 0);

		ret = 0;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
