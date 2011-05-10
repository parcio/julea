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

#include <errno.h>
#include <string.h>

int
jfs_readdir (char const* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi)
{
	JCollection* collection = NULL;
	JItem* item = NULL;
	JStore* store = NULL;
	guint depth;
	int ret = -ENOENT;

	depth = jfs_path_parse(path, &store, &collection, &item);

	if (depth > 2)
	{
		goto end;
	}

	if (depth == 0)
	{
		filler(buf, "JULEA", NULL, 0);

		ret = 0;
	}
	else if (depth == 1)
	{
		JStoreIterator* siterator;

		siterator = j_store_iterator_new(store);

		while (j_store_iterator_next(siterator))
		{
			JCollection* c = j_store_iterator_get(siterator);

			filler(buf, j_collection_name(c), NULL, 0);
			j_collection_unref(c);
		}

		j_store_iterator_free(siterator);

		ret = 0;
	}
	else if (depth == 2)
	{
		JCollectionIterator* citerator;

		citerator = j_collection_iterator_new(collection);

		while (j_collection_iterator_next(citerator))
		{
			JItem* i = j_collection_iterator_get(citerator);

			filler(buf, j_item_name(i), NULL, 0);
			j_item_unref(i);
		}

		j_collection_iterator_free(citerator);

		ret = 0;
	}

end:
	if (collection != NULL)
	{
		j_collection_unref(collection);
	}

	if (store != NULL)
	{
		j_store_unref(store);
	}

	if (item != NULL)
	{
		j_item_unref(item);
	}

	return ret;
}
