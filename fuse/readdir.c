/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include "juleafs.h"

#include <errno.h>
#include <string.h>

int
jfs_readdir (char const* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi)
{
	JURI* uri;
	int ret = -ENOENT;

	(void)offset;
	(void)fi;

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
		JItemIterator* citerator;

		citerator = j_item_iterator_new(j_uri_get_collection(uri));

		while (j_item_iterator_next(citerator))
		{
			JItem* i = j_item_iterator_get(citerator);

			filler(buf, j_item_get_name(i), NULL, 0);
			j_item_unref(i);
		}

		j_item_iterator_free(citerator);

		ret = 0;
	}
	else
	{
		JCollectionIterator* siterator;

		siterator = j_collection_iterator_new();

		while (j_collection_iterator_next(siterator))
		{
			JCollection* c = j_collection_iterator_get(siterator);

			filler(buf, j_collection_get_name(c), NULL, 0);
			j_collection_unref(c);
		}

		j_collection_iterator_free(siterator);

		ret = 0;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
