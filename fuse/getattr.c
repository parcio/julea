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
jfs_getattr (char const* path, struct stat* stbuf)
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
		g_autoptr(JBatch) batch = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_item_get_status(j_uri_get_item(uri), batch);
		j_batch_execute(batch);

		stbuf->st_mode = S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
		stbuf->st_nlink = 1;
		stbuf->st_uid = 0;
		stbuf->st_gid = 0;
		stbuf->st_size = j_item_get_size(j_uri_get_item(uri));
		stbuf->st_atime = stbuf->st_ctime = stbuf->st_mtime = j_item_get_modification_time(j_uri_get_item(uri)) / G_USEC_PER_SEC;

		ret = 0;
	}
	else
	{
		stbuf->st_mode = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
		stbuf->st_nlink = 1;
		stbuf->st_uid = 0;
		stbuf->st_gid = 0;
		stbuf->st_size = 0;
		stbuf->st_atime = stbuf->st_ctime = stbuf->st_mtime = 0;

		ret = 0;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
