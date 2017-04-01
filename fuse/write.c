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

int
jfs_write (char const* path, char const* buf, size_t size, off_t offset, struct fuse_file_info* fi)
{
	JURI* uri;
	guint64 bytes_written;
	int ret = -ENOENT;

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
		g_autoptr(JBatch) batch = NULL;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_item_write(j_uri_get_item(uri), buf, size, offset, &bytes_written, batch);
		j_batch_execute(batch);

		ret = bytes_written;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
