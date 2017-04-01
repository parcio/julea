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

int jfs_create (char const* path, mode_t mode, struct fuse_file_info* fi)
{
	g_autoptr(JBatch) batch = NULL;
	JURI* uri;
	int ret = -ENOENT;

	(void)mode;
	(void)fi;

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

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (j_uri_get_collection(uri) != NULL)
	{
		g_autoptr(JItem) item = NULL;

		item = j_item_create(j_uri_get_collection(uri), j_uri_get_item_name(uri), NULL, batch);
		j_batch_execute(batch);

		ret = 0;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
