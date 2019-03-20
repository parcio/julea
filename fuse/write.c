/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include "julea-fuse.h"

#include <errno.h>

int
jfs_write (char const* path, char const* buf, size_t size, off_t offset, struct fuse_file_info* fi)
{
	int ret = -ENOENT;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autoptr(JObject) object = NULL;
	bson_t file[1];
	guint64 bytes_written;

	(void)fi;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_POSIX);
	kv = j_kv_new("posix", path);
	object = j_object_new("posix", path);

	j_kv_get(kv, file, batch);
	j_object_write(object, buf, size, offset, &bytes_written, batch);

	if (j_batch_execute(batch))
	{
		bson_iter_t iter;
		gint64 old_size = 0;

		if (bson_iter_init_find(&iter, file, "size") && bson_iter_type(&iter) == BSON_TYPE_INT64)
		{
			old_size = bson_iter_int64(&iter);

			if ((guint64)old_size < offset + size)
			{
				bson_iter_overwrite_int64(&iter, offset + size);

				j_kv_put(kv, bson_copy(file), batch);
				j_batch_execute(batch);
			}
		}

		ret = bytes_written;

		bson_destroy(file);
	}

	return ret;
}
