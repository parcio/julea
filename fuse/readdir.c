/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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
#include <string.h>

int
jfs_readdir(char const* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi)
{
	int ret = -ENOENT;

	JKVIterator* it;
	g_autofree gchar* prefix = NULL;

	(void)offset;
	(void)fi;

	if (g_strcmp0(path, "/") == 0)
	{
		prefix = g_strdup(path);
	}
	else
	{
		prefix = g_strdup_printf("%s/", path);
	}

	it = j_kv_iterator_new("posix", prefix);

	while (j_kv_iterator_next(it))
	{
		gconstpointer value;
		guint32 len;
		bson_t tmp[1];
		bson_iter_t iter;

		j_kv_iterator_get(it, &value, &len);
		bson_init_static(tmp, value, len);

		if (bson_iter_init_find(&iter, tmp, "name") && bson_iter_type(&iter) == BSON_TYPE_UTF8)
		{
			gchar const* name;

			name = bson_iter_utf8(&iter, NULL);
			filler(buf, name, NULL, 0);
		}
		else
		{
			filler(buf, "???", NULL, 0);
		}
	}

	j_kv_iterator_free(it);

	ret = 0;

	return ret;
}
