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

#include <glib.h>

struct fuse_operations jfs_vtable = {
	.access = jfs_access,
	.chmod = jfs_chmod,
	.chown = jfs_chown,
	.create = jfs_create,
	.destroy = jfs_destroy,
	.getattr = jfs_getattr,
	.init = jfs_init,
	.mkdir = jfs_mkdir,
	.read = jfs_read,
	.readdir = jfs_readdir,
	.rmdir = jfs_rmdir,
	.truncate = jfs_truncate,
	.unlink = jfs_unlink,
	.utimens = jfs_utimens,
	.write = jfs_write,
};

int
main(int argc, char** argv)
{
	gint ret;

	ret = fuse_main(argc, argv, &jfs_vtable, NULL);

	return ret;
}
