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

#define FUSE_USE_VERSION 26
#define _XOPEN_SOURCE

#include <fuse.h>

#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>

#include <glib.h>

int jfs_access(char const*, int);
int jfs_chmod(char const*, mode_t);
int jfs_chown(char const*, uid_t, gid_t);
int jfs_create(char const*, mode_t, struct fuse_file_info*);
void jfs_destroy(void*);
int jfs_getattr(char const*, struct stat*);
void* jfs_init(struct fuse_conn_info*);
int jfs_link(char const*, char const*);
int jfs_mkdir(char const*, mode_t);
int jfs_open(char const*, struct fuse_file_info*);
int jfs_read(char const*, char*, size_t, off_t, struct fuse_file_info*);
int jfs_readdir(char const*, void*, fuse_fill_dir_t, off_t, struct fuse_file_info*);
int jfs_rmdir(char const*);
int jfs_statfs(char const*, struct statvfs*);
int jfs_truncate(char const*, off_t);
int jfs_unlink(char const*);
int jfs_utimens(char const*, const struct timespec*);
int jfs_write(char const*, char const*, size_t, off_t, struct fuse_file_info*);
