/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#define FUSE_USE_VERSION 26
#define _XOPEN_SOURCE

#include <fuse.h>

#include <julea.h>
#include <juri.h>

#include <glib.h>

int jfs_access (char const*, int);
int jfs_chmod (char const*, mode_t);
int jfs_chown (char const*, uid_t, gid_t);
int jfs_create (char const*, mode_t, struct fuse_file_info*);
void jfs_destroy (void*);
int jfs_getattr (char const*, struct stat*);
void* jfs_init (struct fuse_conn_info*);
int jfs_link (char const*, char const*);
int jfs_mkdir(char const*, mode_t);
int jfs_open (char const*, struct fuse_file_info*);
int jfs_read (char const*, char*, size_t, off_t, struct fuse_file_info*);
int jfs_readdir (char const*, void*, fuse_fill_dir_t, off_t, struct fuse_file_info*);
int jfs_rmdir (char const*);
int jfs_statfs (char const*, struct statvfs*);
int jfs_truncate (char const*, off_t);
int jfs_unlink (char const*);
int jfs_utimens (char const*, const struct timespec*);
int jfs_write (char const*, char const*, size_t, off_t, struct fuse_file_info*);

JURI* jfs_get_uri (gchar const*);
gboolean jfs_uri_last (JURI*);
