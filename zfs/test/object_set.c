/*
 * Copyright (c) 2010-2012 Michael Kuhn
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


#include <glib.h>
#include "../jzfs.h"
#include "test.h"


static
void
test_object_set_create_destroy (void)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;

	pool = j_zfs_pool_open("jzfs");
	object_set = j_zfs_object_set_create(pool, "object_set");
	g_assert(object_set != 0);

	j_zfs_object_set_destroy(object_set);
	g_assert(object_set == NULL); 
	j_zfs_pool_close(pool);
	
}

static
void
test_object_set_open(void)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;

	pool = j_zfs_pool_open("jzfs");
	object_set = j_zfs_object_set_create(pool, "object_set");

	object_set = j_zfs_object_set_open(pool, "object_set");
	g_assert(object_set != 0);
	j_zfs_object_set_close(object_set);

	object_set = j_zfs_object_set_open(pool, "object_set");
	g_assert(object_set != 0);
	j_zfs_object_set_close(object_set);

	object_set = j_zfs_object_set_open(pool, "irgendwas");
	g_assert(object_set == 0);
	j_zfs_object_set_close(object_set);

	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);
}

void
test_object_set (void)
{
	g_test_add_func("/object_set/create_destroy", test_object_set_create_destroy);
	g_test_add_func("/object_set/open", test_object_set_open);	
}
