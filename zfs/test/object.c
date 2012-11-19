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
#include <string.h>
#include "../jzfs.h"
#include "test.h"


static
void
test_object_create_destroy (void)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;
	JZFSObject* object1;
	JZFSObject* object2;
	guint64 object_id1, object_id2, size;

	pool = j_zfs_pool_open("jzfs");
	g_assert(pool != 0);
	object_set = j_zfs_object_set_create(pool, "object_set");
	g_assert(object_set != 0);

	object1 = j_zfs_object_create(object_set);
	g_assert(object1 != 0);
	size = j_zfs_object_get_size(object1);
	g_assert(size == 0);
	object_id1 = j_zfs_get_object_id(object1);	
	g_assert(object_id1 != 0);
	
	object2 = j_zfs_object_create(object_set);
	g_assert(object2 != 0);	
	object_id2 = j_zfs_get_object_id(object2);	
	g_assert(object_id2 != 0);
	
	g_assert(object_id1 != object_id2);

	j_zfs_object_destroy(object1);
	j_zfs_object_destroy(object2);
	j_zfs_object_set_destroy(object_set);

	object1 = j_zfs_object_create(object_set);
	g_assert(object1 == 0);
	
	j_zfs_pool_close(pool);
}

static
void
test_object_open_close(void)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;
	JZFSObject* object;

	pool = j_zfs_pool_open("jzfs");
	g_assert(pool != 0);
	object_set = j_zfs_object_set_create(pool, "object_set");
	object = j_zfs_object_create(object_set);
	
	guint64 object_id = j_zfs_get_object_id(object);
	object = j_zfs_object_open(object_set, object_id);
	g_assert(object != 0);

	object = j_zfs_object_open(object_set, 1234567);
	g_assert(object == 0);	

	j_zfs_object_close(object);
	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);
}

static
void
test_object_read_write(void)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;
	JZFSObject* object;
	gchar dummy[] = "abcdefghij";
	gchar dummy2[] = "\0\0\0\0\0\0\0\0\0\0";
	gchar dummy3[] = "Hello World.";
	int ret;
	guint64 size;

	pool = j_zfs_pool_open("jzfs");
	g_assert(pool != 0);
	object_set = j_zfs_object_set_create(pool, "object_set");
	object = j_zfs_object_create(object_set);

	j_zfs_object_read(object, dummy, 10, 0); //read new object
	ret = memcmp(dummy, dummy2, 10); 
	g_assert(ret == 0);	

	j_zfs_object_write(object, dummy3, 12, 0);
	size = j_zfs_object_get_size(object);
	g_assert(size == 12);

	j_zfs_object_read(object, dummy, 12, 0);
	ret = memcmp(dummy, dummy3, 12);
	g_assert(ret==0);

	j_zfs_object_set_size(object, 8); 
	size = j_zfs_object_get_size(object);
	g_assert(size == 8);

	j_zfs_object_destroy(object);
	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);	
}

static 
void
test_object_truncate(void)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;
	JZFSObject* object;
	int i, ret;

	gchar dummy[] = "Hello world.";
	gchar dummy2[] = "\0\0\0\0\0\0\0\0\0\0";
	pool = j_zfs_pool_open("jzfs");
	g_assert(pool != 0);
	object_set = j_zfs_object_set_create(pool, "object_set");
	object = j_zfs_object_create(object_set);

	j_zfs_object_write(object, dummy, 10, 0);
	j_zfs_object_set_size(object, 0); 
	j_zfs_object_read(object, dummy, 10, 0);
	
	ret = memcmp(dummy, dummy2, 10); 
	g_assert(ret == 0);	

	j_zfs_object_destroy(object);
	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);	
}

void
test_object (void)
{
	g_test_add_func("/object/create_destroy", test_object_create_destroy);
	g_test_add_func("/object/open_close", test_object_open_close);
	g_test_add_func("/object/read_write", test_object_read_write);
	g_test_add_func("/object/truncate", test_object_truncate);
}
