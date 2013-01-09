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



#include <glib.h>
#include <string.h>
#include <stdio.h>
#include "../jzfs.h"
#include "test.h"


struct Arg //arguments
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;
};

typedef struct Arg Arg;

static
void
test_object_setup(Arg* arg, gconstpointer data)
{
	JZFSPool* pool;
	JZFSObjectSet* object_set;

	pool = j_zfs_pool_open("jzfs");
	g_assert(pool != 0);

	object_set = j_zfs_object_set_create(pool, "object_set");
	g_assert(object_set != 0);

	arg->pool = pool;
	arg->object_set = object_set;
}

static 
void
test_object_teardown(Arg* arg, gconstpointer data)
{
	j_zfs_object_set_destroy(arg->object_set);
	j_zfs_pool_close(arg->pool);	
}

static
void
test_object_create_destroy (Arg* arg, gconstpointer data)
{
	JZFSObject* object1;
	JZFSObject* object2;
	guint64 object_id1, object_id2, size;

	object1 = j_zfs_object_create(arg->object_set);
	g_assert(object1 != 0);
	size = j_zfs_object_get_size(object1);
	g_assert(size == 0); //new object has size 0
	object_id1 = j_zfs_get_object_id(object1);	
	g_assert(object_id1 != 0); //id of new object != 0
	
	object2 = j_zfs_object_create(arg->object_set);
	g_assert(object2 != 0);	
	object_id2 = j_zfs_get_object_id(object2);	
	g_assert(object_id2 != 0);
	
	g_assert(object_id1 != object_id2); //two different objects have different object ids

	j_zfs_object_destroy(object1);
	j_zfs_object_destroy(object2);
}

static
void
test_object_open_close(Arg* arg, gconstpointer data)
{
	JZFSObject* object;
	JZFSObject* object2;

	object = j_zfs_object_create(arg->object_set);
	
	guint64 object_id = j_zfs_get_object_id(object);
	object = j_zfs_object_open(arg->object_set, object_id);
	g_assert(object != 0);
	j_zfs_object_destroy(object);

	object2 = j_zfs_object_open(arg->object_set, 1234567); //open object with unknown object id
	g_assert(object2 == 0);		
	object2 = j_zfs_object_open(arg->object_set, 1234567);
	g_assert(object2 == 0);	
}

static
void
test_object_read_write(Arg* arg, gconstpointer data)
{
	JZFSObject* object;
	gchar dummy[] = "abcdefghij";
	gchar dummy2[] = "\0\0\0\0\0\0\0\0\0\0";
	gchar dummy3[] = "Hello World.";
	int ret;
	guint64 size;

	object = j_zfs_object_create(arg->object_set);

	j_zfs_object_read(object, dummy, 10, 0); //read empty object, content should be zeroes
	ret = memcmp(dummy, dummy2, 10); 
	g_assert(ret == 0);	

	j_zfs_object_write(object, dummy3, 12, 0);
	size = j_zfs_object_get_size(object);
	g_assert(size == 12);	// size = bytes written

	j_zfs_object_read(object, dummy, 12, 0);
	ret = memcmp(dummy, dummy3, 12); //read non-empty object, should be "Hello World."
	g_assert(ret==0);

	j_zfs_object_destroy(object);	
}

static 
void
test_object_truncate(Arg* arg, gconstpointer data)
{
	JZFSObject* object;
	int i, ret;
	guint64 size;

	gchar dummy[] = "Hello world.";
	gchar dummy2[] = "\0\0\0\0\0\0\0\0\0\0\0";
	object = j_zfs_object_create(arg->object_set);

	j_zfs_object_set_size(object, 8); //truncate
	size = j_zfs_object_get_size(object);
	g_assert(size == 8); // assert correct size

	j_zfs_object_write(object, dummy, 10, 0);
	j_zfs_object_set_size(object, 0); //truncate
	j_zfs_object_read(object, dummy, 10, 0);
	
	ret = memcmp(dummy, dummy2, 10); 
	if(ret!=0)
		printf("Dummy: %s\n", dummy);	
	g_assert(ret == 0);	

	j_zfs_object_destroy(object);
}

void
test_object (void)
{
	g_test_add("/object/create_destroy", Arg, NULL, test_object_setup, test_object_create_destroy, test_object_teardown);
	g_test_add("/object/open_close", Arg, NULL, test_object_setup, test_object_open_close, test_object_teardown);
	g_test_add("/object/read_write", Arg, NULL, test_object_setup, test_object_read_write, test_object_teardown);
	g_test_add("/object/truncate", Arg, NULL, test_object_setup, test_object_truncate, test_object_teardown);
}
