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
test_object_set_setup(JZFSPool** pool, gconstpointer data)
{
	*pool = j_zfs_pool_open("jzfs");
	g_assert(pool != 0);
}

static 
void
test_object_set_teardown(JZFSPool** pool, gconstpointer data)
{
	j_zfs_pool_close(*pool);	
}

static
void
test_object_set_create_destroy (JZFSPool** pool, gconstpointer data)
{
	JZFSObjectSet* object_set;

	object_set = j_zfs_object_set_create(*pool, "object_set");
	g_assert(object_set != 0);

	j_zfs_object_set_destroy(object_set);
}

static
void
test_object_set_open(JZFSPool** pool, gconstpointer data)
{
	JZFSObjectSet* object_set;
	JZFSObjectSet* object_set2;

	object_set = j_zfs_object_set_create(*pool, "object_set");
	j_zfs_object_set_close(object_set);

	object_set = j_zfs_object_set_open(*pool, "object_set");
	g_assert(object_set != 0);
	j_zfs_object_set_destroy(object_set);

	object_set2 = j_zfs_object_set_open(*pool, "irgendwas"); //open with unknown name
	g_assert(object_set2 == 0);
}

void
test_object_set (void)
{
	g_test_add("/object_set/create_destroy", JZFSPool*, NULL, test_object_set_setup, test_object_set_create_destroy, test_object_set_teardown);
	g_test_add("/object_set/open", JZFSPool*, NULL, test_object_set_setup, test_object_set_open, test_object_set_teardown);	
}
