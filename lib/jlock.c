/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <bson.h>

#include <jlock-internal.h>

#include <jbackend.h>
#include <jbackend-internal.h>
#include <jcollection.h>
#include <jcollection-internal.h>
#include <jcommon-internal.h>
#include <jconnection-pool-internal.h>
#include <jhelper-internal.h>
#include <jitem.h>
#include <jitem-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JLock Lock
 *
 * Data structures and functions for managing locks.
 *
 * @{
 **/

/**
 * A JLock.
 **/
struct JLock
{
	/**
	 * The parent item.
	 **/
	JItem* item;

	GArray* blocks;

	gboolean acquired;
};

/**
 * Creates a new lock.
 *
 * \author Michael Kuhn
 *
 * \code
 * JItem* item;
 * JLock* lock;
 *
 * ...
 *
 * lock = j_lock_new(item);
 * \endcode
 *
 * \param item An item.
 *
 * \return A new item. Should be freed with j_lock_unref().
 **/
JLock*
j_lock_new (JItem* item)
{
	JLock* lock;

	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	lock = g_slice_new(JLock);
	lock->item = j_item_ref(item);
	lock->blocks = g_array_new(FALSE, FALSE, sizeof(guint64));
	lock->acquired = FALSE;

	j_trace_leave(G_STRFUNC);

	return lock;
}

/**
 * Frees the memory allocated for the lock.
 *
 * \author Michael Kuhn
 *
 * \code
 * JLock* lock;
 *
 * ...
 *
 * j_lock_free(lock);
 * \endcode
 *
 * \param lock A lock.
 **/
void
j_lock_free (JLock* lock)
{
	g_return_if_fail(lock != NULL);

	j_trace_enter(G_STRFUNC);

	if (lock->acquired)
	{
		j_lock_release(lock);
	}

	if (lock->item != NULL)
	{
		j_item_unref(lock->item);
	}

	g_array_free(lock->blocks, TRUE);

	g_slice_free(JLock, lock);

	j_trace_leave(G_STRFUNC);
}

gboolean
j_lock_acquire (JLock* lock)
{
	JBackend* meta_backend;
	bson_t empty[1];
	gboolean acquired = TRUE;
	gpointer meta_batch;

	g_return_val_if_fail(lock != NULL, FALSE);

	//j_lock_serialize(lock, obj);

	//write_concern = mongoc_write_concern_new();
	//write_concern->j = 1;
	//mongoc_write_concern_set_w(write_concern, 1);

	//bson_init(index);
	//bson_append_int32(index, "item", -1, 1);
	// FIXME speed
	//bson_append_int32(index, "blocks", -1, 1);

	//j_helper_create_index(J_STORE_COLLECTION_LOCKS, mongo_connection, index);

	bson_init(empty);

	meta_backend = j_metadata_backend();

	//mongo_connection = j_connection_pool_pop_meta(0);

	if (lock->blocks->len > 0)
	{
		if (meta_backend != NULL)
		{
			acquired = meta_backend->u.meta.batch_start("locks", &meta_batch);
		}

		for (guint i = 0; i < lock->blocks->len; i++)
		{
			gchar* block_str;
			guint64 block;

			block = g_array_index(lock->blocks, guint64, i);
			block_str = g_strdup_printf("%" G_GUINT64_FORMAT, block);

			if (meta_backend != NULL)
			{
				gchar* path;

				path = g_build_path("/", j_collection_get_name(j_item_get_collection(lock->item)), j_item_get_name(lock->item), block_str, NULL);
				acquired = meta_backend->u.meta.create(path, empty, meta_batch) && acquired;
				g_free(path);
			}

			g_free(block_str);
		}

		if (meta_backend != NULL)
		{
			acquired = meta_backend->u.meta.batch_execute(meta_batch) && acquired;
		}
	}

	lock->acquired = acquired;

	//j_connection_pool_push_meta(0, mongo_connection);

	bson_destroy(empty);

	return lock->acquired;
}

gboolean
j_lock_release (JLock* lock)
{
	JBackend* meta_backend;
	gboolean released = TRUE;
	gpointer meta_batch;

	g_return_val_if_fail(lock != NULL, FALSE);
	g_return_val_if_fail(lock->acquired, FALSE);

	//write_concern = mongoc_write_concern_new();
	//write_concern->j = 1;
	//mongoc_write_concern_set_w(write_concern, 1);

	//bson_init(obj);
	//bson_append_oid(obj, "_id", -1, &(lock->id));
	//bson_finish(obj);

	meta_backend = j_metadata_backend();
	//mongo_connection = j_connection_pool_pop_meta(0);

	if (lock->blocks->len > 0)
	{
		if (meta_backend != NULL)
		{
			released = meta_backend->u.meta.batch_start("locks", &meta_batch);
		}

		for (guint i = 0; i < lock->blocks->len; i++)
		{
			gchar* block_str;
			guint64 block;

			block = g_array_index(lock->blocks, guint64, i);
			block_str = g_strdup_printf("%" G_GUINT64_FORMAT, block);

			if (meta_backend != NULL)
			{
				gchar* path;

				path = g_build_path("/", j_collection_get_name(j_item_get_collection(lock->item)), j_item_get_name(lock->item), block_str, NULL);
				released = meta_backend->u.meta.delete(path, meta_batch) && released;
				g_free(path);
			}

			g_free(block_str);
		}

		if (meta_backend != NULL)
		{
			released = meta_backend->u.meta.batch_execute(meta_batch) && released;
		}
	}

	lock->acquired = !released;

	//j_connection_pool_push_meta(0, mongo_connection);

	return !(lock->acquired);
}

void
j_lock_add (JLock* lock, guint64 block)
{
	g_return_if_fail(lock != NULL);

	/* FIXME handle duplicates */
	g_array_append_val(lock->blocks, block);
}

/**
 * @}
 **/
