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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <bson.h>
#include <mongo.h>

#include <jlock-internal.h>

#include <jcollection.h>
#include <jcollection-internal.h>
#include <jconnection-pool-internal.h>
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
	 * The ID.
	 **/
	bson_oid_t id;

	/**
	 * The parent item.
	 **/
	JItem* item;

	GArray* blocks;

	gboolean acquired;
};

/**
 * Serializes a lock.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param lock A lock.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
static
void
j_lock_serialize (JLock* lock, bson* b)
{
	// FIXME might be too small
	gchar numstr[10];

	g_return_if_fail(lock != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC);

	bson_init(b);
	bson_append_oid(b, "_id", &(lock->id));
	bson_append_oid(b, "Item", j_item_get_id(lock->item));

	bson_append_start_array(b, "Blocks");

	for (guint i = 0; i < lock->blocks->len; i++)
	{
		bson_numstr(numstr, i);
		bson_append_long(b, numstr, g_array_index(lock->blocks, guint64, i));
	}

	bson_append_finish_array(b);

	bson_finish(b);

	j_trace_leave(G_STRFUNC);
}

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
	bson_oid_gen(&(lock->id));
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
	JCollection* collection;
	bson obj[1];
	bson index[1];
	mongo* mongo_connection;
	mongo_write_concern write_concern[1];

	g_return_val_if_fail(lock != NULL, FALSE);

	j_lock_serialize(lock, obj);

	collection = j_item_get_collection(lock->item);

	mongo_write_concern_init(write_concern);
	//write_concern->j = 1;
	write_concern->w = 1;
	mongo_write_concern_finish(write_concern);

	bson_init(index);
	bson_append_int(index, "Item", 1);
	// FIXME speed
	bson_append_int(index, "Blocks", 1);
	bson_finish(index);

	mongo_connection = j_connection_pool_pop_meta(0);

	mongo_create_index(mongo_connection, j_collection_collection_locks(collection), index, NULL, MONGO_INDEX_UNIQUE, -1, NULL);
	lock->acquired = (mongo_insert(mongo_connection, j_collection_collection_locks(collection), obj, write_concern) == MONGO_OK);

	j_connection_pool_push_meta(0, mongo_connection);

	bson_destroy(index);
	bson_destroy(obj);

	mongo_write_concern_destroy(write_concern);

	return lock->acquired;
}

gboolean
j_lock_release (JLock* lock)
{
	JCollection* collection;
	bson obj[1];
	mongo* mongo_connection;
	mongo_write_concern write_concern[1];

	g_return_val_if_fail(lock != NULL, FALSE);
	g_return_val_if_fail(lock->acquired, FALSE);

	collection = j_item_get_collection(lock->item);

	mongo_write_concern_init(write_concern);
	//write_concern->j = 1;
	write_concern->w = 1;
	mongo_write_concern_finish(write_concern);

	bson_init(obj);
	bson_append_oid(obj, "_id", &(lock->id));
	bson_finish(obj);

	mongo_connection = j_connection_pool_pop_meta(0);

	lock->acquired = !(mongo_remove(mongo_connection, j_collection_collection_locks(collection), obj, write_concern) == MONGO_OK);

	j_connection_pool_push_meta(0, mongo_connection);

	bson_destroy(obj);
	mongo_write_concern_destroy(write_concern);

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
