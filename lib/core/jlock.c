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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <bson.h>

#include <jlock.h>

#include <jbackend.h>
#include <jcommon.h>
#include <jconnection-pool-internal.h>
#include <jhelper-internal.h>
#include <jtrace.h>

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
	gchar* namespace;
	gchar* path;

	GArray* blocks;

	gboolean acquired;
};

/**
 * Creates a new lock.
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
j_lock_new (gchar const* namespace, gchar const* path)
{
	J_TRACE_FUNCTION(NULL);

	JLock* lock;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(path != NULL, NULL);

	lock = g_slice_new(JLock);
	lock->namespace = g_strdup(namespace);
	lock->path = g_strdup(path);
	lock->blocks = g_array_new(FALSE, FALSE, sizeof(guint64));
	lock->acquired = FALSE;

	return lock;
}

/**
 * Frees the memory allocated for the lock.
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
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(lock != NULL);

	if (lock->acquired)
	{
		j_lock_release(lock);
	}

	g_free(lock->path);
	g_free(lock->namespace);

	g_array_free(lock->blocks, TRUE);

	g_slice_free(JLock, lock);
}

gboolean
j_lock_acquire (JLock* lock)
{
	JBackend* kv_backend;
	bson_t empty[1];
	gboolean acquired = TRUE;
	gpointer kv_batch;

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

	kv_backend = j_kv_backend();

	//mongo_connection = j_connection_pool_pop_kv(0);

	if (lock->blocks->len > 0)
	{
		if (kv_backend != NULL)
		{
			acquired = j_backend_kv_batch_start(kv_backend, "locks", J_SEMANTICS_SAFETY_NETWORK, &kv_batch);
		}

		for (guint i = 0; i < lock->blocks->len; i++)
		{
			g_autofree gchar* block_str = NULL;
			guint64 block;

			block = g_array_index(lock->blocks, guint64, i);
			block_str = g_strdup_printf("%" G_GUINT64_FORMAT, block);

			if (kv_backend != NULL)
			{
				g_autofree gchar* path = NULL;

				path = g_build_path("/", lock->namespace, lock->path, block_str, NULL);
				acquired = j_backend_kv_put(kv_backend, kv_batch, path, bson_get_data(empty), empty->len) && acquired;
			}
		}

		if (kv_backend != NULL)
		{
			acquired = j_backend_kv_batch_execute(kv_backend, kv_batch) && acquired;
		}
	}

	lock->acquired = acquired;

	//j_connection_pool_push_kv(0, mongo_connection);

	bson_destroy(empty);

	return lock->acquired;
}

gboolean
j_lock_release (JLock* lock)
{
	JBackend* kv_backend;
	gboolean released = TRUE;
	gpointer kv_batch;

	g_return_val_if_fail(lock != NULL, FALSE);
	g_return_val_if_fail(lock->acquired, FALSE);

	//write_concern = mongoc_write_concern_new();
	//write_concern->j = 1;
	//mongoc_write_concern_set_w(write_concern, 1);

	//bson_init(obj);
	//bson_append_oid(obj, "_id", -1, &(lock->id));
	//bson_finish(obj);

	kv_backend = j_kv_backend();
	//mongo_connection = j_connection_pool_pop_kv(0);

	if (lock->blocks->len > 0)
	{
		if (kv_backend != NULL)
		{
			released = j_backend_kv_batch_start(kv_backend, "locks", J_SEMANTICS_SAFETY_NETWORK, &kv_batch);
		}

		for (guint i = 0; i < lock->blocks->len; i++)
		{
			g_autofree gchar* block_str = NULL;
			guint64 block;

			block = g_array_index(lock->blocks, guint64, i);
			block_str = g_strdup_printf("%" G_GUINT64_FORMAT, block);

			if (kv_backend != NULL)
			{
				g_autofree gchar* path = NULL;

				path = g_build_path("/", lock->namespace, lock->path, block_str, NULL);
				released = j_backend_kv_delete(kv_backend, kv_batch, path) && released;
			}
		}

		if (kv_backend != NULL)
		{
			released = j_backend_kv_batch_execute(kv_backend, kv_batch) && released;
		}
	}

	lock->acquired = !released;

	//j_connection_pool_push_kv(0, mongo_connection);

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
