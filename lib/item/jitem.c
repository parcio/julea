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

#include <string.h>

#include <bson.h>

#include <item/jitem.h>
#include <item/jitem-internal.h>

#include <item/jcollection.h>
#include <item/jcollection-internal.h>

#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>

/**
 * \defgroup JItem Item
 *
 * Data structures and functions for managing items.
 *
 * @{
 **/

struct JItemGetData
{
	JCollection* collection;
	JItem** item;
};

typedef struct JItemGetData JItemGetData;

/**
 * A JItem.
 **/
struct JItem
{
	/**
	 * The ID.
	 **/
	bson_oid_t id;

	/**
	 * The name.
	 **/
	gchar* name;

	JCredentials* credentials;
	JDistribution* distribution;

	JKV* kv;
	JDistributedObject* object;

	/**
	 * The status.
	 **/
	struct
	{
		guint64 age;

		/**
		 * The size.
		 * Stored in bytes.
		 */
		guint64 size;

		/**
		 * The time of the last modification.
		 * Stored in microseconds since the Epoch.
		 */
		gint64 modification_time;
	}
	status;

	/**
	 * The parent collection.
	 **/
	JCollection* collection;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Increases an item's reference count.
 *
 * \code
 * JItem* i;
 *
 * j_item_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JItem*
j_item_ref (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, NULL);

	g_atomic_int_inc(&(item->ref_count));

	return item;
}

/**
 * Decreases an item's reference count.
 * When the reference count reaches zero, frees the memory allocated for the item.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 **/
void
j_item_unref (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
		if (item->kv != NULL)
		{
			j_kv_unref(item->kv);
		}

		if (item->object != NULL)
		{
			j_distributed_object_unref(item->object);
		}

		if (item->collection != NULL)
		{
			j_collection_unref(item->collection);
		}

		j_credentials_unref(item->credentials);
		j_distribution_unref(item->distribution);

		g_free(item->name);

		g_slice_free(JItem, item);
	}
}

/**
 * Returns an item's name.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return The name.
 **/
gchar const*
j_item_get_name (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, NULL);

	return item->name;
}

/**
 * Creates an item in a collection.
 *
 * \code
 * \endcode
 *
 * \param collection   A collection.
 * \param name         A name.
 * \param distribution A distribution.
 * \param batch        A batch.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_create (JCollection* collection, gchar const* name, JDistribution* distribution, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JItem* item;
	bson_t* tmp;
	gpointer value;
	guint32 len;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	if ((item = j_item_new(collection, name, distribution)) == NULL)
	{
		return NULL;
	}

	tmp = j_item_serialize(item, j_batch_get_semantics(batch));
	value = bson_destroy_with_steal(tmp, TRUE, &len);

	j_distributed_object_create(item->object, batch);
	j_kv_put(item->kv, value, len, bson_free, batch);

	return item;
}

static
void
j_item_get_callback (gpointer value, guint32 len, gpointer data_)
{
	JItemGetData* data = data_;
	bson_t tmp[1];

	bson_init_static(tmp, value, len);
	*(data->item) = j_item_new_from_bson(data->collection, tmp);

	j_collection_unref(data->collection);
	g_slice_free(JItemGetData, data);

	g_free(value);
}

/**
 * Gets an item from a collection.
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param item       A pointer to an item.
 * \param name       A name.
 * \param batch      A batch.
 **/
void
j_item_get (JCollection* collection, JItem** item, gchar const* name, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JItemGetData* data;
	g_autoptr(JKV) kv = NULL;
	g_autofree gchar* path = NULL;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(item != NULL);
	g_return_if_fail(name != NULL);

	data = g_slice_new(JItemGetData);
	data->collection = j_collection_ref(collection);
	data->item = item;

	path = g_build_path("/", j_collection_get_name(collection), name, NULL);
	kv = j_kv_new("items", path);
	j_kv_get_callback(kv, j_item_get_callback, data, batch);
}

/**
 * Deletes an item from a collection.
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param item       An item.
 * \param batch      A batch.
 **/
void
j_item_delete (JItem* item, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);
	g_return_if_fail(batch != NULL);

	j_kv_delete(item->kv, batch);
	j_distributed_object_delete(item->object, batch);
}

/**
 * Reads an item.
 *
 * \code
 * \endcode
 *
 * \param item       An item.
 * \param data       A buffer to hold the read data.
 * \param length     Number of bytes to read.
 * \param offset     An offset within #item.
 * \param bytes_read Number of bytes read.
 * \param batch      A batch.
 **/
void
j_item_read (JItem* item, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_read != NULL);

	j_distributed_object_read(item->object, data, length, offset, bytes_read, batch);
}

/**
 * Writes an item.
 *
 * \note
 * j_item_write() modifies bytes_written even if j_batch_execute() is not called.
 *
 * \code
 * \endcode
 *
 * \param item          An item.
 * \param data          A buffer holding the data to write.
 * \param length        Number of bytes to write.
 * \param offset        An offset within #item.
 * \param bytes_written Number of bytes written.
 * \param batch         A batch.
 **/
void
j_item_write (JItem* item, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(bytes_written != NULL);

	// FIXME see j_item_write_exec
	j_distributed_object_write(item->object, data, length, offset, bytes_written, batch);
}

/**
 * Get the status of an item.
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param batch     A batch.
 **/
void
j_item_get_status (JItem* item, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);

	// FIXME check j_item_get_status_exec
	j_distributed_object_status(item->object, &(item->status.modification_time), &(item->status.size), batch);
}

/**
 * Returns an item's size.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A size.
 **/
guint64
j_item_get_size (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, 0);

	return item->status.size;
}

/**
 * Returns an item's modification time.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A modification time.
 **/
gint64
j_item_get_modification_time (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, 0);

	return item->status.modification_time;
}

/* Internal */

/**
 * Creates a new item.
 *
 * \code
 * JItem* i;
 *
 * i = j_item_new("JULEA");
 * \endcode
 *
 * \param collection   A collection.
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_new (JCollection* collection, gchar const* name, JDistribution* distribution)
{
	J_TRACE_FUNCTION(NULL);

	JItem* item = NULL;
	g_autofree gchar* path = NULL;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	if (strpbrk(name, "/") != NULL)
	{
		return NULL;
	}

	if (distribution == NULL)
	{
		distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
	}

	item = g_slice_new(JItem);
	bson_oid_init(&(item->id), bson_context_get_default());
	item->name = g_strdup(name);
	item->credentials = j_credentials_new();
	item->distribution = distribution;
	item->status.age = g_get_real_time();
	item->status.size = 0;
	item->status.modification_time = g_get_real_time();
	item->collection = j_collection_ref(collection);
	item->ref_count = 1;

	path = g_build_path("/", j_collection_get_name(item->collection), item->name, NULL);
	item->kv = j_kv_new("items", path);
	item->object = j_distributed_object_new("item", path, item->distribution);

	return item;
}

/**
 * Creates a new item from a BSON object.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param b          A BSON object.
 *
 * \return A new item. Should be freed with j_item_unref().
 **/
JItem*
j_item_new_from_bson (JCollection* collection, bson_t const* b)
{
	J_TRACE_FUNCTION(NULL);

	JItem* item;
	g_autofree gchar* path = NULL;

	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(b != NULL, NULL);

	item = g_slice_new(JItem);
	item->name = NULL;
	item->credentials = j_credentials_new();
	item->distribution = NULL;
	item->status.age = 0;
	item->status.size = 0;
	item->status.modification_time = 0;
	item->collection = j_collection_ref(collection);
	item->ref_count = 1;

	j_item_deserialize(item, b);

	path = g_build_path("/", j_collection_get_name(item->collection), item->name, NULL);
	item->kv = j_kv_new("items", path);
	item->object = j_distributed_object_new("item", path, item->distribution);

	return item;
}

/**
 * Returns an item's collection.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A collection.
 **/
JCollection*
j_item_get_collection (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, NULL);

	return item->collection;
}

/**
 * Returns an item's credentials.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return A collection.
 **/
JCredentials*
j_item_get_credentials (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, NULL);

	return item->credentials;
}

/**
 * Serializes an item.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param semantics A semantics object.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
bson_t*
j_item_serialize (JItem* item, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	bson_t* b;
	bson_t* b_cred;
	bson_t* b_distribution;

	g_return_val_if_fail(item != NULL, NULL);

	b = bson_new();
	b_cred = j_credentials_serialize(item->credentials);
	b_distribution = j_distribution_serialize(item->distribution);

	bson_append_oid(b, "_id", -1, &(item->id));
	bson_append_oid(b, "collection", -1, j_collection_get_id(item->collection));
	bson_append_utf8(b, "name", -1, item->name, -1);

	if (j_semantics_get(semantics, J_SEMANTICS_CONCURRENCY) == J_SEMANTICS_CONCURRENCY_NONE)
	{
		bson_t b_document[1];

		bson_append_document_begin(b, "status", -1, b_document);

		bson_append_int64(b_document, "size", -1, item->status.size);
		bson_append_int64(b_document, "modification_time", -1, item->status.modification_time);

		bson_append_document_end(b, b_document);

		bson_destroy(b_document);
	}

	bson_append_document(b, "credentials", -1, b_cred);
	bson_append_document(b, "distribution", -1, b_distribution);

	//bson_finish(b);

	bson_destroy(b_cred);
	bson_destroy(b_distribution);

	return b;
}

static
void
j_item_deserialize_status (JItem* item, bson_t const* b)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(b != NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "size") == 0)
		{
			item->status.size = bson_iter_int64(&iterator);
			item->status.age = g_get_real_time();
		}
		else if (g_strcmp0(key, "modification_time") == 0)
		{
			item->status.modification_time = bson_iter_int64(&iterator);
			item->status.age = g_get_real_time();
		}
	}
}

/**
 * Deserializes an item.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param b    A BSON object.
 **/
void
j_item_deserialize (JItem* item, bson_t const* b)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;

	g_return_if_fail(item != NULL);
	g_return_if_fail(b != NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "_id") == 0)
		{
			item->id = *bson_iter_oid(&iterator);
		}
		else if (g_strcmp0(key, "name") == 0)
		{
			g_free(item->name);
			item->name = g_strdup(bson_iter_utf8(&iterator, NULL /*FIXME*/));
		}
		else if (g_strcmp0(key, "status") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_status[1];

			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_status, data, len);
			j_item_deserialize_status(item, b_status);
			bson_destroy(b_status);
		}
		else if (g_strcmp0(key, "credentials") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_cred[1];

			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_cred, data, len);
			j_credentials_deserialize(item->credentials, b_cred);
			bson_destroy(b_cred);
		}
		else if (g_strcmp0(key, "distribution") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_distribution[1];

			if (item->distribution != NULL)
			{
				j_distribution_unref(item->distribution);
			}

			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_distribution, data, len);
			item->distribution = j_distribution_new_from_bson(b_distribution);
			bson_destroy(b_distribution);
		}
	}
}

/**
 * Returns an item's ID.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param item An item.
 *
 * \return An ID.
 **/
bson_oid_t const*
j_item_get_id (JItem* item)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(item != NULL, NULL);

	return &(item->id);
}

/**
 * Sets an item's modification time.
 *
 * \code
 * \endcode
 *
 * \param item              An item.
 * \param modification_time A modification time.
 **/
void
j_item_set_modification_time (JItem* item, gint64 modification_time)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);

	item->status.age = g_get_real_time();
	item->status.modification_time = MAX(item->status.modification_time, modification_time);
}

/**
 * Sets an item's size.
 *
 * \code
 * \endcode
 *
 * \param item An item.
 * \param size A size.
 **/
void
j_item_set_size (JItem* item, guint64 size)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(item != NULL);

	item->status.age = g_get_real_time();
	item->status.size = size;
}

/*
gboolean
j_item_write_exec (JList* operations, JSemantics* semantics)
{
	if (j_semantics_get(semantics, J_SEMANTICS_CONCURRENCY) == J_SEMANTICS_CONCURRENCY_NONE && FALSE)
	{
		bson_t b_document[1];
		bson_t cond[1];
		bson_t op[1];
		mongoc_client_t* mongo_connection;
		mongoc_collection_t* mongo_collection;
		mongoc_write_concern_t* write_concern;
		gint ret;

		j_helper_set_write_concern(write_concern, semantics);

		bson_init(cond);
		bson_append_oid(cond, "_id", -1, &(item->id));
		bson_append_int32(cond, "Status.ModificationTime", -1, item->status.modification_time);
		//bson_finish(cond);

		j_item_set_modification_time(item, g_get_real_time());

		bson_init(op);

		bson_append_document_begin(op, "$set", -1, b_document);

		if (max_offset > item->status.size)
		{
			j_item_set_size(item, max_offset);
			bson_append_int64(b_document, "Status.Size", -1, item->status.size);
		}

		bson_append_int64(b_document, "Status.ModificationTime", -1, item->status.modification_time);
		bson_append_document_end(op, b_document);

		//bson_finish(op);

		mongo_connection = j_connection_pool_pop_kv(0);
		mongo_collection = mongoc_client_get_collection(mongo_connection, "JULEA", "Items");

		ret = mongoc_collection_update(mongo_collection, MONGOC_UPDATE_NONE, cond, op, write_concern, NULL);

		j_connection_pool_push_kv(0, mongo_connection);

		if (!ret)
		{

		}

		bson_destroy(cond);
		bson_destroy(op);

		mongoc_write_concern_destroy(write_concern);
	}
}
*/

/*
gboolean
j_item_get_status_exec (JList* operations, JSemantics* semantics)
{
	if (semantics_consistency != J_SEMANTICS_CONSISTENCY_IMMEDIATE)
	{
		if (item->status.age >= (guint64)g_get_real_time() - G_USEC_PER_SEC)
		{
			continue;
		}
	}

	if (semantics_concurrency == J_SEMANTICS_CONCURRENCY_NONE)
	{
		bson_t result[1];
		gchar* path;

		bson_init(&opts);
		bson_append_int32(&opts, "limit", -1, 1);
		bson_append_document_begin(&opts, "projection", -1, &projection);

		bson_append_bool(&projection, "Status.Size", -1, TRUE);
		bson_append_bool(&projection, "Status.ModificationTime", -1, TRUE);

		bson_append_document_end(&opts, &projection);

		if (kv_backend != NULL)
		{
			path = g_build_path("/", j_collection_get_name(item->collection), item->name, NULL);
			ret = j_backend_kv_get(kv_backend, "items", path, result) && ret;
			g_free(path);
		}

		bson_init(&b);
		bson_append_oid(&b, "_id", -1, &(item->id));

		if (ret)
		{
			j_item_deserialize(item, result);
			bson_destroy(result);
		}
	}
}
*/

/**
 * @}
 **/
