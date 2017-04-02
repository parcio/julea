/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include <item/jcollection.h>
#include <item/jcollection-internal.h>

#include <item/jitem.h>
#include <item/jitem-internal.h>

#include <kv/jkv.h>

#include <jbackground-operation-internal.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <jcommon-internal.h>
#include <jconnection-pool.h>
#include <jcredentials-internal.h>
#include <jhelper-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jmessage-internal.h>
#include <joperation-internal.h>
#include <jsemantics.h>
#include <jtrace-internal.h>

/**
 * \defgroup JCollection Collection
 *
 * Data structures and functions for managing collections.
 *
 * @{
 **/

struct JCollectionOperation
{
	union
	{
		struct
		{
			JCollection* collection;
		}
		create;

		struct
		{
			JCollection* collection;
		}
		delete;

		struct
		{
			JCollection** collection;
			gchar* name;
		}
		get;
	};
};

typedef struct JCollectionOperation JCollectionOperation;

/**
 * A collection of items.
 **/
struct JCollection
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

	JKV* kv;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

static
void
j_collection_get_free (gpointer data)
{
	JCollectionOperation* operation = data;

	g_free(operation->get.name);
	g_slice_free(JCollectionOperation, operation);
}

/**
 * Increases a collection's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 *
 * j_collection_ref(c);
 * \endcode
 *
 * \param collection A collection.
 *
 * \return #collection.
 **/
JCollection*
j_collection_ref (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(collection->ref_count));

	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Decreases a collection's reference count.
 * When the reference count reaches zero, frees the memory allocated for the collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 **/
void
j_collection_unref (JCollection* collection)
{
	g_return_if_fail(collection != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(collection->ref_count)))
	{
		j_kv_unref(collection->kv);
		j_credentials_unref(collection->credentials);

		g_free(collection->name);

		g_slice_free(JCollection, collection);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Returns a collection's name.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return A collection name.
 **/
gchar const*
j_collection_get_name (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);
	j_trace_leave(G_STRFUNC);

	return collection->name;
}

/**
 * Creates a collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param batch      A batch.
 **/
JCollection*
j_collection_create (gchar const* name, JBatch* batch)
{
	JCollection* collection;
	bson_t* value;

	g_return_val_if_fail(name != NULL, NULL);

	if ((collection = j_collection_new(name)) == NULL)
	{
		goto end;
	}

	value = j_collection_serialize(collection);

	j_kv_put(collection->kv, value, batch);

end:
	return collection;
}

/**
 * Gets a collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A pointer to a collection.
 * \param name       A name.
 * \param batch      A batch.
 **/
void
j_collection_get (JCollection** collection, gchar const* name, JBatch* batch)
{
	JCollectionOperation* cop;
	JOperation* operation;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(name != NULL);

	cop = g_slice_new(JCollectionOperation);
	cop->get.collection = collection;
	cop->get.name = g_strdup(name);

	operation = j_operation_new();
	operation->key = NULL;
	operation->data = cop;
	operation->exec_func = j_collection_get_exec;
	operation->free_func = j_collection_get_free;

	j_batch_add(batch, operation);
}

/**
 * Deletes a collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param batch      A batch.
 **/
void
j_collection_delete (JCollection* collection, JBatch* batch)
{
	g_return_if_fail(collection != NULL);
	g_return_if_fail(batch != NULL);

	j_kv_delete(collection->kv, batch);
}

/* Internal */

/**
 * Creates a new collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * JCollection* c;
 *
 * c = j_collection_new("JULEA");
 * \endcode
 *
 * \param name  A collection name.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_collection_new (gchar const* name)
{
	JCollection* collection = NULL;

	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (strpbrk(name, "/") != NULL)
	{
		goto end;
	}

	/*
	: m_initialized(false),
	*/

	collection = g_slice_new(JCollection);
	bson_oid_init(&(collection->id), bson_context_get_default());
	collection->name = g_strdup(name);
	collection->credentials = j_credentials_new();
	collection->kv = j_kv_new(0, "collections", collection->name);
	collection->ref_count = 1;

end:
	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Creates a new collection from a BSON object.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param b     A BSON object.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_collection_new_from_bson (bson_t const* b)
{
	/*
		: m_initialized(true),
	*/
	JCollection* collection;

	g_return_val_if_fail(b != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	collection = g_slice_new(JCollection);
	collection->name = NULL;
	collection->credentials = j_credentials_new();
	collection->ref_count = 1;

	j_collection_deserialize(collection, b);

	collection->kv = j_kv_new(0, "collections", collection->name);

	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Serializes a collection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
bson_t*
j_collection_serialize (JCollection* collection)
{
	/*
			.append("User", m_owner.User())
			.append("Group", m_owner.Group())
	*/

	bson_t* b;
	bson_t* b_cred;

	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	b_cred = j_credentials_serialize(collection->credentials);

	b = g_slice_new(bson_t);
	bson_init(b);

	bson_append_oid(b, "_id", -1, &(collection->id));
	bson_append_utf8(b, "name", -1, collection->name, -1);
	bson_append_document(b, "credentials", -1, b_cred);
	//bson_finish(b);

	bson_destroy(b_cred);
	g_slice_free(bson_t, b_cred);

	j_trace_leave(G_STRFUNC);

	return b;
}

/**
 * Deserializes a collection.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 * \param b          A BSON object.
 **/
void
j_collection_deserialize (JCollection* collection, bson_t const* b)
{
	bson_iter_t iterator;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	//j_bson_print(bson_t);

	bson_iter_init(&iterator, b);

	/*
		m_owner.m_user = o.getField("User").Int();
		m_owner.m_group = o.getField("Group").Int();
	*/

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "_id") == 0)
		{
			collection->id = *bson_iter_oid(&iterator);
		}
		else if (g_strcmp0(key, "name") == 0)
		{
			g_free(collection->name);
			collection->name = g_strdup(bson_iter_utf8(&iterator, NULL /*FIXME*/));
		}
		else if (g_strcmp0(key, "credentials") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_cred[1];

			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_cred, data, len);
			j_credentials_deserialize(collection->credentials, b_cred);
			bson_destroy(b_cred);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Returns a collection's ID.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param collection A collection.
 *
 * \return An ID.
 **/
bson_oid_t const*
j_collection_get_id (JCollection* collection)
{
	g_return_val_if_fail(collection != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);
	j_trace_leave(G_STRFUNC);

	return &(collection->id);
}

/**
 * Gets collections.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \param batch      A batch.
 * \param operations A list of operations.
 *
 * \return TRUE.
 */
gboolean
j_collection_get_exec (JList* operations, JSemantics* semantics)
{
	JBackend* meta_backend;
	g_autoptr(JListIterator) it = NULL;
	GSocketConnection* meta_connection;
	gboolean ret = TRUE;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	it = j_list_iterator_new(operations);
	//mongo_connection = j_connection_pool_pop_meta(0);

	meta_backend = j_metadata_backend();

	if (meta_backend == NULL)
	{
		meta_connection = j_connection_pool_pop_meta(0);
	}

	/* FIXME do some optimizations for len(operations) > 1 */
	while (j_list_iterator_next(it))
	{
		JCollectionOperation* operation = j_list_iterator_get(it);
		JCollection** collection = operation->get.collection;
		g_autoptr(JMessage) message = NULL;
		g_autoptr(JMessage) reply = NULL;
		bson_t result[1];
		gchar const* name = operation->get.name;

		if (meta_backend != NULL)
		{
			ret = j_backend_meta_get(meta_backend, "collections", name, result) && ret;
		}
		else
		{
			gconstpointer data;
			gsize name_len;
			guint32 len;

			name_len = strlen(name) + 1;

			message = j_message_new(J_MESSAGE_META_GET, 12);
			j_message_append_n(message, "collections", 12);
			j_message_add_operation(message, name_len);
			j_message_append_n(message, name, name_len);

			j_message_send(message, meta_connection);

			reply = j_message_new_reply(message);
			j_message_receive(reply, meta_connection);

			len = j_message_get_4(reply);

			if (len > 0)
			{
				ret = TRUE;

				data = j_message_get_n(reply, len);

				// result points to reply's memory
				bson_init_static(result, data, len);
			}
			else
			{
				ret = FALSE;
			}
		}

		*collection = NULL;

		if (ret)
		{
			*collection = j_collection_new_from_bson(result);
			bson_destroy(result);
		}
	}

	if (meta_backend == NULL)
	{
		j_connection_pool_push_meta(0, meta_connection);
	}

	//j_connection_pool_push_meta(0, mongo_connection);

	return ret;
}

/**
 * @}
 **/
