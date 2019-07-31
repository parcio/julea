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

#include <item/jcollection.h>
#include <item/jcollection-internal.h>

#include <item/jitem.h>
#include <item/jitem-internal.h>

#include <jtrace-internal.h>

#include <julea.h>
#include <julea-kv.h>

/**
 * \defgroup JCollection Collection
 *
 * Data structures and functions for managing collections.
 *
 * @{
 **/

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

/**
 * Increases a collection's reference count.
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
	bson_t* tmp;
	gpointer value;
	guint32 len;

	g_return_val_if_fail(name != NULL, NULL);

	if ((collection = j_collection_new(name)) == NULL)
	{
		goto end;
	}

	tmp = j_collection_serialize(collection);
	value = bson_destroy_with_steal(tmp, TRUE, &len);

	j_kv_put(collection->kv, value, len, bson_free, batch);

end:
	return collection;
}

static
void
j_collection_get_callback (gpointer value, guint32 len, gpointer data)
{
	JCollection** collection = data;
	bson_t tmp[1];

	bson_init_static(tmp, value, len);
	*collection = j_collection_new_from_bson(tmp);

	g_free(value);
}

/**
 * Gets a collection.
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
	g_autoptr(JKV) kv = NULL;

	g_return_if_fail(collection != NULL);
	g_return_if_fail(name != NULL);

	kv = j_kv_new("collections", name);
	j_kv_get_callback(kv, j_collection_get_callback, collection, batch);
}

/**
 * Deletes a collection.
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
	collection->kv = j_kv_new("collections", collection->name);
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

	collection->kv = j_kv_new("collections", collection->name);

	j_trace_leave(G_STRFUNC);

	return collection;
}

/**
 * Serializes a collection.
 *
 * \private
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

	b = bson_new();
	b_cred = j_credentials_serialize(collection->credentials);

	bson_append_oid(b, "_id", -1, &(collection->id));
	bson_append_utf8(b, "name", -1, collection->name, -1);
	bson_append_document(b, "credentials", -1, b_cred);
	//bson_finish(b);

	bson_destroy(b_cred);

	j_trace_leave(G_STRFUNC);

	return b;
}

/**
 * Deserializes a collection.
 *
 * \private
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
 * @}
 **/
