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

#include <item/jcollection-iterator.h>

#include <item/jcollection.h>
#include <item/jcollection-internal.h>

#include <jbackend.h>
#include <jbackend-internal.h>
#include <jcommon-internal.h>
#include <jconnection-pool-internal.h>
#include <jmessage-internal.h>
#include <joperation-cache-internal.h>

/**
 * \defgroup JCollectionIterator Store Iterator
 *
 * Data structures and functions for iterating over stores.
 *
 * @{
 **/

struct JCollectionIterator
{
	JBackend* meta_backend;

	/**
	 * The MongoDB cursor.
	 **/
	gpointer cursor;

	/**
	 * The current document.
	 **/
	bson_t current[1];

	JMessage* reply;
};

/**
 * Creates a new JCollectionIterator.
 *
 * \author Michael Kuhn
 *
 * \param store A JStore.
 *
 * \return A new JCollectionIterator.
 **/
JCollectionIterator*
j_collection_iterator_new (void)
{
	JCollectionIterator* iterator;

	/* FIXME still necessary? */
	j_operation_cache_flush();

	iterator = g_slice_new(JCollectionIterator);
	iterator->meta_backend = j_metadata_backend();
	iterator->cursor = NULL;
	iterator->reply = NULL;

	if (iterator->meta_backend != NULL)
	{
		j_backend_meta_get_all(iterator->meta_backend, "collections", &(iterator->cursor));
	}
	else
	{
		JMessage* message;
		GSocketConnection* meta_connection;

		meta_connection = j_connection_pool_pop_meta(0);
		message = j_message_new(J_MESSAGE_META_GET_ALL, 12);
		j_message_append_n(message, "collections", 12);

		j_message_send(message, meta_connection);

		iterator->reply = j_message_new_reply(message);
		j_message_receive(iterator->reply, meta_connection);

		j_message_unref(message);
		j_connection_pool_push_meta(0, meta_connection);
	}

	return iterator;
}

/**
 * Frees the memory allocated by the JCollectionIterator.
 *
 * \author Michael Kuhn
 *
 * \param iterator A JCollectionIterator.
 **/
void
j_collection_iterator_free (JCollectionIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	if (iterator->reply != NULL)
	{
		j_message_unref(iterator->reply);
	}

	g_slice_free(JCollectionIterator, iterator);
}

/**
 * Checks whether another collection is available.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return TRUE on success, FALSE if the end of the store is reached.
 **/
gboolean
j_collection_iterator_next (JCollectionIterator* iterator)
{
	gboolean ret = FALSE;

	g_return_val_if_fail(iterator != NULL, FALSE);

	if (iterator->meta_backend != NULL)
	{
		ret = j_backend_meta_iterate(iterator->meta_backend, iterator->cursor, iterator->current);
	}
	else
	{
		guint32 len;

		len = j_message_get_4(iterator->reply);

		if (len > 0)
		{
			gconstpointer data;

			data = j_message_get_n(iterator->reply, len);
			bson_init_static(iterator->current, data, len);

			ret = TRUE;
		}
	}

	return ret;
}

/**
 * Returns the current collection.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return A new collection. Should be freed with j_collection_unref().
 **/
JCollection*
j_collection_iterator_get (JCollectionIterator* iterator)
{
	JCollection* collection;

	g_return_val_if_fail(iterator != NULL, NULL);

	collection = j_collection_new_from_bson(iterator->current);

	return collection;
}

/**
 * @}
 **/
