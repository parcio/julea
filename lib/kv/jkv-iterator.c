/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
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

#include <kv/jkv-iterator.h>

#include <kv/jkv.h>

#include <julea.h>

/**
 * \defgroup JKVIterator KV Iterator
 *
 * Data structures and functions for iterating over stores.
 *
 * @{
 **/

struct JKVIterator
{
	JBackend* kv_backend;

	/**
	 * The iterate cursor.
	 **/
	gpointer cursor;

	/**
	 * The current document.
	 **/
	bson_t current[1];

	JMessage* reply;
};

/**
 * Creates a new JKVIterator.
 *
 * \author Michael Kuhn
 *
 * \param store A JStore.
 *
 * \return A new JKVIterator.
 **/
JKVIterator*
j_kv_iterator_new (guint32 index, gchar const* namespace, gchar const* prefix)
{
	JKVIterator* iterator;

	JConfiguration* configuration = j_configuration();

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_kv_server_count(configuration), NULL);

	/* FIXME still necessary? */
	//j_operation_cache_flush();

	iterator = g_slice_new(JKVIterator);
	iterator->kv_backend = j_kv_backend();
	iterator->cursor = NULL;
	iterator->reply = NULL;

	if (iterator->kv_backend != NULL)
	{
		if (prefix == NULL)
		{
			j_backend_kv_get_all(iterator->kv_backend, namespace, &(iterator->cursor));
		}
		else
		{
			j_backend_kv_get_by_prefix(iterator->kv_backend, namespace, prefix, &(iterator->cursor));
		}
	}
	else
	{
		g_autoptr(JMessage) message = NULL;
		JMessageType message_type;
		GSocketConnection* kv_connection;
		gsize namespace_len;
		gsize prefix_len;

		namespace_len = strlen(namespace) + 1;

		if (prefix == NULL)
		{
			message_type = J_MESSAGE_KV_GET_ALL;
			prefix_len = 0;
		}
		else
		{
			message_type = J_MESSAGE_KV_GET_BY_PREFIX;
			prefix_len = strlen(prefix) + 1;
		}

		message = j_message_new(message_type, namespace_len + prefix_len);
		j_message_append_n(message, namespace, namespace_len);

		if (prefix != NULL)
		{
			j_message_append_n(message, prefix, prefix_len);
		}

		kv_connection = j_connection_pool_pop_kv(index);
		j_message_send(message, kv_connection);

		iterator->reply = j_message_new_reply(message);
		j_message_receive(iterator->reply, kv_connection);

		j_connection_pool_push_kv(index, kv_connection);
	}

	return iterator;
}

/**
 * Frees the memory allocated by the JKVIterator.
 *
 * \author Michael Kuhn
 *
 * \param iterator A JKVIterator.
 **/
void
j_kv_iterator_free (JKVIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	if (iterator->reply != NULL)
	{
		j_message_unref(iterator->reply);
	}

	g_slice_free(JKVIterator, iterator);
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
j_kv_iterator_next (JKVIterator* iterator)
{
	gboolean ret = FALSE;

	g_return_val_if_fail(iterator != NULL, FALSE);

	if (iterator->kv_backend != NULL)
	{
		ret = j_backend_kv_iterate(iterator->kv_backend, iterator->cursor, iterator->current);
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
 * \return A new collection. Should be freed with j_kv_unref().
 **/
bson_t const*
j_kv_iterator_get (JKVIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	// FIXME return key
	return iterator->current;
}

/**
 * @}
 **/
