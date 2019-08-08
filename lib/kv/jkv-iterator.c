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
	 * The current key.
	 **/
	gchar const* key;

	/**
	 * The current value.
	 **/
	gconstpointer value;
	guint32 len;

	JMessage** replies;
	guint32 replies_n;
	guint32 replies_cur;
};

static
JMessage*
fetch_reply (guint32 index, gchar const* namespace, gchar const* prefix)
{
	g_autoptr(JMessage) message = NULL;
	JMessage* reply;
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

	reply = j_message_new_reply(message);
	j_message_receive(reply, kv_connection);

	j_connection_pool_push_kv(index, kv_connection);

	return reply;
}

/**
 * Creates a new JKVIterator.
 *
 * \param store A JStore.
 *
 * \return A new JKVIterator.
 **/
JKVIterator*
j_kv_iterator_new (gchar const* namespace, gchar const* prefix)
{
	JKVIterator* iterator;

	JConfiguration* configuration = j_configuration();

	g_return_val_if_fail(namespace != NULL, NULL);

	/* FIXME still necessary? */
	//j_operation_cache_flush();

	iterator = g_slice_new(JKVIterator);
	iterator->kv_backend = j_kv_backend();
	iterator->cursor = NULL;
	iterator->key = NULL;
	iterator->value = NULL;
	iterator->len = 0;
	iterator->replies_n = j_configuration_get_kv_server_count(configuration);
	iterator->replies = g_new0(JMessage*, iterator->replies_n);
	iterator->replies_cur = 0;

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
		for (guint32 i = 0; i < iterator->replies_n; i++)
		{
			iterator->replies[i] = fetch_reply(i, namespace, prefix);
		}
	}

	return iterator;
}

JKVIterator*
j_kv_iterator_new_for_index (guint32 index, gchar const* namespace, gchar const* prefix)
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
	iterator->key = NULL;
	iterator->value = NULL;
	iterator->len = 0;
	iterator->replies_n = 1;
	iterator->replies = g_new0(JMessage*, 1);
	iterator->replies_cur = 0;

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
		iterator->replies[0] = fetch_reply(index, namespace, prefix);
	}

	return iterator;
}

/**
 * Frees the memory allocated by the JKVIterator.
 *
 * \param iterator A JKVIterator.
 **/
void
j_kv_iterator_free (JKVIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	for (guint32 i = 0; i < iterator->replies_n; i++)
	{
		if (iterator->replies[i] != NULL)
		{
			j_message_unref(iterator->replies[i]);
		}
	}

	g_free(iterator->replies);

	g_slice_free(JKVIterator, iterator);
}

/**
 * Checks whether another collection is available.
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
		ret = j_backend_kv_iterate(iterator->kv_backend, iterator->cursor, &(iterator->key), &(iterator->value), &(iterator->len));
	}
	else
	{
retry:
		iterator->len = j_message_get_4(iterator->replies[iterator->replies_cur]);

		if (iterator->len > 0)
		{
			iterator->value = j_message_get_n(iterator->replies[iterator->replies_cur], iterator->len);
			iterator->key = j_message_get_string(iterator->replies[iterator->replies_cur]);

			ret = TRUE;
		}
		else if (iterator->replies_cur < iterator->replies_n - 1)
		{
			iterator->replies_cur++;
			goto retry;
		}
	}

	return ret;
}

/**
 * Returns the current collection.
 *
 * \code
 * \endcode
 *
 * \param iterator A store iterator.
 *
 * \return A new collection. Should be freed with j_kv_unref().
 **/
gchar const*
j_kv_iterator_get (JKVIterator* iterator, gconstpointer* value, guint32* len)
{
	g_return_val_if_fail(iterator != NULL, NULL);
	g_return_val_if_fail(len != NULL, NULL);

	*value = iterator->value;
	*len = iterator->len;

	return iterator->key;
}

/**
 * @}
 **/
