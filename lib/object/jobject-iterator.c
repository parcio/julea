/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2022 Michael Kuhn
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

#include <object/jobject-iterator.h>

#include <object/jobject-internal.h>

#include <julea.h>

/**
 * \addtogroup JObjectIterator
 *
 * @{
 **/

struct JObjectIterator
{
	JBackend* object_backend;

	/**
	 * The iterate cursor.
	 **/
	gpointer cursor;

	/**
	 * The current name.
	 **/
	gchar const* name;

	JMessage** replies;
	guint32 replies_n;
	guint32 replies_cur;
};

static JMessage*
fetch_reply(guint32 index, gchar const* namespace, gchar const* prefix)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JMessage) message = NULL;
	JMessage* reply;
	JMessageType message_type;
	gpointer object_connection;
	gsize namespace_len;
	gsize prefix_len;

	namespace_len = strlen(namespace) + 1;

	if (prefix == NULL)
	{
		message_type = J_MESSAGE_OBJECT_GET_ALL;
		prefix_len = 0;
	}
	else
	{
		message_type = J_MESSAGE_OBJECT_GET_BY_PREFIX;
		prefix_len = strlen(prefix) + 1;
	}

	message = j_message_new(message_type, namespace_len + prefix_len);
	j_message_append_n(message, namespace, namespace_len);

	if (prefix != NULL)
	{
		j_message_append_n(message, prefix, prefix_len);
	}

	object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, index);
	j_message_send(message, object_connection);

	reply = j_message_new_reply(message);
	j_message_receive(reply, object_connection);

	j_connection_pool_push(J_BACKEND_TYPE_OBJECT, index, object_connection);

	return reply;
}

JObjectIterator*
j_object_iterator_new(gchar const* namespace, gchar const* prefix)
{
	J_TRACE_FUNCTION(NULL);

	JObjectIterator* iterator;

	JConfiguration* configuration = j_configuration();

	g_return_val_if_fail(namespace != NULL, NULL);

	//// \todo still necessary?
	//j_operation_cache_flush();

	iterator = g_slice_new(JObjectIterator);
	iterator->object_backend = j_object_get_backend();
	iterator->cursor = NULL;
	iterator->name = NULL;
	iterator->replies_n = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT);
	iterator->replies = g_new0(JMessage*, iterator->replies_n);
	iterator->replies_cur = 0;

	if (iterator->object_backend == NULL)
	{
		for (guint32 i = 0; i < iterator->replies_n; i++)
		{
			iterator->replies[i] = fetch_reply(i, namespace, prefix);
		}
	}
	else
	{
		if (prefix == NULL)
		{
			/// \todo j_backend_object_get_all(iterator->object_backend, namespace, &(iterator->cursor));
		}
		else
		{
			/// \todo j_backend_object_get_by_prefix(iterator->object_backend, namespace, prefix, &(iterator->cursor));
		}
	}

	return iterator;
}

JObjectIterator*
j_object_iterator_new_for_index(guint32 index, gchar const* namespace, gchar const* prefix)
{
	J_TRACE_FUNCTION(NULL);

	JObjectIterator* iterator;

	JConfiguration* configuration = j_configuration();

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT), NULL);

	/// \todo still necessary?
	//j_operation_cache_flush();

	iterator = g_slice_new(JObjectIterator);
	iterator->object_backend = j_object_get_backend();
	iterator->cursor = NULL;
	iterator->name = NULL;
	iterator->replies_n = 1;
	iterator->replies = g_new0(JMessage*, 1);
	iterator->replies_cur = 0;

	if (iterator->object_backend == NULL)
	{
		iterator->replies[0] = fetch_reply(index, namespace, prefix);
	}
	else
	{
		if (prefix == NULL)
		{
			/// \todo j_backend_object_get_all(iterator->object_backend, namespace, &(iterator->cursor));
		}
		else
		{
			/// \todo j_backend_object_get_by_prefix(iterator->object_backend, namespace, prefix, &(iterator->cursor));
		}
	}

	return iterator;
}

void
j_object_iterator_free(JObjectIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(iterator != NULL);

	for (guint32 i = 0; i < iterator->replies_n; i++)
	{
		if (iterator->replies[i] != NULL)
		{
			j_message_unref(iterator->replies[i]);
		}
	}

	g_free(iterator->replies);

	g_slice_free(JObjectIterator, iterator);
}

gboolean
j_object_iterator_next(JObjectIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	g_return_val_if_fail(iterator != NULL, FALSE);

	if (iterator->object_backend == NULL)
	{
	retry:
		iterator->name = j_message_get_string(iterator->replies[iterator->replies_cur]);

		if (iterator->name[0] != '\0')
		{
			ret = TRUE;
		}
		else if (iterator->replies_cur < iterator->replies_n - 1)
		{
			iterator->replies_cur++;
			goto retry;
		}
	}
	else
	{
		/// \todo ret = j_backend_object_iterate(iterator->object_backend, iterator->cursor, &(iterator->key), &(iterator->value), &(iterator->len));
	}

	return ret;
}

gchar const*
j_object_iterator_get(JObjectIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, NULL);

	return iterator->name;
}

/**
 * @}
 **/
