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

#include <object/jobject-iterator.h>

#include <julea.h>

/**
 * \defgroup JObjectIterator Object Iterator
 *
 * Data structures and functions for iterating over objects.
 *
 * @{
 **/

struct JObjectIterator
{
	gchar* namespace;
	JBackend* object_backend;
	guint32 servers;
	JMessage* message;
};

/**
 * Creates a new JObjectIterator.
 *
 * \param collection A JCollection.
 *
 * \return A new JObjectIterator.
 **/
JObjectIterator*
j_object_iterator_new (gchar const* namespace)
{
	J_TRACE_FUNCTION(NULL);

	JObjectIterator* iterator;

	JConfiguration* configuration;

	g_return_val_if_fail(namespace != NULL, NULL);

	configuration = j_configuration();

	iterator = g_slice_new(JObjectIterator);
	iterator->namespace = g_strdup(namespace);
	iterator->object_backend = j_backend(J_BACKEND_TYPE_OBJECT);
	iterator->servers = j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT);
	iterator->message = NULL;

	if (iterator->object_backend != NULL)
	{
	}
	else
	{
	}

	return iterator;
}

/**
 * Frees the memory allocated by the JObjectIterator.
 *
 * \param iterator A JObjectIterator.
 **/
void
j_object_iterator_free (JObjectIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(iterator != NULL);

	if (iterator->message != NULL)
	{
		j_message_unref(iterator->message);
	}

	g_free(iterator->namespace);

	g_slice_free(JObjectIterator, iterator);
}

/**
 * Checks whether another item is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A collection iterator.
 *
 * \return TRUE on success, FALSE if the end of the collection is reached.
 **/
gboolean
j_object_iterator_next (JObjectIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	g_return_val_if_fail(iterator != NULL, FALSE);

	if (iterator->object_backend != NULL)
	{
	}
	else
	{
	}

	return ret;
}

/**
 * Returns the current item.
 *
 * \code
 * \endcode
 *
 * \param iterator A collection iterator.
 *
 * \return A new item. Should be freed with j_object_unref().
 **/
gchar const*
j_object_iterator_get (JObjectIterator* iterator, guint64* index)
{
	J_TRACE_FUNCTION(NULL);

	(void)index;

	g_return_val_if_fail(iterator != NULL, NULL);

	return NULL;
}

/**
 * @}
 **/
