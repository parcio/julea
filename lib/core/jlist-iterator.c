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

#include <jlist-iterator.h>

#include <jlist.h>
#include <jlist-internal.h>
#include <jtrace.h>

/**
 * \defgroup JListIterator List Iterator
 * @{
 **/

/**
 *
 **/
struct JListIterator
{
	/**
	 * The associated list.
	 **/
	JList* list;
	/**
	 * The current list element.
	 **/
	JListElement* current;
	/**
	 * Whether the current element is the first one.
	 **/
	gboolean first;
};

/**
 * Creates a new list iterator.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 *
 * \return A new list iterator.
 **/
JListIterator*
j_list_iterator_new (JList* list)
{
	J_TRACE_FUNCTION(NULL);

	JListIterator* iterator;

	g_return_val_if_fail(list != NULL, NULL);

	iterator = g_slice_new(JListIterator);
	iterator->list = j_list_ref(list);
	iterator->current = j_list_head(iterator->list);
	iterator->first = TRUE;

	return iterator;
}

/**
 * Frees the memory allocated by a list iterator.
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 **/
void
j_list_iterator_free (JListIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(iterator != NULL);

	j_list_unref(iterator->list);

	g_slice_free(JListIterator, iterator);
}

/**
 * Checks whether another list element is available.
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 *
 * \return TRUE on success, FALSE if the end of the list is reached.
 **/
gboolean
j_list_iterator_next (JListIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, FALSE);

	if (G_UNLIKELY(iterator->first))
	{
		iterator->first = FALSE;
	}
	else
	{
		iterator->current = iterator->current->next;
	}

	return (iterator->current != NULL);
}

/**
 * Returns the current list element.
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 *
 * \return A list element.
 **/
gpointer
j_list_iterator_get (JListIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, NULL);
	g_return_val_if_fail(iterator->current != NULL, NULL);

	return iterator->current->data;
}

/**
 * @}
 **/
