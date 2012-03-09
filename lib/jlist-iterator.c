/*
 * Copyright (c) 2010-2012 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <glib.h>

#include <jlist-iterator.h>

#include <jlist.h>
#include <jlist-internal.h>

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
 * \author Michael Kuhn
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
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param iterator A list iterator.
 **/
void
j_list_iterator_free (JListIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	j_list_unref(iterator->list);

	g_slice_free(JListIterator, iterator);
}

/**
 * Checks whether another list element is available.
 *
 * \author Michael Kuhn
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
 * \author Michael Kuhn
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
	g_return_val_if_fail(iterator != NULL, NULL);
	g_return_val_if_fail(iterator->current != NULL, NULL);

	return iterator->current->data;
}

/**
 * @}
 **/
