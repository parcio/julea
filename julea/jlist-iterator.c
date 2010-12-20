/*
 * Copyright (c) 2010 Michael Kuhn
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

#include "jlist-iterator.h"

#include "jlist.h"
#include "jlist-internal.h"

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
	iterator->list = list;
	iterator->current = j_list_head(iterator->list);
	iterator->first = TRUE;

	return iterator;
}

void
j_list_iterator_free (JListIterator* iterator)
{
	g_return_if_fail(iterator != NULL);

	g_slice_free(JListIterator, iterator);
}

gboolean
j_list_iterator_next (JListIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, FALSE);

	if (!iterator->first)
	{
		iterator->current = iterator->current->next;
	}
	else
	{
		iterator->first = FALSE;
	}

	return (iterator->current != NULL);
}

gpointer
j_list_iterator_get (JListIterator* iterator)
{
	g_return_val_if_fail(iterator != NULL, NULL);

	return iterator->current->data;
}

/**
 * @}
 **/
