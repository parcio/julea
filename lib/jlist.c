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

#include <jlist.h>
#include <jlist-internal.h>

/**
 * \defgroup JList List
 * @{
 **/

/**
 * A linked list which allows fast prepend and append operations.
 * Also allows querying the length of the list without iterating over it.
 **/
struct JList
{
	/**
	 * The first element.
	 **/
	JListElement* head;
	/**
	 * The last element.
	 **/
	JListElement* tail;

	/**
	 * The length.
	 **/
	guint length;

	/**
	 * The function used to free the list elements.
	 **/
	JListFreeFunc free_func;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Creates a new list.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new list.
 **/
JList*
j_list_new (JListFreeFunc free_func)
{
	JList* list;

	list = g_slice_new(JList);
	list->head = NULL;
	list->tail = NULL;
	list->length = 0;
	list->free_func = free_func;
	list->ref_count = 1;

	return list;
}

/**
 * Increases the list's reference count.
 *
 * \author Michael Kuhn
 *
 * \param list A list.
 *
 * \return The list.
 **/
JList*
j_list_ref (JList* list)
{
	g_return_val_if_fail(list != NULL, NULL);

	g_atomic_int_inc(&(list->ref_count));

	return list;
}

/**
 * Decreases the list's reference count.
 * When the reference count reaches zero, frees the memory allocated for the list.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param list A list.
 * \param func A function to free the element data, or NULL.
 **/
void
j_list_unref (JList* list)
{
	g_return_if_fail(list != NULL);

	if (g_atomic_int_dec_and_test(&(list->ref_count)))
	{
		j_list_delete_all(list);

		g_slice_free(JList, list);
	}
}

/**
 * Returns the list's length.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param list A list.
 *
 * \return The list's length.
 **/
guint
j_list_length (JList* list)
{
	g_return_val_if_fail(list != NULL, 0);

	return list->length;
}

/**
 * Appends a new list element to a list.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param list A list.
 * \param data A list element.
 **/
void
j_list_append (JList* list, gpointer data)
{
	JListElement* element;

	g_return_if_fail(list != NULL);
	g_return_if_fail(data != NULL);

	element = g_slice_new(JListElement);
	element->next = NULL;
	element->data = data;

	list->length++;

	if (list->tail != NULL)
	{
		list->tail->next = element;
	}

	list->tail = element;

	if (list->head == NULL)
	{
		list->head = list->tail;
	}
}

/**
 * Prepends a new list element to a list.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param list A list.
 * \param data A list element.
 **/
void
j_list_prepend (JList* list, gpointer data)
{
	JListElement* element;

	g_return_if_fail(list != NULL);
	g_return_if_fail(data != NULL);

	element = g_slice_new(JListElement);
	element->next = list->head;
	element->data = data;

	list->length++;
	list->head = element;

	if (list->tail == NULL)
	{
		list->tail = list->head;
	}
}

/**
 * Returns the first list element.
 *
 * \author Michael Kuhn
 *
 * \param list  A list.
 *
 * \return A list element, or NULL.
 **/
gpointer
j_list_get_first (JList* list)
{
	gpointer data = NULL;

	g_return_val_if_fail(list != NULL, NULL);

	if (list->head != NULL && list->tail != NULL)
	{
		data = list->head->data;
	}

	return data;
}

/**
 * Returns the last list element.
 *
 * \author Michael Kuhn
 *
 * \param list  A list.
 *
 * \return A list element, or NULL.
 **/
gpointer
j_list_get_last (JList* list)
{
	gpointer data = NULL;

	g_return_val_if_fail(list != NULL, NULL);

	if (list->head != NULL && list->tail != NULL)
	{
		data = list->tail->data;
	}

	return data;
}

/**
 * Deletes all list elements.
 *
 * \author Michael Kuhn
 *
 * \param list A list.
 **/
void
j_list_delete_all (JList* list)
{
	JListElement* element;

	g_return_if_fail(list != NULL);

	element = list->head;

	while (element != NULL)
	{
		JListElement* next;

		if (list->free_func != NULL)
		{
			list->free_func(element->data);
		}

		next = element->next;
		g_slice_free(JListElement, element);
		element = next;
	}

	list->head = NULL;
	list->tail = NULL;
	list->length = 0;
}

/* Internal */

/**
 * Returns the list's first element.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param list A JList.
 *
 * \return A JListElement.
 **/
JListElement*
j_list_head (JList* list)
{
	g_return_val_if_fail(list != NULL, NULL);

	return list->head;
}

/**
 * @}
 **/
