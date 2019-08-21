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

#include <jlist.h>
#include <jlist-internal.h>

#include <jtrace.h>

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
 * \code
 * \endcode
 *
 * \param free_func A function to free the element data, or NULL.
 *
 * \return A new list.
 **/
JList*
j_list_new (JListFreeFunc free_func)
{
	J_TRACE_FUNCTION(NULL);

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
 * \param list A list.
 *
 * \return The list.
 **/
JList*
j_list_ref (JList* list)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(list != NULL, NULL);

	g_atomic_int_inc(&(list->ref_count));

	return list;
}

/**
 * Decreases the list's reference count.
 * When the reference count reaches zero, frees the memory allocated for the list.
 *
 * \code
 * \endcode
 *
 * \param list A list.
 **/
void
j_list_unref (JList* list)
{
	J_TRACE_FUNCTION(NULL);

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
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(list != NULL, 0);

	return list->length;
}

/**
 * Appends a new list element to a list.
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
	J_TRACE_FUNCTION(NULL);

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
 * \code
 * \endcode
 *
 * \param list A list.
 * \param data A list element.
 **/
void
j_list_prepend (JList* list, gpointer data)
{
	J_TRACE_FUNCTION(NULL);

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
 * \param list  A list.
 *
 * \return A list element, or NULL.
 **/
gpointer
j_list_get_first (JList* list)
{
	J_TRACE_FUNCTION(NULL);

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
 * \param list  A list.
 *
 * \return A list element, or NULL.
 **/
gpointer
j_list_get_last (JList* list)
{
	J_TRACE_FUNCTION(NULL);

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
 * \param list A list.
 **/
void
j_list_delete_all (JList* list)
{
	J_TRACE_FUNCTION(NULL);

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
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(list != NULL, NULL);

	return list->head;
}

/**
 * @}
 **/
