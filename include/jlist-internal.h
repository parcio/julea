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

#ifndef H_LIST_INTERNAL
#define H_LIST_INTERNAL

#include <glib.h>

#include <julea-internal.h>

/**
 * A JList element.
 **/
struct JListElement
{
	/**
	 * Pointer to the next element.
	 **/
	struct JListElement* next;
	/**
	 * Pointer to data.
	 **/
	gpointer data;
};

typedef struct JListElement JListElement;

#include <jlist.h>

J_GNUC_INTERNAL JListElement* j_list_head (JList*);

#endif
