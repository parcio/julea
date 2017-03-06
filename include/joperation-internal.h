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

#ifndef H_OPERATION_INTERNAL
#define H_OPERATION_INTERNAL

#include <glib.h>

#include <julea-internal.h>

#include <jbatch.h>

#include <jitem.h>

enum JOperationType
{
	J_OPERATION_NONE,
	J_OPERATION_COLLECTION_CREATE,
	J_OPERATION_COLLECTION_DELETE,
	J_OPERATION_COLLECTION_GET,
	J_OPERATION_ITEM_CREATE,
	J_OPERATION_ITEM_GET,
	J_OPERATION_ITEM_DELETE,
	J_OPERATION_ITEM_GET_STATUS,
	J_OPERATION_ITEM_READ,
	J_OPERATION_ITEM_WRITE
};

typedef enum JOperationType JOperationType;

/**
 * An operation.
 **/
struct JOperation
{
	/**
	 * The type.
	 **/
	JOperationType type;

	gpointer key;

	union
	{
		struct
		{
			JCollection* collection;
		}
		collection_create;

		struct
		{
			JCollection* collection;
		}
		collection_delete;

		struct
		{
			JCollection** collection;
			gchar* name;
		}
		collection_get;

		struct
		{
			JCollection* collection;
			JItem* item;
		}
		item_create;

		struct
		{
			JCollection* collection;
			JItem* item;
		}
		item_delete;

		struct
		{
			JCollection* collection;
			JItem** item;
			gchar* name;
		}
		item_get;

		struct
		{
			JItem* item;
			JItemStatusFlags flags;
		}
		item_get_status;

		struct
		{
			JItem* item;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		}
		item_read;

		struct
		{
			JItem* item;
			gconstpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		}
		item_write;
	}
	u;
};

typedef struct JOperation JOperation;

J_GNUC_INTERNAL JOperation* j_operation_new (JOperationType);
J_GNUC_INTERNAL void j_operation_free (JOperation*);

#endif
