/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
