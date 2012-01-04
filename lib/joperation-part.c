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

#include <joperation-part-internal.h>

/**
 * \defgroup JOperationPart Operation Part
 *
 * @{
 **/

/**
 * Creates a new operation part.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new operation part. Should be freed with j_operation_part_free().
 **/
JOperationPart*
j_operation_part_new (JOperationType type)
{
	JOperationPart* part;

	part = g_slice_new(JOperationPart);
	part->type = type;
	part->key = NULL;
	part->cached = FALSE;

	return part;
}

/**
 * Frees the memory allocated by an operation part.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param data An operation part.
 **/
/* FIXME */
void
j_operation_part_free (JOperationPart* part)
{
	switch (part->type)
	{
		case J_OPERATION_CREATE_STORE:
			j_connection_unref(part->u.create_store.connection);
			j_store_unref(part->u.create_store.store);
			break;
		case J_OPERATION_DELETE_STORE:
			j_connection_unref(part->u.delete_store.connection);
			j_store_unref(part->u.delete_store.store);
			break;
		case J_OPERATION_GET_STORE:
			j_connection_unref(part->u.get_store.connection);
			g_free(part->u.get_store.name);
			break;
		case J_OPERATION_STORE_CREATE_COLLECTION:
			j_store_unref(part->u.store_create_collection.store);
			j_collection_unref(part->u.store_create_collection.collection);
			break;
		case J_OPERATION_STORE_DELETE_COLLECTION:
			j_store_unref(part->u.store_delete_collection.store);
			j_collection_unref(part->u.store_delete_collection.collection);
			break;
		case J_OPERATION_STORE_GET_COLLECTION:
			j_store_unref(part->u.store_get_collection.store);
			g_free(part->u.store_get_collection.name);
			break;
		case J_OPERATION_COLLECTION_CREATE_ITEM:
			j_collection_unref(part->u.collection_create_item.collection);
			j_item_unref(part->u.collection_create_item.item);
			break;
		case J_OPERATION_COLLECTION_DELETE_ITEM:
			j_collection_unref(part->u.collection_delete_item.collection);
			j_item_unref(part->u.collection_delete_item.item);
			break;
		case J_OPERATION_COLLECTION_GET_ITEM:
			j_collection_unref(part->u.collection_get_item.collection);
			g_free(part->u.collection_get_item.name);
			break;
		case J_OPERATION_ITEM_GET_STATUS:
			j_item_unref(part->u.item_get_status.item);
			break;
		case J_OPERATION_ITEM_READ:
			j_item_unref(part->u.item_read.item);
			break;
		case J_OPERATION_ITEM_WRITE:
			if (part->cached)
			{
				g_free((gpointer)part->u.item_write.data);
			}

			j_item_unref(part->u.item_write.item);
			break;
		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	g_slice_free(JOperationPart, part);
}

gboolean
j_operation_part_can_cache (JOperationPart* part)
{
	gboolean ret = FALSE;

	switch (part->type)
	{
		case J_OPERATION_CREATE_STORE:
		case J_OPERATION_DELETE_STORE:
		case J_OPERATION_STORE_CREATE_COLLECTION:
		case J_OPERATION_STORE_DELETE_COLLECTION:
		case J_OPERATION_COLLECTION_CREATE_ITEM:
		case J_OPERATION_COLLECTION_DELETE_ITEM:
		case J_OPERATION_ITEM_WRITE:
			ret = TRUE;
			break;

		case J_OPERATION_GET_STORE:
		case J_OPERATION_STORE_GET_COLLECTION:
		case J_OPERATION_COLLECTION_GET_ITEM:
		case J_OPERATION_ITEM_GET_STATUS:
		case J_OPERATION_ITEM_READ:
			ret = FALSE;
			break;

		case J_OPERATION_NONE:
		default:
			g_warn_if_reached();
	}

	return ret;
}

void
j_operation_part_cache (JOperationPart* part)
{
	if (part->type == J_OPERATION_ITEM_WRITE)
	{
		part->cached = TRUE;
		part->u.item_write.data = g_memdup(part->u.item_write.data, part->u.item_write.length);
	}
}

/**
 * @}
 **/
