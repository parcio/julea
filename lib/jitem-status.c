/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <string.h>

#include <bson.h>

#include "jitem-status.h"
#include "jitem-status-internal.h"

#include "jcommon.h"
#include "jtrace.h"

/**
 * \defgroup JItemStatus Item Status
 *
 * Data structures and functions for managing item statuses.
 *
 * @{
 **/

/**
 * A JItemStatus.
 **/
struct JItemStatus
{
	guint flags;

	guint64 size;

	gint64 access_time;
	gint64 modification_time;

	guint ref_count;
};

JItemStatus*
j_item_status_new (JItemStatusFlags flags)
{
	JItemStatus* status;

	j_trace_enter(j_trace(), G_STRFUNC);

	status = g_slice_new(JItemStatus);
	status->flags = flags;
	status->size = 0;
	status->ref_count = 1;

	j_trace_leave(j_trace(), G_STRFUNC);

	return status;
}

JItemStatus*
j_item_status_ref (JItemStatus* status)
{
	g_return_val_if_fail(status != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	status->ref_count++;

	j_trace_leave(j_trace(), G_STRFUNC);

	return status;
}

void
j_item_status_unref (JItemStatus* status)
{
	g_return_if_fail(status != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	status->ref_count--;

	if (status->ref_count == 0)
	{
		g_slice_free(JItemStatus, status);
	}

	j_trace_leave(j_trace(), G_STRFUNC);
}

guint64
j_item_status_size (JItemStatus* status)
{
	g_return_val_if_fail(status != NULL, 0);
	g_return_val_if_fail((status->flags & J_ITEM_STATUS_SIZE) == J_ITEM_STATUS_SIZE, 0);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return status->size;
}

void
j_item_status_set_size (JItemStatus* status, guint64 size)
{
	g_return_if_fail(status != NULL);
	g_return_if_fail((status->flags & J_ITEM_STATUS_SIZE) == J_ITEM_STATUS_SIZE);

	j_trace_enter(j_trace(), G_STRFUNC);
	status->size = size;
	j_trace_leave(j_trace(), G_STRFUNC);
}

gint64
j_item_status_access_time (JItemStatus* status)
{
	g_return_val_if_fail(status != NULL, 0);
	g_return_val_if_fail((status->flags & J_ITEM_STATUS_ACCESS_TIME) == J_ITEM_STATUS_ACCESS_TIME, 0);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return status->access_time;
}

void
j_item_status_set_access_time (JItemStatus* status, gint64 access_time)
{
	g_return_if_fail(status != NULL);
	g_return_if_fail((status->flags & J_ITEM_STATUS_ACCESS_TIME) == J_ITEM_STATUS_ACCESS_TIME);

	j_trace_enter(j_trace(), G_STRFUNC);
	status->access_time = access_time;
	j_trace_leave(j_trace(), G_STRFUNC);
}

gint64
j_item_status_modification_time (JItemStatus* status)
{
	g_return_val_if_fail(status != NULL, 0);
	g_return_val_if_fail((status->flags & J_ITEM_STATUS_MODIFICATION_TIME) == J_ITEM_STATUS_MODIFICATION_TIME, 0);

	j_trace_enter(j_trace(), G_STRFUNC);
	j_trace_leave(j_trace(), G_STRFUNC);

	return status->modification_time;
}

void
j_item_status_set_modification_time (JItemStatus* status, gint64 modification_time)
{
	g_return_if_fail(status != NULL);
	g_return_if_fail((status->flags & J_ITEM_STATUS_MODIFICATION_TIME) == J_ITEM_STATUS_MODIFICATION_TIME);

	j_trace_enter(j_trace(), G_STRFUNC);
	status->modification_time = modification_time;
	j_trace_leave(j_trace(), G_STRFUNC);
}

/* Internal */

bson*
j_item_status_serialize (JItemStatus* status)
{
	bson* b;

	g_return_val_if_fail(status != NULL, NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	b = g_slice_new(bson);
	bson_init(b);
	//bson_append_oid(&b, "Collection", j_collection_id(item->collection));
	bson_append_long(b, "Size", status->size);
	bson_append_long(b, "AccessTime", status->access_time);
	bson_append_long(b, "ModificationTime", status->modification_time);
	bson_finish(b);

	j_trace_leave(j_trace(), G_STRFUNC);

	return b;
}

void
j_item_status_deserialize (JItemStatus* status, bson* b)
{
	bson_iterator iterator;

	g_return_if_fail(status != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(j_trace(), G_STRFUNC);

	bson_iterator_init(&iterator, b->data);

	while (bson_iterator_next(&iterator))
	{
		const gchar* key;

		key = bson_iterator_key(&iterator);

		if (g_strcmp0(key, "Size") == 0)
		{
			status->size = bson_iterator_long(&iterator);
		}
		else if (g_strcmp0(key, "AccessTime") == 0)
		{
			status->access_time = bson_iterator_long(&iterator);
		}
		else if (g_strcmp0(key, "ModificationTime") == 0)
		{
			status->modification_time = bson_iterator_long(&iterator);
		}
	}

	j_trace_leave(j_trace(), G_STRFUNC);
}

/**
 * @}
 **/
