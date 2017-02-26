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

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <bson.h>
#include <mongoc.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <jhelper-internal.h>

#include <jsemantics.h>
#include <jtrace-internal.h>

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

void
j_helper_set_nodelay (GSocketConnection* connection, gboolean enable)
{
	gint const flag = (enable) ? 1 : 0;

	GSocket* socket_;
	gint fd;

	g_return_if_fail(connection != NULL);

	j_trace_enter(G_STRFUNC);

	socket_ = g_socket_connection_get_socket(connection);
	fd = g_socket_get_fd(socket_);

	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(gint));

	j_trace_leave(G_STRFUNC);
}

void
j_helper_set_cork (GSocketConnection* connection, gboolean enable)
{
	gint const flag = (enable) ? 1 : 0;

	GSocket* socket_;
	gint fd;

	g_return_if_fail(connection != NULL);

	j_trace_enter(G_STRFUNC);

	socket_ = g_socket_connection_get_socket(connection);
	fd = g_socket_get_fd(socket_);

	setsockopt(fd, IPPROTO_TCP, TCP_CORK, &flag, sizeof(gint));

	j_trace_leave(G_STRFUNC);
}

gboolean
j_helper_insert_batch (mongoc_collection_t* collection, bson_t** obj, guint length, mongoc_write_concern_t* write_concern)
{
	gboolean ret = TRUE;

	bson_t reply[1];
	mongoc_bulk_operation_t* bulk_op;

	bulk_op = mongoc_collection_create_bulk_operation(collection, FALSE, write_concern);

	for (guint i = 0; i < length; i++)
	{
		mongoc_bulk_operation_insert(bulk_op, obj[i]);
	}

	ret = mongoc_bulk_operation_execute(bulk_op, reply, NULL);
	/* FIXME do something with reply */

	mongoc_bulk_operation_destroy(bulk_op);
	bson_destroy(reply);

	return ret;
}

void
j_helper_set_write_concern (mongoc_write_concern_t* write_concern, JSemantics* semantics)
{
	g_return_if_fail(write_concern != NULL);
	g_return_if_fail(semantics != NULL);

	j_trace_enter(G_STRFUNC);

	//mongo_write_concern_init(write_concern);

	if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) != J_SEMANTICS_SAFETY_NONE)
	{
		mongoc_write_concern_set_w(write_concern, 1);

		if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_STORAGE)
		{
			mongoc_write_concern_set_journal(write_concern, TRUE);
		}
	}

	//mongo_write_concern_finish(write_concern);

	j_trace_leave(G_STRFUNC);
}

void
j_helper_get_number_string (gchar* string, guint32 length, guint32 number)
{
	gint ret;

	/* FIXME improve */
	ret = g_snprintf(string, length, "%d", number);
	g_return_if_fail((guint)ret <= length);
}

/**
 * Returns the MongoDB collection used for collections.
 *
 * \author Michael Kuhn
 *
 * \param store A store.
 *
 * \return The MongoDB collection.
 */
/*
gchar const*
j_store_collection (JStore* store, JStoreCollection collection)
{
	gchar const* m_collection = NULL;

	g_return_val_if_fail(store != NULL, NULL);

	j_trace_enter(G_STRFUNC);

	switch (collection)
	{
		case J_STORE_COLLECTION_COLLECTIONS:
			if (G_UNLIKELY(store->collection.collections == NULL))
			{
				store->collection.collections = g_strdup_printf("%s.Collections", store->name);
			}

			m_collection = store->collection.collections;
			break;
		case J_STORE_COLLECTION_ITEMS:
			if (G_UNLIKELY(store->collection.items == NULL))
			{
				store->collection.items = g_strdup_printf("%s.Items", store->name);
			}

			m_collection = store->collection.items;
			break;
		case J_STORE_COLLECTION_LOCKS:
			if (G_UNLIKELY(store->collection.locks == NULL))
			{
				store->collection.locks = g_strdup_printf("%s.Locks", store->name);
			}

			m_collection = store->collection.locks;
			break;
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);

	return m_collection;
}
*/

void
j_helper_create_index (JStoreCollection collection, mongoc_client_t* connection, bson_t const* index)
{
	static struct
	{
		gboolean collections;
		gboolean items;
		gboolean locks;
	}
	index_created = {
		.collections = FALSE,
		.items = FALSE,
		.locks = FALSE
	};

	g_return_if_fail(connection != NULL);
	g_return_if_fail(index != NULL);

	j_trace_enter(G_STRFUNC);

	switch (collection)
	{
		case J_STORE_COLLECTION_COLLECTIONS:
			if (G_UNLIKELY(!index_created.collections))
			{
				mongoc_collection_t* m_collection;
				mongoc_index_opt_t m_index_opt[1];

				mongoc_index_opt_init(m_index_opt);
				m_index_opt->unique = TRUE;

				/* FIXME */
				m_collection = mongoc_client_get_collection(connection, "JULEA", "Collections");
				mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);
				index_created.collections = TRUE;
			}
			break;
		case J_STORE_COLLECTION_ITEMS:
			if (G_UNLIKELY(!index_created.items))
			{
				mongoc_collection_t* m_collection;
				mongoc_index_opt_t m_index_opt[1];

				mongoc_index_opt_init(m_index_opt);
				m_index_opt->unique = TRUE;

				/* FIXME */
				m_collection = mongoc_client_get_collection(connection, "JULEA", "Items");
				mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);
				index_created.items = TRUE;
			}
			break;
		case J_STORE_COLLECTION_LOCKS:
			if (G_UNLIKELY(!index_created.locks))
			{
				mongoc_collection_t* m_collection;
				mongoc_index_opt_t m_index_opt[1];

				mongoc_index_opt_init(m_index_opt);
				m_index_opt->unique = TRUE;

				/* FIXME */
				m_collection = mongoc_client_get_collection(connection, "JULEA", "Locks");
				mongoc_collection_create_index(m_collection, index, m_index_opt, NULL);
				index_created.locks = TRUE;
			}
			break;
		default:
			g_warn_if_reached();
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
