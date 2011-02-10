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

#include "jmongo.h"

#include "jbson.h"
#include "jbson-iterator.h"
#include "jlist-iterator.h"
#include "jmongo-connection.h"
#include "jmongo-iterator.h"
#include "jmongo-message.h"

/**
 * \defgroup JMongo MongoDB Convenience Functions
 *
 * @{
 **/

static const gint32 j_mongo_zero = 0;

static gchar*
j_mongo_get_db (const gchar* ns)
{
	gchar* pos;

	pos = strchr(ns, '.');

	if (pos == NULL)
	{
		return g_strdup(ns);
	}

	return g_strndup(ns, pos - ns);
}

void
j_mongo_create_index(JMongoConnection* connection, const gchar* collection, JBSON* bson, gboolean is_unique)
{
	JBSON* index;
	JBSONIterator* iterator;
	gchar name[255];
	gsize pos;
	gchar* db;
	gchar* ns;

	g_return_if_fail(connection != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(bson != NULL);

	iterator = j_bson_iterator_new(bson);
	pos = 0;

	while (j_bson_iterator_next(iterator))
	{
		const gchar* key;
		gsize len;

		key = j_bson_iterator_get_key(iterator);
		len = strlen(key);

		if (pos >= 255)
		{
			break;
		}

		if (pos > 0)
		{
			g_strlcpy(name + pos, "_", 255 - pos);
			pos += 1;
		}

		g_strlcpy(name + pos, key, 255 - pos);
		pos += len;
	}

	j_bson_iterator_free(iterator);

	if (pos == 0)
	{
		return;
	}

	index = j_bson_new();

	j_bson_append_document(index, "key", bson);
	j_bson_append_string(index, "ns", collection);
	j_bson_append_string(index, "name", name);

	if (is_unique)
	{
		j_bson_append_boolean(index, "unique", TRUE);
	}

	db = j_mongo_get_db(collection);
	ns = g_strdup_printf("%s.system.indexes", db);

	j_mongo_insert(connection, ns, index);

	j_bson_unref(index);

	g_free(db);
	g_free(ns);
}

void
j_mongo_insert (JMongoConnection* connection, const gchar* collection, JBSON* bson)
{
	JMongoMessage* message;
	gsize length;
	gsize message_length;

	g_return_if_fail(connection != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(bson != NULL);

	length = strlen(collection) + 1;
	message_length = 4 + length + j_bson_size(bson);

	message = j_mongo_message_new(message_length, J_MONGO_MESSAGE_OP_INSERT);

	j_mongo_message_append_4(message, &j_mongo_zero);
	j_mongo_message_append_n(message, collection, length);
	j_mongo_message_append_n(message, j_bson_data(bson), j_bson_size(bson));

	j_mongo_connection_send(connection, message);

	j_mongo_message_free(message);
}

void
j_mongo_insert_list (JMongoConnection* connection, const gchar* collection, JList* list)
{
	JListIterator* it;
	JMongoMessage* message;
	gsize length;
	gsize message_length;

	g_return_if_fail(connection != NULL);
	g_return_if_fail(collection != NULL);
	g_return_if_fail(list != NULL);

	length = strlen(collection) + 1;
	message_length = 4 + length;
	it = j_list_iterator_new(list);

	while (j_list_iterator_next(it))
	{
		JBSON* bson = j_list_iterator_get(it);

		message_length += j_bson_size(bson);
	}

	j_list_iterator_free(it);

	message = j_mongo_message_new(message_length, J_MONGO_MESSAGE_OP_INSERT);

	j_mongo_message_append_4(message, &j_mongo_zero);
	j_mongo_message_append_n(message, collection, length);

	it = j_list_iterator_new(list);

	while (j_list_iterator_next(it))
	{
		JBSON* bson = j_list_iterator_get(it);

		j_mongo_message_append_n(message, j_bson_data(bson), j_bson_size(bson));
	}

	j_list_iterator_free(it);

	j_mongo_connection_send(connection, message);

	j_mongo_message_free(message);
}

JMongoIterator*
j_mongo_find (JMongoConnection* connection, const gchar* collection, JBSON* query, JBSON* fields, gint32 number_to_skip, gint32 number_to_return)
{
	JMongoMessage* message;
	JMongoReply* reply;
	gsize length;
	gsize message_length;

	g_return_val_if_fail(connection != NULL, NULL);
	g_return_val_if_fail(collection != NULL, NULL);
	g_return_val_if_fail(query != NULL, NULL);

	length = strlen(collection) + 1;
	message_length = 4 + length + 4 + 4 + j_bson_size(query);

	if (fields != NULL)
	{
		message_length += j_bson_size(fields);
	}

	message = j_mongo_message_new(message_length, J_MONGO_MESSAGE_OP_QUERY);

	j_mongo_message_append_4(message, &j_mongo_zero);
	j_mongo_message_append_n(message, collection, length);
	j_mongo_message_append_4(message, &number_to_skip);
	j_mongo_message_append_4(message, &number_to_return);
	j_mongo_message_append_n(message, j_bson_data(query), j_bson_size(query));

	if (fields != NULL)
	{
		j_mongo_message_append_n(message, j_bson_data(fields), j_bson_size(fields));
	}

	j_mongo_connection_send(connection, message);

	j_mongo_message_free(message);

	reply = j_mongo_connection_receive(connection);

	return j_mongo_iterator_new(connection, collection, reply);
}

/**
 * @}
 **/
