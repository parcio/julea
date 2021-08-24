/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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
#include <gio/gio.h>

#include <math.h>
#include <string.h>

#include <jmessage.h>

#include <jnetwork.h>
#include <jhelper.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jsemantics.h>
#include <jtrace.h>

#define EXE(cmd, ...) do { if(cmd == FALSE) { g_warning(__VA_ARGS__); goto end; } } while(FALSE)

/**
 * \addtogroup JMessage Message
 *
 * @{
 **/

enum JMessageSemantics
{
	J_MESSAGE_SEMANTICS_ATOMICITY_BATCH = 1 << 0,
	J_MESSAGE_SEMANTICS_ATOMICITY_OPERATION = 1 << 1,
	J_MESSAGE_SEMANTICS_ATOMICITY_NONE = 1 << 2,
	J_MESSAGE_SEMANTICS_CONCURRENCY_OVERLAPPING = 1 << 3,
	J_MESSAGE_SEMANTICS_CONCURRENCY_NON_OVERLAPPING = 1 << 4,
	J_MESSAGE_SEMANTICS_CONCURRENCY_NONE = 1 << 5,
	J_MESSAGE_SEMANTICS_CONSISTENCY_IMMEDIATE = 1 << 6,
	J_MESSAGE_SEMANTICS_CONSISTENCY_EVENTUAL = 1 << 7,
	J_MESSAGE_SEMANTICS_CONSISTENCY_NONE = 1 << 8,
	J_MESSAGE_SEMANTICS_ORDERING_STRICT = 1 << 9,
	J_MESSAGE_SEMANTICS_ORDERING_SEMI_RELAXED = 1 << 10,
	J_MESSAGE_SEMANTICS_ORDERING_RELAXED = 1 << 11,
	J_MESSAGE_SEMANTICS_PERSISTENCY_IMMEDIATE = 1 << 12,
	J_MESSAGE_SEMANTICS_PERSISTENCY_EVENTUAL = 1 << 13,
	J_MESSAGE_SEMANTICS_PERSISTENCY_NONE = 1 << 14,
	J_MESSAGE_SEMANTICS_SAFETY_STORAGE = 1 << 15,
	J_MESSAGE_SEMANTICS_SAFETY_NETWORK = 1 << 16,
	J_MESSAGE_SEMANTICS_SAFETY_NONE = 1 << 17,
	J_MESSAGE_SEMANTICS_SECURITY_STRICT = 1 << 18,
	J_MESSAGE_SEMANTICS_SECURITY_NONE = 1 << 19
};

typedef enum JMessageSemantics JMessageSemantics;

/**
 * Additional message data.
 **/
struct JMessageData
{
	/**
	 * The data.
	 **/
	gconstpointer data;

	/**
	 * The data length.
	 **/
	guint64 length;

	void* header;
	guint64 header_size;
};

typedef struct JMessageData JMessageData;

/**
 * A message header.
 **/
#pragma pack(4)
struct JMessageHeader
{
	/**
	 * The message length.
	 **/
	guint32 length;

	/**
	 * The message ID.
	 **/
	guint32 id;

	/**
	 * The semantics.
	 **/
	guint32 semantics;

	/**
	 * The operation type.
	 **/
	guint32 op_type;

	/**
	 * The operation count.
	 **/
	guint32 op_count;
};
#pragma pack()

typedef struct JMessageHeader JMessageHeader;

G_STATIC_ASSERT(sizeof(JMessageHeader) == 5 * sizeof(guint32));

/**
 * A message.
 **/
struct JMessage
{
	/**
	 * The header.
	 */
	JMessageHeader header;

	/**
	 * The current size.
	 **/
	gsize size;

	/**
	 * The data.
	 **/
	gchar* data;

	/**
	 * The current position within #data.
	 **/
	gchar* current;

	/**
	 * The list of additional data to send in j_message_write().
	 * Contains JMessageData elements.
	 **/
	JList* send_list;

	/**
	 * The original message.
	 * Set if the message is a reply, NULL otherwise.
	 **/
	JMessage* original_message;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Returns a message's length.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 *
 * \return The message's length.
 **/
static gsize
j_message_length(JMessage const* message)
{
	J_TRACE_FUNCTION(NULL);

	guint32 length;

	length = message->header.length;

	return GUINT32_FROM_LE(length);
}

static void
j_message_data_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	g_slice_free(JMessageData, data);
}

/**
 * Checks whether it is possible to append data to a message.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  A length.
 *
 * \return TRUE if it is possible, FALSE otherwise.
 **/
static gboolean
j_message_can_append(JMessage const* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	return (message->current + length <= message->data + message->size);
}

/**
 * Checks whether it is possible to get data from a message.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param message A message.
 * \param length  A length.
 *
 * \return TRUE if it is possible, FALSE otherwise.
 **/
static gboolean
j_message_can_get(JMessage const* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	return (message->current + length <= message->data + j_message_length(message));
}

static void
j_message_extend(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	gsize factor = 1;
	gsize current_length;
	gsize position;
	guint32 count;

	if (length == 0)
	{
		return;
	}

	current_length = j_message_length(message);

	if (current_length + length <= message->size)
	{
		return;
	}

	count = j_message_get_count(message);

	if (count > 10)
	{
		factor = pow(10, floor(log10(count)));
	}

	message->size += length * factor;

	position = message->current - message->data;
	message->data = g_realloc(message->data, message->size);
	message->current = message->data + position;
}

static void
j_message_ensure_size(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	gsize position;

	if (length <= message->size)
	{
		return;
	}

	message->size = length;

	position = message->current - message->data;
	message->data = g_realloc(message->data, message->size);
	message->current = message->data + position;
}

JMessage*
j_message_new(JMessageType op_type, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	JMessage* message;
	guint32 rand;

	//g_return_val_if_fail(op_type != J_MESSAGE_NONE, NULL);

	length = MAX(256, length);
	rand = g_random_int();

	message = g_slice_new(JMessage);
	message->size = length;
	message->data = g_malloc(message->size);
	message->current = message->data;
	message->send_list = j_list_new(j_message_data_free);
	message->original_message = NULL;
	message->ref_count = 1;

	message->header.length = GUINT32_TO_LE(0);
	message->header.id = GUINT32_TO_LE(rand);
	message->header.semantics = GUINT32_TO_LE(0);
	message->header.op_type = GUINT32_TO_LE(op_type);
	message->header.op_count = GUINT32_TO_LE(0);

	return message;
}

JMessage*
j_message_new_reply(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	JMessage* reply;

	g_return_val_if_fail(message != NULL, NULL);

	reply = g_slice_new(JMessage);
	reply->size = 256;
	reply->data = g_malloc(reply->size);
	reply->current = reply->data;
	reply->send_list = j_list_new(j_message_data_free);
	reply->original_message = j_message_ref(message);
	reply->ref_count = 1;

	reply->header.length = GUINT32_TO_LE(0);
	reply->header.id = message->header.id;
	reply->header.semantics = GUINT32_TO_LE(0);
	reply->header.op_type = message->header.op_type;
	reply->header.op_count = GUINT32_TO_LE(0);

	return reply;
}

JMessage*
j_message_ref(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, NULL);

	g_atomic_int_inc(&(message->ref_count));

	return message;
}

void
j_message_unref(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(message != NULL);

	if (g_atomic_int_dec_and_test(&(message->ref_count)))
	{
		if (message->original_message != NULL)
		{
			j_message_unref(message->original_message);
		}

		if (message->send_list != NULL)
		{
			j_list_unref(message->send_list);
		}

		g_free(message->data);

		g_slice_free(JMessage, message);
	}
}

JMessageType
j_message_get_type(JMessage const* message)
{
	J_TRACE_FUNCTION(NULL);

	JMessageType op_type;

	g_return_val_if_fail(message != NULL, J_MESSAGE_NONE);

	op_type = message->header.op_type;
	op_type = GUINT32_FROM_LE(op_type);

	return op_type;
}

guint32
j_message_get_count(JMessage const* message)
{
	J_TRACE_FUNCTION(NULL);

	guint32 op_count;

	g_return_val_if_fail(message != NULL, 0);

	op_count = message->header.op_count;
	op_count = GUINT32_FROM_LE(op_count);

	return op_count;
}

gboolean
j_message_append_1(JMessage* message, gconstpointer data)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 1), FALSE);

	*(message->current) = *((gchar const*)data);
	message->current += 1;

	new_length = j_message_length(message) + 1;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

gboolean
j_message_append_4(JMessage* message, gconstpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gint32 new_data;
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 4), FALSE);

	new_data = GINT32_TO_LE(*((gint32 const*)data));
	memcpy(message->current, &new_data, 4);
	message->current += 4;

	new_length = j_message_length(message) + 4;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

gboolean
j_message_append_8(JMessage* message, gconstpointer data)
{
	J_TRACE_FUNCTION(NULL);

	gint64 new_data;
	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, 8), FALSE);

	new_data = GINT64_TO_LE(*((gint64 const*)data));
	memcpy(message->current, &new_data, 8);
	message->current += 8;

	new_length = j_message_length(message) + 8;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

gboolean
j_message_append_n(JMessage* message, gconstpointer data, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_length;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(data != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, length), FALSE);

	memcpy(message->current, data, length);
	message->current += length;

	new_length = j_message_length(message) + length;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

gboolean
j_message_append_memory_id(JMessage* message, const struct JConnectionMemoryID* id)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_length;
	guint64 padding = (guint64)message->current % 8;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(id != NULL, FALSE);
	g_return_val_if_fail(j_message_can_append(message, sizeof(*id)+padding), FALSE);

	memcpy(message->current+padding, id, sizeof(*id));
	message->current += sizeof(*id) + padding;

	new_length = j_message_length(message) + sizeof(*id) + padding;
	message->header.length = GUINT32_TO_LE(new_length);

	return TRUE;
}

gboolean
j_message_append_string(JMessage* message, gchar const* str)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(str != NULL, FALSE);

	return j_message_append_n(message, str, strlen(str) + 1);
}

gchar
j_message_get_1(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gchar ret;

	g_return_val_if_fail(message != NULL, '\0');
	g_return_val_if_fail(j_message_can_get(message, 1), '\0');

	ret = *((gchar const*)(message->current));
	message->current += 1;

	return ret;
}

gint32
j_message_get_4(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gint32 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(j_message_can_get(message, 4), 0);

	memcpy(&ret, message->current, 4);
	ret = GINT32_FROM_LE(ret);
	message->current += 4;

	return ret;
}

gint64
j_message_get_8(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gint64 ret;

	g_return_val_if_fail(message != NULL, 0);
	g_return_val_if_fail(j_message_can_get(message, 8), 0);

	memcpy(&ret, message->current, 8);
	ret = GINT64_FROM_LE(ret);
	message->current += 8;

	return ret;
}

gpointer
j_message_get_n(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	gpointer ret;

	g_return_val_if_fail(message != NULL, NULL);
	g_return_val_if_fail(j_message_can_get(message, length), NULL);

	ret = message->current;
	message->current += length;

	return ret;
}

gchar const*
j_message_get_string(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	gchar const* ret;

	g_return_val_if_fail(message != NULL, NULL);

	ret = message->current;
	message->current += strlen(ret) + 1;

	return ret;
}

const struct JConnectionMemoryID*
j_message_get_memory_id(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	const struct JConnectionMemoryID* ret;
	guint64 padding;

	g_return_val_if_fail(message != NULL, NULL);

	padding = (guint64)message->current % 8;

	ret = (const void*)(message->current + padding);
	message->current += sizeof(*ret) + padding;

	return ret;
}

gboolean
j_message_receive(JMessage* message, struct JConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(connection != NULL, FALSE);

	return j_message_read(message, connection);
}

gboolean
j_message_send_ack(JMessage* message, struct JConnection* connection)
{
	const JConnectionAck ack = J_CONNECTION_ACK;
	
	// No data where send -> no need to acknowledge
	if(message != NULL && j_message_get_count(message) == 0) { return TRUE; }
	EXE(j_connection_wait_for_completion(connection),
			"Failed to wait to finishe all operations before sending ack!");
	EXE(j_connection_send(connection, &ack, sizeof(ack)),
			"Failed to initiated sending ACK!");
	EXE(j_connection_wait_for_completion(connection),
			"Failed to verify completion of sending ACK!");
	return TRUE;
end:
	return FALSE;
}

struct JConnectionMemory {
	struct fid_mr* memory_region;
	guint64 addr;
	guint64 size;
};

gboolean
j_message_send(JMessage* message, struct JConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret;
	int n_memory_regions = 0;
	struct JConnectionMemory* memory_itr = NULL;
	struct JConnectionMemory* memory_regions = NULL;
	struct JConnectionMemory* memory_regions_end = NULL;
	struct JConnectionMemoryID mem_id;
	g_autoptr(JListIterator) iterator = NULL;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(connection != NULL, FALSE);

	ret = FALSE;

	n_memory_regions = j_list_length(message->send_list);
	if(n_memory_regions) {
		size_t size = sizeof(struct JConnectionMemory) * n_memory_regions;
		memory_regions = malloc(size);
		memset(memory_regions, 0, size);
		memory_regions_end = memory_regions + n_memory_regions;
	}
	memory_itr = memory_regions;

	if (message->send_list != NULL)
	{
		iterator = j_list_iterator_new(message->send_list);

		while (j_list_iterator_next(iterator))
		{
			JMessageData* message_data = j_list_iterator_get(iterator);

			EXE(j_connection_rma_register(connection, message_data->data, message_data->length, memory_itr),
					"Failed to register message data memory!");

			j_message_add_operation(message, sizeof(struct JConnectionMemoryID) + message_data->header_size);

			EXE(j_connection_memory_get_id(memory_itr, &mem_id),
					"Failed to get memory it!");
			if(message_data->header_size > 0) {
				if(message_data->header_size <= sizeof(message_data->header)) {
					EXE(j_message_append_n(message, &message_data->header, message_data->header_size),
							"Failed to append header");
				} else {
					EXE(j_message_append_n(message, message_data->header, message_data->header_size),
							"Failed to append header");
				}
			}
			EXE(j_message_append_memory_id(message, &mem_id),
					"Failed to append memory id to message!");
			++memory_itr;
		}
	}

	ret = j_message_write(message, connection);

end:
	if(memory_regions != NULL) {
		struct JConnectionMemory* mrs = memory_regions;
		while(memory_regions != memory_itr) {
			j_connection_rma_unregister(connection, memory_regions++);
		}
		if(memory_itr != memory_regions_end && memory_itr->memory_region) {
			j_connection_rma_unregister(connection, memory_itr);
		}
		free(mrs);
	}
	return ret;
}

gboolean
j_message_read(JMessage* message, struct JConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	GError* error = NULL;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(connection != NULL, FALSE);

	EXE(j_connection_recv(connection, sizeof(message->header), &(message->header)),
			"Failed to initiated header receive!");
	if(!j_connection_wait_for_completion(connection)) {
		EXE(j_connection_closed(connection),
			"Failed to wait for header receive!");
		goto end;
	}

	j_message_ensure_size(message, j_message_length(message));

	if(j_message_length(message) > 0) {
		EXE(j_connection_recv(connection, j_message_length(message), message->data),
				"Failed to initiated message body receive!");
		EXE(j_connection_wait_for_completion(connection),
				"Failed to wait for message body receive!");
	}

	message->current = message->data;

	if (message->original_message != NULL)
	{
		g_assert(message->header.id == message->original_message->header.id);
	}

	ret = TRUE;

end:
	if (error != NULL)
	{
		g_critical("%s", error->message);
		g_error_free(error);
	}

	return ret;
}

gboolean
j_message_write(JMessage* message, struct JConnection* connection)
{
	J_TRACE_FUNCTION(NULL);

	gboolean ret = FALSE;

	JConnectionAck ack;

	g_return_val_if_fail(message != NULL, FALSE);
	g_return_val_if_fail(connection != NULL, FALSE);

	EXE(j_connection_send(connection, &(message->header), sizeof(message->header)),
			"Failed to initiated sending message header.");
	if(j_message_length(message) > 0) {
		EXE(j_connection_send(connection, message->data, j_message_length(message)),
				"Failed to initiated sending message body.");
	}

	if(j_list_length(message->send_list)) {
		EXE(j_connection_recv(connection, sizeof(ack), &ack),
				"Failed to initiated ACK receive.");
	}

	EXE(j_connection_wait_for_completion(connection),
			"Failed to wait for completion of message send!");

	if(j_list_length(message->send_list) && ack != J_CONNECTION_ACK) {
		g_warning("Wrong ACK flag received! got: %i, instead of: %i", ack, J_CONNECTION_ACK);
		goto end;
	}

	ret = TRUE;

end:
	return ret;
}

void
j_message_add_send(JMessage* message, gconstpointer data, guint64 length, void* header, guint64 h_size)
{
	J_TRACE_FUNCTION(NULL);

	JMessageData* message_data;

	g_return_if_fail(message != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);

	message_data = g_slice_new(JMessageData);
	message_data->data = data;
	message_data->length = length;
	if(h_size == 0) {
		message_data->header = NULL;
		message_data->header_size = 0;
	} else if(h_size <= sizeof(message_data->header)) {
		memcpy(&message_data->header, header, h_size);
		message_data->header_size = h_size;
	} else {
		message_data->header = header;
		message_data->header_size = h_size;
	}

	j_list_append(message->send_list, message_data);
}

void
j_message_add_operation(JMessage* message, gsize length)
{
	J_TRACE_FUNCTION(NULL);

	guint32 new_op_count;

	g_return_if_fail(message != NULL);

	new_op_count = j_message_get_count(message) + 1;
	message->header.op_count = GUINT32_TO_LE(new_op_count);

	j_message_extend(message, length);
}

void
j_message_set_semantics(JMessage* message, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	guint32 serialized_semantics = 0;

	g_return_if_fail(message != NULL);
	g_return_if_fail(semantics != NULL);

#define SERIALIZE_SEMANTICS(type, key) \
	{ \
		gint tmp; \
		tmp = j_semantics_get(semantics, J_SEMANTICS_##type); \
		if (tmp == J_SEMANTICS_##type##_##key) \
		{ \
			serialized_semantics |= J_MESSAGE_SEMANTICS_##type##_##key; \
		} \
	}

	SERIALIZE_SEMANTICS(ATOMICITY, BATCH)
	SERIALIZE_SEMANTICS(ATOMICITY, OPERATION)
	SERIALIZE_SEMANTICS(ATOMICITY, NONE)
	SERIALIZE_SEMANTICS(CONCURRENCY, OVERLAPPING)
	SERIALIZE_SEMANTICS(CONCURRENCY, NON_OVERLAPPING)
	SERIALIZE_SEMANTICS(CONCURRENCY, NONE)
	SERIALIZE_SEMANTICS(CONSISTENCY, IMMEDIATE)
	SERIALIZE_SEMANTICS(CONSISTENCY, EVENTUAL)
	SERIALIZE_SEMANTICS(CONSISTENCY, NONE)
	SERIALIZE_SEMANTICS(ORDERING, STRICT)
	SERIALIZE_SEMANTICS(ORDERING, SEMI_RELAXED)
	SERIALIZE_SEMANTICS(ORDERING, RELAXED)
	SERIALIZE_SEMANTICS(PERSISTENCY, IMMEDIATE)
	SERIALIZE_SEMANTICS(PERSISTENCY, EVENTUAL)
	SERIALIZE_SEMANTICS(PERSISTENCY, NONE)
	SERIALIZE_SEMANTICS(SAFETY, STORAGE)
	SERIALIZE_SEMANTICS(SAFETY, NETWORK)
	SERIALIZE_SEMANTICS(SAFETY, NONE)
	SERIALIZE_SEMANTICS(SECURITY, STRICT)
	SERIALIZE_SEMANTICS(SECURITY, NONE)

#undef SERIALIZE_SEMANTICS

	message->header.semantics = GUINT32_TO_LE(serialized_semantics);
}

JSemantics*
j_message_get_semantics(JMessage* message)
{
	J_TRACE_FUNCTION(NULL);

	JSemantics* semantics;

	guint32 serialized_semantics;

	g_return_val_if_fail(message != NULL, NULL);

	serialized_semantics = message->header.semantics;
	serialized_semantics = GUINT32_FROM_LE(serialized_semantics);

	// If serialized_semantics is 0, we will end up with the default semantics.
	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

#define DESERIALIZE_SEMANTICS(type, key) \
	if (serialized_semantics & J_MESSAGE_SEMANTICS_##type##_##key) \
	{ \
		j_semantics_set(semantics, J_SEMANTICS_##type, J_SEMANTICS_##type##_##key); \
	}

	DESERIALIZE_SEMANTICS(ATOMICITY, BATCH)
	DESERIALIZE_SEMANTICS(ATOMICITY, OPERATION)
	DESERIALIZE_SEMANTICS(ATOMICITY, NONE)
	DESERIALIZE_SEMANTICS(CONCURRENCY, OVERLAPPING)
	DESERIALIZE_SEMANTICS(CONCURRENCY, NON_OVERLAPPING)
	DESERIALIZE_SEMANTICS(CONCURRENCY, NONE)
	DESERIALIZE_SEMANTICS(CONSISTENCY, IMMEDIATE)
	DESERIALIZE_SEMANTICS(CONSISTENCY, EVENTUAL)
	DESERIALIZE_SEMANTICS(CONSISTENCY, NONE)
	DESERIALIZE_SEMANTICS(ORDERING, STRICT)
	DESERIALIZE_SEMANTICS(ORDERING, SEMI_RELAXED)
	DESERIALIZE_SEMANTICS(ORDERING, RELAXED)
	DESERIALIZE_SEMANTICS(PERSISTENCY, IMMEDIATE)
	DESERIALIZE_SEMANTICS(PERSISTENCY, EVENTUAL)
	DESERIALIZE_SEMANTICS(PERSISTENCY, NONE)
	DESERIALIZE_SEMANTICS(SAFETY, STORAGE)
	DESERIALIZE_SEMANTICS(SAFETY, NETWORK)
	DESERIALIZE_SEMANTICS(SAFETY, NONE)
	DESERIALIZE_SEMANTICS(SECURITY, STRICT)
	DESERIALIZE_SEMANTICS(SECURITY, NONE)

#undef DESERIALIZE_SEMANTICS

	return semantics;
}

/**
 * @}
 **/
