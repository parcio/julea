/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
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

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <julea.h>

#include "server.h"

static guint jd_thread_num = 0;

gboolean
jd_handle_message (JMessage* message, GSocketConnection* connection, JMemoryChunk* memory_chunk, guint64 memory_chunk_size, JStatistics* statistics)
{
	J_TRACE_FUNCTION(NULL);

	gchar const* key;
	gchar const* namespace;
	gchar const* path;
	guint32 operation_count;
	JBackendOperation backend_operation;
	g_autoptr(JSemantics) semantics;
	JSemanticsSafety safety;
	gboolean message_matched = FALSE;
	guint i;

	operation_count = j_message_get_count(message);
	semantics = j_message_get_semantics(message);
	safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);

	switch (j_message_get_type(message))
	{
		case J_MESSAGE_NONE:
			break;
		case J_MESSAGE_OBJECT_CREATE:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer object;

				if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
				{
					reply = j_message_new_reply(message);
				}

				namespace = j_message_get_string(message);

				for (i = 0; i < operation_count; i++)
				{
					path = j_message_get_string(message);

					if (j_backend_object_create(jd_object_backend, namespace, path, &object))
					{
						j_statistics_add(statistics, J_STATISTICS_FILES_CREATED, 1);

						if (safety == J_SEMANTICS_SAFETY_STORAGE)
						{
							j_backend_object_sync(jd_object_backend, object);
							j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
						}

						j_backend_object_close(jd_object_backend, object);
					}

					if (reply != NULL)
					{
						j_message_add_operation(reply, 0);
					}
				}

				if (reply != NULL)
				{
					j_message_send(reply, connection);
				}
			}
			break;
		case J_MESSAGE_OBJECT_DELETE:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer object;

				if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
				{
					reply = j_message_new_reply(message);
				}

				namespace = j_message_get_string(message);

				for (i = 0; i < operation_count; i++)
				{
					path = j_message_get_string(message);

					if (j_backend_object_open(jd_object_backend, namespace, path, &object)
					    && j_backend_object_delete(jd_object_backend, object))
					{
						j_statistics_add(statistics, J_STATISTICS_FILES_DELETED, 1);
					}

					if (reply != NULL)
					{
						j_message_add_operation(reply, 0);
					}
				}

				if (reply != NULL)
				{
					j_message_send(reply, connection);
				}
			}
			break;
		case J_MESSAGE_OBJECT_READ:
			{
				JMessage* reply;
				gpointer object;

				namespace = j_message_get_string(message);
				path = j_message_get_string(message);

				reply = j_message_new_reply(message);

				// FIXME return value
				j_backend_object_open(jd_object_backend, namespace, path, &object);

				for (i = 0; i < operation_count; i++)
				{
					gchar* buf;
					guint64 length;
					guint64 offset;
					guint64 bytes_read = 0;

					length = j_message_get_8(message);
					offset = j_message_get_8(message);

					if (length > memory_chunk_size)
					{
						// FIXME return proper error
						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_read);
						continue;
					}

					buf = j_memory_chunk_get(memory_chunk, length);

					if (buf == NULL)
					{
						// FIXME ugly
						j_message_send(reply, connection);
						j_message_unref(reply);

						reply = j_message_new_reply(message);

						j_memory_chunk_reset(memory_chunk);
						buf = j_memory_chunk_get(memory_chunk, length);
					}

					j_backend_object_read(jd_object_backend, object, buf, length, offset, &bytes_read);
					j_statistics_add(statistics, J_STATISTICS_BYTES_READ, bytes_read);

					j_message_add_operation(reply, sizeof(guint64));
					j_message_append_8(reply, &bytes_read);

					if (bytes_read > 0)
					{
						j_message_add_send(reply, buf, bytes_read);
					}

					j_statistics_add(statistics, J_STATISTICS_BYTES_SENT, bytes_read);
				}

				j_backend_object_close(jd_object_backend, object);

				j_message_send(reply, connection);
				j_message_unref(reply);

				j_memory_chunk_reset(memory_chunk);
			}
			break;
		case J_MESSAGE_OBJECT_WRITE:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer object;

				if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
				{
					reply = j_message_new_reply(message);
				}

				namespace = j_message_get_string(message);
				path = j_message_get_string(message);

				// FIXME return value
				j_backend_object_open(jd_object_backend, namespace, path, &object);

				for (i = 0; i < operation_count; i++)
				{
					GInputStream* input;
					gchar* buf;
					guint64 length;
					guint64 offset;
					guint64 bytes_written = 0;

					length = j_message_get_8(message);
					offset = j_message_get_8(message);

					if (length > memory_chunk_size)
					{
						// FIXME return proper error
						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_written);
						continue;
					}

					// Guaranteed to work because memory_chunk is reset below
					buf = j_memory_chunk_get(memory_chunk, length);
					g_assert(buf != NULL);

					input = g_io_stream_get_input_stream(G_IO_STREAM(connection));
					g_input_stream_read_all(input, buf, length, NULL, NULL, NULL);
					j_statistics_add(statistics, J_STATISTICS_BYTES_RECEIVED, length);

					j_backend_object_write(jd_object_backend, object, buf, length, offset, &bytes_written);
					j_statistics_add(statistics, J_STATISTICS_BYTES_WRITTEN, bytes_written);

					if (reply != NULL)
					{
						j_message_add_operation(reply, sizeof(guint64));
						j_message_append_8(reply, &bytes_written);
					}

					j_memory_chunk_reset(memory_chunk);
				}

				if (safety == J_SEMANTICS_SAFETY_STORAGE)
				{
					j_backend_object_sync(jd_object_backend, object);
					j_statistics_add(statistics, J_STATISTICS_SYNC, 1);
				}

				j_backend_object_close(jd_object_backend, object);

				if (reply != NULL)
				{
					j_message_send(reply, connection);
				}

				j_memory_chunk_reset(memory_chunk);
			}
			break;
		case J_MESSAGE_OBJECT_STATUS:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer object;

				reply = j_message_new_reply(message);

				namespace = j_message_get_string(message);

				for (i = 0; i < operation_count; i++)
				{
					gint64 modification_time = 0;
					guint64 size = 0;

					path = j_message_get_string(message);

					// FIXME return value
					j_backend_object_open(jd_object_backend, namespace, path, &object);

					if (j_backend_object_status(jd_object_backend, object, &modification_time, &size))
					{
						j_statistics_add(statistics, J_STATISTICS_FILES_STATED, 1);
					}

					j_message_add_operation(reply, sizeof(gint64) + sizeof(guint64));
					j_message_append_8(reply, &modification_time);
					j_message_append_8(reply, &size);

					j_backend_object_close(jd_object_backend, object);
				}

				j_message_send(reply, connection);
			}
			break;
		case J_MESSAGE_STATISTICS:
			{
				g_autoptr(JMessage) reply = NULL;
				JStatistics* r_statistics;
				gchar get_all;
				guint64 value;

				get_all = j_message_get_1(message);
				r_statistics = (get_all == 0) ? statistics : jd_statistics;

				if (get_all != 0)
				{
					g_mutex_lock(jd_statistics_mutex);
					/* FIXME add statistics of all threads */
				}

				reply = j_message_new_reply(message);
				j_message_add_operation(reply, 8 * sizeof(guint64));

				value = j_statistics_get(r_statistics, J_STATISTICS_FILES_CREATED);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_FILES_DELETED);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_FILES_STATED);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_SYNC);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_READ);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_WRITTEN);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_RECEIVED);
				j_message_append_8(reply, &value);
				value = j_statistics_get(r_statistics, J_STATISTICS_BYTES_SENT);
				j_message_append_8(reply, &value);

				if (get_all != 0)
				{
					g_mutex_unlock(jd_statistics_mutex);
				}

				j_message_send(reply, connection);
			}
			break;
		case J_MESSAGE_PING:
			{
				g_autoptr(JMessage) reply = NULL;
				guint num;

				num = g_atomic_int_add(&jd_thread_num, 1);

				(void)num;
				//g_print("HELLO %d\n", num);

				reply = j_message_new_reply(message);

				if (jd_object_backend != NULL)
				{
					j_message_add_operation(reply, 7);
					j_message_append_string(reply, "object");
				}

				if (jd_kv_backend != NULL)
				{
					j_message_add_operation(reply, 3);
					j_message_append_string(reply, "kv");
				}

				j_message_send(reply, connection);
			}
			break;
		case J_MESSAGE_KV_PUT:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer batch;

				if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
				{
					reply = j_message_new_reply(message);
				}

				namespace = j_message_get_string(message);
				j_backend_kv_batch_start(jd_kv_backend, namespace, semantics, &batch);

				for (i = 0; i < operation_count; i++)
				{
					gconstpointer data;
					guint32 len;
					gboolean ret;

					key = j_message_get_string(message);
					len = j_message_get_4(message);
					data = j_message_get_n(message, len);

					ret = j_backend_kv_put(jd_kv_backend, batch, key, data, len);

					if (reply != NULL)
					{
						guint32 dummy;

						dummy = (ret) ? 1 : 0;
						j_message_add_operation(reply, 4);
						j_message_append_4(reply, &dummy);
					}
				}

				j_backend_kv_batch_execute(jd_kv_backend, batch);

				if (reply != NULL)
				{
					j_message_send(reply, connection);
				}
			}
			break;
		case J_MESSAGE_KV_DELETE:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer batch;

				if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
				{
					reply = j_message_new_reply(message);
				}

				namespace = j_message_get_string(message);
				j_backend_kv_batch_start(jd_kv_backend, namespace, semantics, &batch);

				for (i = 0; i < operation_count; i++)
				{
					gboolean ret;

					key = j_message_get_string(message);

					ret = j_backend_kv_delete(jd_kv_backend, batch, key);

					if (reply != NULL)
					{
						guint32 dummy;

						dummy = (ret) ? 1 : 0;
						j_message_add_operation(reply, 4);
						j_message_append_4(reply, &dummy);
					}
				}

				j_backend_kv_batch_execute(jd_kv_backend, batch);

				if (reply != NULL)
				{
					j_message_send(reply, connection);
				}
			}
			break;
		case J_MESSAGE_KV_GET:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer batch;

				reply = j_message_new_reply(message);
				namespace = j_message_get_string(message);
				j_backend_kv_batch_start(jd_kv_backend, namespace, semantics, &batch);

				for (i = 0; i < operation_count; i++)
				{
					gpointer value;
					guint32 len;

					key = j_message_get_string(message);

					if (j_backend_kv_get(jd_kv_backend, batch, key, &value, &len))
					{
						j_message_add_operation(reply, 4 + len);
						j_message_append_4(reply, &len);
						j_message_append_n(reply, value, len);

						g_free(value);
					}
					else
					{
						guint32 zero = 0;

						j_message_add_operation(reply, 4);
						j_message_append_4(reply, &zero);
					}
				}

				j_backend_kv_batch_execute(jd_kv_backend, batch);

				j_message_send(reply, connection);
			}
			break;
		case J_MESSAGE_KV_GET_ALL:
			{
				g_autoptr(JMessage) reply = NULL;
				gpointer iterator;
				gconstpointer value;
				guint32 len;
				guint32 zero = 0;

				reply = j_message_new_reply(message);
				namespace = j_message_get_string(message);

				j_backend_kv_get_all(jd_kv_backend, namespace, &iterator);

				while (j_backend_kv_iterate(jd_kv_backend, iterator, &key, &value, &len))
				{
					gsize key_len;

					key_len = strlen(key) + 1;

					j_message_add_operation(reply, 4 + len + key_len);
					j_message_append_4(reply, &len);
					j_message_append_n(reply, value, len);
					j_message_append_string(reply, key);
				}

				j_message_add_operation(reply, 4);
				j_message_append_4(reply, &zero);

				j_message_send(reply, connection);
			}
			break;
		case J_MESSAGE_KV_GET_BY_PREFIX:
			{
				g_autoptr(JMessage) reply = NULL;
				gchar const* prefix;
				gpointer iterator;
				gconstpointer value;
				guint32 len;
				guint32 zero = 0;

				reply = j_message_new_reply(message);
				namespace = j_message_get_string(message);
				prefix = j_message_get_string(message);

				j_backend_kv_get_by_prefix(jd_kv_backend, namespace, prefix, &iterator);

				while (j_backend_kv_iterate(jd_kv_backend, iterator, &key, &value, &len))
				{
					gsize key_len;

					key_len = strlen(key) + 1;

					j_message_add_operation(reply, 4 + len + key_len);
					j_message_append_4(reply, &len);
					j_message_append_n(reply, value, len);
					j_message_append_string(reply, key);
				}

				j_message_add_operation(reply, 4);
				j_message_append_4(reply, &zero);

				j_message_send(reply, connection);
			}
			break;
		case J_MESSAGE_DB_SCHEMA_CREATE:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_schema_create, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			// fallthrough
		case J_MESSAGE_DB_SCHEMA_GET:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_schema_get, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			// fallthrough
		case J_MESSAGE_DB_SCHEMA_DELETE:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_schema_delete, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			// fallthrough
		case J_MESSAGE_DB_INSERT:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_insert, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			// fallthrough
		case J_MESSAGE_DB_UPDATE:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_update, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			// fallthrough
		case J_MESSAGE_DB_DELETE:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_delete, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			// fallthrough
		case J_MESSAGE_DB_QUERY:
			if (!message_matched)
			{
				memcpy(&backend_operation, &j_backend_operation_db_query, sizeof(JBackendOperation));
				message_matched = TRUE;
			}
			{
				g_autoptr(JMessage) reply = NULL;
				GError* error = NULL;
				gpointer batch = NULL;
				gboolean ret = TRUE;

				reply = j_message_new_reply(message);

				for (guint j = 0; j < backend_operation.out_param_count; j++)
				{
					if (backend_operation.out_param[j].type == J_BACKEND_OPERATION_PARAM_TYPE_ERROR)
					{
						backend_operation.out_param[j].ptr = &backend_operation.out_param[j].error_ptr;
						backend_operation.out_param[j].error_ptr = NULL;
					}
					else if (backend_operation.out_param[j].type == J_BACKEND_OPERATION_PARAM_TYPE_BSON)
					{
						backend_operation.out_param[j].bson_initialized = FALSE;
						backend_operation.out_param[j].ptr = &backend_operation.out_param[j].bson;
					}
				}

				if (operation_count)
				{
					j_backend_operation_from_message_static(message, backend_operation.in_param, backend_operation.in_param_count);
				}

				switch (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY))
				{
					case J_SEMANTICS_ATOMICITY_BATCH:
						j_backend_db_batch_start(jd_db_backend, backend_operation.in_param[0].ptr, semantics, &batch, &error);
						break;
					case J_SEMANTICS_ATOMICITY_OPERATION:
					case J_SEMANTICS_ATOMICITY_NONE:
						break;
					default:
						g_warn_if_reached();
				}

				for (i = 0; i < operation_count; i++)
				{
					backend_operation.out_param[backend_operation.out_param_count - 1].error_ptr = NULL;

					if (i)
					{
						j_backend_operation_from_message_static(message, backend_operation.in_param, backend_operation.in_param_count);
					}

					switch (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY))
					{
						case J_SEMANTICS_ATOMICITY_BATCH:
							if (ret && !error)
							{
								//message must be read completely, and reply must answer all requests - but there should be no more executions in a failed 'J_SEMANTICS_ATOMICITY_BATCH'
								ret = backend_operation.backend_func(jd_db_backend, batch, &backend_operation);
							}
							else
							{
								backend_operation.out_param[backend_operation.out_param_count - 1].error_ptr = g_error_copy(error);
							}

							break;
						case J_SEMANTICS_ATOMICITY_OPERATION:
						case J_SEMANTICS_ATOMICITY_NONE:
							j_backend_db_batch_start(jd_db_backend, backend_operation.in_param[0].ptr, semantics, &batch, &error);
							ret = backend_operation.backend_func(jd_db_backend, batch, &backend_operation);
							break;
						default:
							g_warn_if_reached();
					}

					for (guint j = 0; j < backend_operation.out_param_count; j++)
					{
						if (ret && backend_operation.out_param[j].type == J_BACKEND_OPERATION_PARAM_TYPE_BSON)
						{
							backend_operation.out_param[j].bson_initialized = TRUE;
						}
					}

					switch (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY))
					{
						case J_SEMANTICS_ATOMICITY_BATCH:
							break;
						case J_SEMANTICS_ATOMICITY_OPERATION:
						case J_SEMANTICS_ATOMICITY_NONE:
							j_backend_db_batch_execute(jd_db_backend, batch, NULL);

							if (error)
							{
								g_error_free(error);
								error = NULL;
							}

							break;
						default:
							g_warn_if_reached();
					}

					if (error && !backend_operation.out_param[backend_operation.out_param_count - 1].error_ptr)
					{
						backend_operation.out_param[backend_operation.out_param_count - 1].error_ptr = g_error_copy(error);
					}

					j_backend_operation_to_message(reply, backend_operation.out_param, backend_operation.out_param_count);

					if (ret)
					{
						for (guint j = 0; j < backend_operation.out_param_count; j++)
						{
							if (backend_operation.out_param[j].type == J_BACKEND_OPERATION_PARAM_TYPE_BSON)
							{
								bson_destroy(backend_operation.out_param[j].ptr);
							}
						}
					}
				}

				switch (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY))
				{
					case J_SEMANTICS_ATOMICITY_BATCH:
						j_backend_db_batch_execute(jd_db_backend, batch, NULL);

						if (error)
						{
							g_error_free(error);
							error = NULL;
						}

						break;
					case J_SEMANTICS_ATOMICITY_OPERATION:
					case J_SEMANTICS_ATOMICITY_NONE:
						break;
					default:
						g_warn_if_reached();
				}

				j_message_send(reply, connection);
			}
			break;
		default:
			g_warn_if_reached();
			break;
	}

	return message_matched;
}
