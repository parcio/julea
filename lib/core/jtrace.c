/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
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
#include <jconfiguration.h>

#include <glib.h>
#include <glib/gprintf.h>

#ifdef HAVE_OTF
#include <otf.h>
#endif

#include <jtrace.h>

#include <bson.h>

/**
 * \addtogroup JTrace
 *
 * @{
 **/

/**
 * Flags used to enable specific trace back-ends.
 */
enum JTraceFlags
{
	J_TRACE_OFF = 0,
	J_TRACE_ECHO = 1 << 0,
	J_TRACE_OTF = 1 << 1,
	J_TRACE_SUMMARY = 1 << 2,
	J_TRACE_ACCESS = 1 << 3
};

typedef enum JTraceFlags JTraceFlags;

struct JTraceStack
{
	gchar* name;
	guint64 enter_time;
};

typedef struct JTraceStack JTraceStack;

struct JTraceTime
{
	gdouble time;
	guint count;
};

typedef struct JTraceTime JTraceTime;

struct Access
{
	guint64 timestamp;
	guint32 uid;
	const char* program_name;
	const char* backend;
	const char* type;
	const char* path;
	const char* namespace;
	const char* name;
	const char* operation;
	guint64 size;
	guint32 complexity;
	const bson_t* bson;
};
typedef struct Access Access;

/**
 * A trace thread.
 **/
struct JTraceThread
{
	/**
	 * Thread name.
	 * "Main thread" or "Thread %d".
	 **/
	gchar* thread_name;

	/**
	 * Function depth within the current thread.
	 **/
	guint function_depth;

	GArray* stack;

#ifdef HAVE_OTF
	/**
	 * OTF-specific structure.
	 **/
	struct
	{
		/**
		 * Thread's process ID.
		 **/
		guint32 process_id;
	} otf;
#endif

	struct
	{
		char* program_name;
		guint32 uid;
	} client;

	struct
	{
		gboolean inside;
		Access row;
		const void* utility_ptr;
		struct
		{
			char* type;
			char* config_path;
			char* namespace;
		} db;
		struct
		{
			char* type;
			char* config_path;
			char* namespace;
			char* name;
		} kv;
		struct
		{
			char* type;
			char* config_path;
			char* namespace;
			char* path;
		} object;

	} access;
};

typedef struct JTraceThread JTraceThread;

struct JTrace
{
	gchar* name;
	guint64 enter_time;
};

static JTraceFlags j_trace_flags = J_TRACE_OFF;

static gchar* j_trace_name = NULL;
static gint j_trace_thread_id = 1;

static GPatternSpec** j_trace_function_patterns = NULL;

#ifdef HAVE_OTF
static OTF_FileManager* otf_manager = NULL;
static OTF_Writer* otf_writer = NULL;

static guint32 otf_process_id = 1;
static guint32 otf_function_id = 1;
static guint32 otf_file_id = 1;
static guint32 otf_counter_id = 1;

static GHashTable* otf_function_table = NULL;
static GHashTable* otf_file_table = NULL;
static GHashTable* otf_counter_table = NULL;

G_LOCK_DEFINE_STATIC(j_trace_otf);
#endif

static void j_trace_thread_default_free(gpointer);

static GPrivate j_trace_thread_default = G_PRIVATE_INIT(j_trace_thread_default_free);
static GHashTable* j_trace_summary_table = NULL;

G_LOCK_DEFINE_STATIC(j_trace_echo);
G_LOCK_DEFINE_STATIC(j_trace_summary);

static void
j_trace_access_print_header(void)
{
	g_printerr("time,process_uid,program_name,backend,type,path,namespace,name,operation,size,complexity,duration,bson\n");
}
/**
 * Creates a new trace thread.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param thread A thread.
 *
 * \return A new trace thread. Should be freed with j_trace_thread_free().
 **/
static JTraceThread*
j_trace_thread_new(GThread* thread)
{
	JTraceThread* trace_thread;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return NULL;
	}

	trace_thread = g_slice_new(JTraceThread);
	memset(trace_thread, 0, sizeof(JTraceThread));
	trace_thread->function_depth = 0;
	trace_thread->stack = g_array_new(FALSE, FALSE, sizeof(JTraceStack));
	trace_thread->client.program_name = NULL;
	trace_thread->client.uid = 0;

	if (thread == NULL)
	{
		trace_thread->thread_name = g_strdup("Main process");
	}
	else
	{
		guint thread_id;

		/// \todo use name?
		thread_id = g_atomic_int_add(&j_trace_thread_id, 1);
		trace_thread->thread_name = g_strdup_printf("Thread %d", thread_id);
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		trace_thread->otf.process_id = g_atomic_int_add(&otf_process_id, 1);

		OTF_Writer_writeDefProcess(otf_writer, 0, trace_thread->otf.process_id, trace_thread->thread_name, 0);
		OTF_Writer_writeBeginProcess(otf_writer, g_get_real_time(), trace_thread->otf.process_id);
	}
#endif

	return trace_thread;
}

/**
 * Decreases a trace thread's reference count.
 * When the reference count reaches zero, frees the memory allocated for the trace thread.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param trace_thread A trace thread.
 **/
static void
j_trace_thread_free(JTraceThread* trace_thread)
{
	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	g_return_if_fail(trace_thread != NULL);

	if (j_trace_flags & J_TRACE_ACCESS)
	{
		g_free(trace_thread->access.db.type);
		g_free(trace_thread->access.db.config_path);
		g_free(trace_thread->access.kv.type);
		g_free(trace_thread->access.kv.config_path);
		g_free(trace_thread->access.object.path);
		g_free(trace_thread->access.object.config_path);
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		OTF_Writer_writeEndProcess(otf_writer, g_get_real_time(), trace_thread->otf.process_id);
	}
#endif

	g_free(trace_thread->thread_name);
	g_array_free(trace_thread->stack, TRUE);
	g_slice_free(JTraceThread, trace_thread);
}

/**
 * Returns the default trace thread.
 *
 * \private
 *
 * \return The default trace thread.
 **/
static JTraceThread*
j_trace_thread_get_default(void)
{
	JTraceThread* trace_thread;

	trace_thread = g_private_get(&j_trace_thread_default);

	if (G_UNLIKELY(trace_thread == NULL))
	{
		trace_thread = j_trace_thread_new(g_thread_self());
		g_private_replace(&j_trace_thread_default, trace_thread);
	}

	return trace_thread;
}

static void
j_trace_thread_default_free(gpointer data)
{
	JTraceThread* trace_thread = data;

	j_trace_thread_free(trace_thread);
}

/**
 * Prints a common header to stderr.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param trace_thread A trace thread.
 * \param timestamp    A timestamp.
 **/
static void
j_trace_echo_printerr(JTraceThread* trace_thread, guint64 timestamp)
{
	guint i;

	g_printerr("[%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "] %s %s: ", timestamp / G_USEC_PER_SEC, timestamp % G_USEC_PER_SEC, j_trace_name, trace_thread->thread_name);

	for (i = 0; i < trace_thread->function_depth; i++)
	{
		g_printerr("  ");
	}
}

/**
 * Returns the name of a file operation.
 *
 * \private
 *
 * \code
 * \endcode
 *
 * \param op A file operation.
 *
 * \return A name.
 **/
static gchar const*
j_trace_file_operation_name(JTraceFileOperation op)
{
	switch (op)
	{
		case J_TRACE_FILE_CLOSE:
			return "close";
		case J_TRACE_FILE_CREATE:
			return "create";
		case J_TRACE_FILE_DELETE:
			return "delete";
		case J_TRACE_FILE_OPEN:
			return "open";
		case J_TRACE_FILE_READ:
			return "read";
		case J_TRACE_FILE_SEEK:
			return "seek";
		case J_TRACE_FILE_STATUS:
			return "status";
		case J_TRACE_FILE_SYNC:
			return "sync";
		case J_TRACE_FILE_WRITE:
			return "write";
		default:
			g_warn_if_reached();
			return NULL;
	}
}

/**
 * Checks whether a function should be traced.
 *
 * \private
 *
 * \param name A function name.
 *
 * \return TRUE if the function should be traced, FALSE otherwise.
 **/
static gboolean
j_trace_function_check(gchar const* name)
{
	if (j_trace_function_patterns != NULL)
	{
		guint i;

		for (i = 0; j_trace_function_patterns[i] != NULL; i++)
		{
#if GLIB_CHECK_VERSION(2, 70, 0)
			if (g_pattern_spec_match_string(j_trace_function_patterns[i], name))
#else
			if (g_pattern_match_string(j_trace_function_patterns[i], name))
#endif
			{
				return TRUE;
			}
		}

		return FALSE;
	}

	return TRUE;
}

void
j_trace_init(gchar const* name)
{
	gchar const* j_trace;
	gchar const* j_trace_function;
	g_auto(GStrv) trace_parts = NULL;
	guint trace_len;

	g_return_if_fail(name != NULL);
	g_return_if_fail(j_trace_flags == J_TRACE_OFF);

	if ((j_trace = g_getenv("JULEA_TRACE")) == NULL)
	{
		return;
	}

	trace_parts = g_strsplit(j_trace, ",", 0);
	trace_len = g_strv_length(trace_parts);

	for (guint i = 0; i < trace_len; i++)
	{
		if (g_strcmp0(trace_parts[i], "echo") == 0)
		{
			j_trace_flags |= J_TRACE_ECHO;
		}
		else if (g_strcmp0(trace_parts[i], "otf") == 0)
		{
			j_trace_flags |= J_TRACE_OTF;
		}
		else if (g_strcmp0(trace_parts[i], "summary") == 0)
		{
			j_trace_flags |= J_TRACE_SUMMARY;
		}
		else if (g_strcmp0(trace_parts[i], "access") == 0)
		{
			j_trace_flags |= J_TRACE_ACCESS;
		}
	}

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	if ((j_trace_function = g_getenv("JULEA_TRACE_FUNCTION")) != NULL)
	{
		g_auto(GStrv) p = NULL;
		guint l;

		p = g_strsplit(j_trace_function, ",", 0);
		l = g_strv_length(p);

		j_trace_function_patterns = g_new(GPatternSpec*, l + 1);

		for (guint i = 0; i < l; i++)
		{
			j_trace_function_patterns[i] = g_pattern_spec_new(p[i]);
		}

		j_trace_function_patterns[l] = NULL;
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		otf_manager = OTF_FileManager_open(8);
		g_assert(otf_manager != NULL);

		otf_writer = OTF_Writer_open(name, 0, otf_manager);
		g_assert(otf_writer != NULL);

		OTF_Writer_writeDefCreator(otf_writer, 0, "JTrace");
		OTF_Writer_writeDefTimerResolution(otf_writer, 0, G_USEC_PER_SEC);

		otf_function_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		otf_file_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		otf_counter_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	}
#endif

	if (j_trace_flags & J_TRACE_SUMMARY)
	{
		j_trace_summary_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
	}

	if (j_trace_flags & J_TRACE_ACCESS)
	{
		G_LOCK(j_trace_echo);
		j_trace_access_print_header();
		G_UNLOCK(j_trace_echo);
	}

	g_free(j_trace_name);
	j_trace_name = g_strdup(name);
}

void
j_trace_fini(void)
{
	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		g_hash_table_unref(otf_function_table);
		otf_function_table = NULL;

		g_hash_table_unref(otf_file_table);
		otf_file_table = NULL;

		g_hash_table_unref(otf_counter_table);
		otf_counter_table = NULL;

		OTF_Writer_close(otf_writer);
		otf_writer = NULL;

		OTF_FileManager_close(otf_manager);
		otf_manager = NULL;
	}
#endif

	if (j_trace_flags & J_TRACE_SUMMARY)
	{
		GHashTableIter iter;
		gchar* key;
		JTraceTime* value;

		g_hash_table_iter_init(&iter, j_trace_summary_table);

		g_printerr("# stack duration[s] count\n");

		while (g_hash_table_iter_next(&iter, (gpointer*)&key, (gpointer*)&value))
		{
			g_printerr("%s %f %d\n", key, value->time, value->count);
		}

		g_hash_table_unref(j_trace_summary_table);
	}

	j_trace_flags = J_TRACE_OFF;

	if (j_trace_function_patterns != NULL)
	{
		guint i;

		for (i = 0; j_trace_function_patterns[i] != NULL; i++)
		{
			g_pattern_spec_free(j_trace_function_patterns[i]);
		}
	}

	g_free(j_trace_function_patterns);
	g_free(j_trace_name);
}

static void
j_trace_access_print(const Access* row, guint64 duration)
{
	g_printerr("%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT ",%u,%s,%s,%s,%s,%s,%s,%s,%" G_GUINT64_FORMAT ",%u,%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT ",\"%s\"\n",
		   row->timestamp / G_USEC_PER_SEC,
		   row->timestamp % G_USEC_PER_SEC,
		   row->uid,
		   row->program_name,
		   row->backend,
		   row->type,
		   row->path,
		   row->namespace,
		   row->name,
		   row->operation,
		   row->size,
		   row->complexity,
		   duration / G_USEC_PER_SEC,
		   duration % G_USEC_PER_SEC,
		   row->bson == NULL ? "{}" : bson_as_json(row->bson, NULL));
}

static void
store_object_path(JTraceThread* trace_thread, const char* namespace, const char* path)
{
	if (trace_thread->access.object.namespace != namespace)
	{
		g_free(trace_thread->access.object.namespace);
		trace_thread->access.object.namespace = NULL;
		if (namespace)
		{
			trace_thread->access.object.namespace = strdup(namespace);
		}
	}

	if (trace_thread->access.object.path != path)
	{
		g_free(trace_thread->access.object.path);
		trace_thread->access.object.path = NULL;
		if (path)
		{
			trace_thread->access.object.path = strdup(path);
		}
	}
}

static void
store_kv_namespace(JTraceThread* trace_thread, const char* namespace, const char* name)
{
	if (trace_thread->access.kv.namespace != namespace)
	{
		g_free(trace_thread->access.kv.namespace);
		trace_thread->access.kv.namespace = NULL;
		if (namespace)
		{
			trace_thread->access.kv.namespace = strdup(namespace);
		}
	}
	if (trace_thread->access.kv.name != name)
	{
		g_free(trace_thread->access.kv.name);
		trace_thread->access.kv.name = NULL;
		if (name)
		{
			trace_thread->access.kv.name = strdup(name);
		}
	}
}

static void
store_db_namespace(JTraceThread* trace_thread, const char* namespace)
{
	if (trace_thread->access.db.namespace != namespace)
	{
		g_free(trace_thread->access.db.namespace);
		trace_thread->access.db.namespace = NULL;
		if (namespace)
		{
			trace_thread->access.db.namespace = strdup(namespace);
		}
	}
}

static guint32
_count_keys_recursive(bson_iter_t* itr)
{
	guint32 cnt = 0;
	do
	{
		cnt += 1;
		if (bson_iter_type(itr) == BSON_TYPE_ARRAY || bson_iter_type(itr) == BSON_TYPE_DOCUMENT)
		{
			bson_iter_t child;
			bson_iter_recurse(itr, &child);
			cnt += _count_keys_recursive(&child);
		}
	} while (bson_iter_next(itr));
	return cnt;
}

static guint32
count_keys_recursive(const bson_t* bson)
{
	bson_iter_t itr;
	g_return_val_if_fail(bson_iter_init(&itr, bson), 0);
	return _count_keys_recursive(&itr);
}

JTrace*
j_trace_enter(gchar const* name, gchar const* format, ...)
{
	JTraceThread* trace_thread;
	JTrace* trace;
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return NULL;
	}

	g_return_val_if_fail(name != NULL, NULL);

	trace_thread = j_trace_thread_get_default();

	if (!j_trace_function_check(name))
	{
		/// \todo also blacklist nested functions
		return NULL;
	}

	timestamp = g_get_real_time();

	trace = g_slice_new(JTrace);
	trace->name = g_strdup(name);
	trace->enter_time = timestamp;

	if (j_trace_flags & J_TRACE_ECHO)
	{
		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);

		if (format != NULL)
		{
			va_list args;
			g_autofree gchar* arguments = NULL;
			va_start(args, format);

			arguments = g_strdup_vprintf(format, args);
			g_printerr("ENTER %s (%s)\n", name, arguments);
			va_end(args);
		}
		else
		{
			g_printerr("ENTER %s\n", name);
		}

		G_UNLOCK(j_trace_echo);
	}

	if (j_trace_flags & J_TRACE_ACCESS)
	{
		if (strcmp(name, "ping") == 0)
		{
			va_list args;
			va_start(args, format);
			g_free(trace_thread->client.program_name);
			trace_thread->client.program_name = strdup(va_arg(args, const char*));
			trace_thread->client.uid = va_arg(args, guint32);
			va_end(args);
		}
		else if (strncmp(name, "backend_", 8) == 0)
		{
			int type = 0;
			if (trace_thread->access.db.type == NULL) /// \TODO more precise checking?
			{
				JConfiguration* config = j_configuration_new();

				trace_thread->access.db.type = strdup(j_configuration_get_backend(config, J_BACKEND_TYPE_DB));
				trace_thread->access.db.config_path = strdup(j_configuration_get_backend_path(config, J_BACKEND_TYPE_DB));

				trace_thread->access.kv.type = strdup(j_configuration_get_backend(config, J_BACKEND_TYPE_KV));
				trace_thread->access.kv.config_path = strdup(j_configuration_get_backend_path(config, J_BACKEND_TYPE_KV));

				trace_thread->access.object.type = strdup(j_configuration_get_backend(config, J_BACKEND_TYPE_OBJECT));
				trace_thread->access.object.config_path = strdup(j_configuration_get_backend_path(config, J_BACKEND_TYPE_OBJECT));

				j_configuration_unref(config);
			}
			name += 8;
			if (strncmp(name, "kv_", 3) == 0)
			{
				type = 1;
			}
			else if (strncmp(name, "db_", 3) == 0)
			{
				type = 2;
			}
			else if (strncmp(name, "object_", 7) == 0)
			{
				type = 3;
			}
			if (type != 0)
			{
				Access* row = &trace_thread->access.row;
				va_list args;
				memset(row, 0, sizeof(Access));
				row->uid = trace_thread->client.uid;
				row->program_name = trace_thread->client.program_name;
				row->timestamp = timestamp;
				trace_thread->access.inside = TRUE;

				if (type == 1)
				{
					name += 3;
					row->backend = "kv";
					row->operation = name;
					row->namespace = trace_thread->access.kv.namespace;
					row->type = trace_thread->access.kv.type;
					row->path = trace_thread->access.kv.config_path;
					row->name = trace_thread->access.kv.name;
					if (strcmp(name, "batch_start") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						va_end(args);
						store_kv_namespace(trace_thread, row->namespace, NULL);
					}
					else if (strcmp(name, "put") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_arg(args, void*);
						row->size = va_arg(args, guint32);
						va_end(args);
					}
					else if (strcmp(name, "delete") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_end(args);
					}
					else if (strcmp(name, "get") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_arg(args, void*);
						trace_thread->access.utility_ptr = va_arg(args, void*);
						va_end(args);
					}
					else if (strcmp(name, "get_all") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						va_end(args);
						store_kv_namespace(trace_thread, row->namespace, NULL);
					}
					else if (strcmp(name, "get_by_prefix") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						row->name = va_arg(args, const char*);
						va_end(args);
						store_kv_namespace(trace_thread, row->namespace, row->name);
					}

					// iterate, init, fini <- no further deatils
					// batch_execute <- handlen in leave
				}
				else if (type == 2)
				{
					name += 3;
					row->backend = "db";
					row->operation = name;
					row->type = trace_thread->access.db.type;
					row->path = trace_thread->access.db.config_path;
					row->namespace = trace_thread->access.db.namespace;
					if (strcmp(name, "batch_start") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						va_end(args);
						store_db_namespace(trace_thread, row->namespace);
					}
					else if (strcmp(name, "schema_create") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						row->size = va_arg(args, guint32);
						row->bson = va_arg(args, const bson_t*);
						va_end(args);
					}
					else if (strcmp(name, "schema_get") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_end(args);
					}
					else if (strcmp(name, "schema_delete") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_end(args);
					}
					else if (strcmp(name, "insert") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						row->size = va_arg(args, guint32);
						row->bson = va_arg(args, const bson_t*);
						/// \TODO complexity needed?
						va_end(args);
					}
					else if (strcmp(name, "update") == 0)
					{
						static bson_t bson;
						const bson_t* selector = NULL;
						bson_init(&bson);
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						row->size = va_arg(args, guint32);
						selector = va_arg(args, const bson_t*);
						bson_append_document(&bson, "selector", -1, selector);
						bson_append_document(&bson, "entry", -1, va_arg(args, const bson_t*));
						row->bson = &bson;
						row->complexity = count_keys_recursive(selector) - 2; // because the added two top level keys
						va_end(args);
					}
					else if (strcmp(name, "delete") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_arg(args, guint32);
						row->bson = va_arg(args, const bson_t*);
						row->complexity = count_keys_recursive(row->bson);
						va_end(args);
					}
					else if (strcmp(name, "query") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						row->name = va_arg(args, const char*);
						va_arg(args, guint32);
						row->bson = va_arg(args, const bson_t*);
						row->complexity = count_keys_recursive(row->bson);
						va_end(args);
					}
					// iterate, init, fini <- no further details
					// batch_execute <- handled in leave
				}
				else if (type == 3)
				{
					name += 7;
					row->backend = "object";
					row->operation = name;
					row->namespace = trace_thread->access.object.namespace;
					row->type = trace_thread->access.object.type;
					row->path = trace_thread->access.object.config_path;
					row->name = trace_thread->access.object.path;
					if (strcmp(name, "create") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						row->name = va_arg(args, const char*);
						va_end(args);
						store_object_path(trace_thread, row->namespace, row->name);
					}
					else if (strcmp(name, "open") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						row->name = va_arg(args, const char*);
						va_end(args);
						store_object_path(trace_thread, row->namespace, row->name);
					}
					else if (strcmp(name, "get_all") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						row->name = NULL;
						va_end(args);
						store_object_path(trace_thread, row->namespace, row->name);
					}
					else if (strcmp(name, "get_by_prefix") == 0)
					{
						va_start(args, format);
						row->namespace = va_arg(args, const char*);
						row->name = va_arg(args, const char*);
						va_end(args);
						store_object_path(trace_thread, row->namespace, row->name);
					}
					else if (strcmp(name, "read") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						va_arg(args, void*);
						row->size = va_arg(args, guint64);
						row->complexity = va_arg(args, guint64);
						va_end(args);
					}
					else if (strcmp(name, "write") == 0)
					{
						va_start(args, format);
						va_arg(args, void*);
						va_arg(args, void*);
						row->size = va_arg(args, guint64);
						row->complexity = va_arg(args, guint64);
						va_end(args);
					}
					// status, sync, iterate, fini, init <- no further data
					// close/delte handle at leave
				}
				else
				{
					/// \TODO handle unknown backends
				}
			}
		}
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		gpointer value;
		guint32 function_id;

		G_LOCK(j_trace_otf);

		if ((value = g_hash_table_lookup(otf_function_table, name)) == NULL)
		{
			function_id = g_atomic_int_add(&otf_function_id, 1);

			OTF_Writer_writeDefFunction(otf_writer, 0, function_id, name, 0, 0);
			g_hash_table_insert(otf_function_table, g_strdup(name), GUINT_TO_POINTER(function_id));
		}
		else
		{
			function_id = GPOINTER_TO_UINT(value);
		}

		G_UNLOCK(j_trace_otf);

		OTF_Writer_writeEnter(otf_writer, timestamp, function_id, trace_thread->otf.process_id, 0);
	}
#endif

	if (j_trace_flags & J_TRACE_SUMMARY)
	{
		JTraceStack current_stack;

		if (trace_thread->stack->len == 0)
		{
			current_stack.name = g_strdup(name);
		}
		else
		{
			JTraceStack* top_stack;

			top_stack = &g_array_index(trace_thread->stack, JTraceStack, trace_thread->stack->len - 1);
			current_stack.name = g_strdup_printf("%s/%s", top_stack->name, name);
		}

		current_stack.enter_time = timestamp;
		g_array_append_val(trace_thread->stack, current_stack);
	}

	trace_thread->function_depth++;

	return trace;
}

void
j_trace_leave(JTrace* trace)
{
	JTraceThread* trace_thread;
	guint64 timestamp;

	if (trace == NULL)
	{
		return;
	}

	if (j_trace_flags == J_TRACE_OFF)
	{
		goto end;
	}

	trace_thread = j_trace_thread_get_default();

	if (!j_trace_function_check(trace->name))
	{
		goto end;
	}

	/// \todo
	if (trace_thread->function_depth == 0)
	{
		goto end;
	}

	trace_thread->function_depth--;
	timestamp = g_get_real_time();

	if (j_trace_flags & J_TRACE_ECHO)
	{
		guint64 duration;

		duration = timestamp - trace->enter_time;

		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);
		g_printerr("LEAVE %s", trace->name);
		g_printerr(" [%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "s]\n", duration / G_USEC_PER_SEC, duration % G_USEC_PER_SEC);
		G_UNLOCK(j_trace_echo);
	}

	if (j_trace_flags & J_TRACE_ACCESS)
	{
		if (trace_thread->access.inside)
		{
			if (strncmp(trace->name, "backend_", 8) == 0)
			{
				if (strncmp(trace->name + 8, "db_", 3) == 0 || strncmp(trace->name + 8, "kv_", 3) == 0 || strncmp(trace->name + 8, "object_", 7) == 0)
				{
					guint64 duration;
					Access* row = &trace_thread->access.row;

					duration = timestamp - trace->enter_time;

					if (strcmp(trace->name, "backend_kv_get") == 0)
					{
						row->size = *(const guint64*)trace_thread->access.utility_ptr;
					}

					G_LOCK(j_trace_echo);
					j_trace_access_print(row, duration);
					G_UNLOCK(j_trace_echo);

					trace_thread->access.inside = FALSE;
					if (strcmp(trace->name, "backend_kv_batch_execute") == 0)
					{
						store_kv_namespace(trace_thread, NULL, NULL);
					}
					else if (strcmp(trace->name, "backend_dbupdate") == 0)
					{
						bson_destroy((bson_t*)row->bson);
					}
					else if (strcmp(trace->name, "backend_db_batch_execute") == 0)
					{
						store_db_namespace(trace_thread, NULL);
					}
					else if (strcmp(trace->name, "backend_object_delete") == 0 || strcmp(trace->name, "backend_object_close") == 0)
					{
						store_object_path(trace_thread, NULL, NULL);
					}
				}
			}
		}
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		gpointer value;
		guint32 function_id;

		G_LOCK(j_trace_otf);

		value = g_hash_table_lookup(otf_function_table, trace->name);
		g_assert(value != NULL);
		function_id = GPOINTER_TO_UINT(value);

		G_UNLOCK(j_trace_otf);

		OTF_Writer_writeLeave(otf_writer, timestamp, function_id, trace_thread->otf.process_id, 0);
	}
#endif

	if (j_trace_flags & J_TRACE_SUMMARY)
	{
		JTraceTime* combined_duration;
		JTraceStack* top_stack;
		guint64 duration;

		g_assert(trace_thread->stack->len > 0);

		top_stack = &g_array_index(trace_thread->stack, JTraceStack, trace_thread->stack->len - 1);
		duration = timestamp - top_stack->enter_time;

		G_LOCK(j_trace_summary);

		combined_duration = g_hash_table_lookup(j_trace_summary_table, top_stack->name);

		if (combined_duration == NULL)
		{
			combined_duration = g_new(JTraceTime, 1);
			combined_duration->time = ((gdouble)duration) / ((gdouble)G_USEC_PER_SEC);
			combined_duration->count = 1;

			g_hash_table_insert(j_trace_summary_table, g_strdup(top_stack->name), combined_duration);
		}
		else
		{
			combined_duration->time += ((gdouble)duration) / ((gdouble)G_USEC_PER_SEC);
			combined_duration->count++;
		}

		G_UNLOCK(j_trace_summary);

		g_free(top_stack->name);
		g_array_set_size(trace_thread->stack, trace_thread->stack->len - 1);
	}

end:
	g_free(trace->name);
	g_slice_free(JTrace, trace);
}

void
j_trace_file_begin(gchar const* path, JTraceFileOperation op)
{
	JTraceThread* trace_thread;
	guint64 timestamp;

	g_return_if_fail(path != NULL);

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	trace_thread = j_trace_thread_get_default();
	timestamp = g_get_real_time();

	if (j_trace_flags & J_TRACE_ECHO)
	{
		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);
		g_printerr("BEGIN %s %s\n", j_trace_file_operation_name(op), path);
		G_UNLOCK(j_trace_echo);
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		gpointer value;
		guint32 file_id;

		G_LOCK(j_trace_otf);

		if ((value = g_hash_table_lookup(otf_file_table, path)) == NULL)
		{
			file_id = g_atomic_int_add(&otf_file_id, 1);

			OTF_Writer_writeDefFile(otf_writer, 0, file_id, path, 0);
			g_hash_table_insert(otf_file_table, g_strdup(path), GUINT_TO_POINTER(file_id));
		}
		else
		{
			file_id = GPOINTER_TO_UINT(value);
		}

		G_UNLOCK(j_trace_otf);

		OTF_Writer_writeBeginFileOperation(otf_writer, timestamp, trace_thread->otf.process_id, 1, 0);
	}
#endif

	return;
}

void
j_trace_file_end(gchar const* path, JTraceFileOperation op, guint64 length, guint64 offset)
{
	JTraceThread* trace_thread;
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	trace_thread = j_trace_thread_get_default();
	timestamp = g_get_real_time();

	if (j_trace_flags & J_TRACE_ECHO)
	{
		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);
		g_printerr("END %s %s", j_trace_file_operation_name(op), path);

		if (op == J_TRACE_FILE_READ || op == J_TRACE_FILE_WRITE)
		{
			g_printerr(" (length=%" G_GUINT64_FORMAT ", offset=%" G_GUINT64_FORMAT ")", length, offset);
		}

		g_printerr("\n");
		G_UNLOCK(j_trace_echo);
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		gpointer value;
		guint32 file_id;
		guint32 otf_op;

		switch (op)
		{
			case J_TRACE_FILE_CLOSE:
				otf_op = OTF_FILEOP_CLOSE;
				break;
			case J_TRACE_FILE_CREATE:
				otf_op = OTF_FILEOP_OTHER;
				break;
			case J_TRACE_FILE_DELETE:
				otf_op = OTF_FILEOP_UNLINK;
				break;
			case J_TRACE_FILE_OPEN:
				otf_op = OTF_FILEOP_OPEN;
				break;
			case J_TRACE_FILE_READ:
				otf_op = OTF_FILEOP_READ;
				break;
			case J_TRACE_FILE_SEEK:
				otf_op = OTF_FILEOP_SEEK;
				break;
			case J_TRACE_FILE_STATUS:
				otf_op = OTF_FILEOP_OTHER;
				break;
			case J_TRACE_FILE_SYNC:
				otf_op = OTF_FILEOP_SYNC;
				break;
			case J_TRACE_FILE_WRITE:
				otf_op = OTF_FILEOP_WRITE;
				break;
			default:
				otf_op = OTF_FILEOP_OTHER;
				g_warn_if_reached();
				break;
		}

		G_LOCK(j_trace_otf);

		value = g_hash_table_lookup(otf_file_table, path);
		g_assert(value != NULL);
		file_id = GPOINTER_TO_UINT(value);

		G_UNLOCK(j_trace_otf);

		OTF_Writer_writeEndFileOperation(otf_writer, timestamp, trace_thread->otf.process_id, file_id, 1, 0, otf_op, length, 0);
	}
#endif
}

void
j_trace_counter(gchar const* name, guint64 counter_value)
{
	JTraceThread* trace_thread;
	guint64 timestamp;

	if (j_trace_flags == J_TRACE_OFF)
	{
		return;
	}

	g_return_if_fail(name != NULL);

	trace_thread = j_trace_thread_get_default();
	timestamp = g_get_real_time();

	if (j_trace_flags & J_TRACE_ECHO)
	{
		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);
		g_printerr("COUNTER %s %" G_GUINT64_FORMAT "\n", name, counter_value);
		G_UNLOCK(j_trace_echo);
	}

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		gpointer value;
		guint32 counter_id;

		G_LOCK(j_trace_otf);

		if ((value = g_hash_table_lookup(otf_counter_table, name)) == NULL)
		{
			counter_id = g_atomic_int_add(&otf_counter_id, 1);

			OTF_Writer_writeDefCounter(otf_writer, 0, counter_id, name, OTF_COUNTER_TYPE_ACC + OTF_COUNTER_SCOPE_START, 0, NULL);
			g_hash_table_insert(otf_counter_table, g_strdup(name), GUINT_TO_POINTER(counter_id));
		}
		else
		{
			counter_id = GPOINTER_TO_UINT(value);
		}

		G_UNLOCK(j_trace_otf);

		OTF_Writer_writeCounter(otf_writer, timestamp, trace_thread->otf.process_id, counter_id, counter_value);
	}
#endif
}

/**
 * @}
 **/
