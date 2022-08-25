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
#include <glib/gprintf.h>

#ifdef HAVE_OTF
#include <otf.h>
#endif

#ifdef HAVE_OTF2
#include <otf2/otf2.h>
#endif

#include <jtrace.h>

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
	J_TRACE_OTF2 = 1 << 3
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

#ifdef HAVE_OTF2
	/**
	 * OTF2-specific structure.
	 **/
	struct
	{
		/**
		 * Thread's process ID.
		 **/
		guint32 process_id;
	} otf2;
#endif
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

#ifdef HAVE_OTF2
// Déclaration des fonctions pour créer une archive
get_time( void )
{
    static uint64_t sequence;
    return sequence++;
}

static OTF2_FlushType
pre_flush( void*            userData,
           OTF2_FileType    fileType,
           OTF2_LocationRef location,
           void*            callerData,
           bool             final )
{
    return OTF2_FLUSH;
}

static OTF2_TimeStamp
post_flush( void*            userData,
            OTF2_FileType    fileType,
            OTF2_LocationRef location )
{
    return get_time();
}

static OTF2_FlushCallbacks flush_callbacks =
{
    .otf2_pre_flush  = pre_flush,
    .otf2_post_flush = post_flush
};
static OTF2_Archive* otf2_archive = NULL;
static OTF2_EvtWriter* otf2_evtwriter = NULL; 
static OTF2_DefWriter* otf2_defwriter = NULL;
static OTF2_GlobalDefWriter* otf2_globaldefwriter = NULL ;
static guint32 otf2_comm = 0;

static guint32 otf2_file_id = 0;
static guint32 otf2_region_id = 1;
static guint32 otf2_location_id = 0;
static guint32 otf2_string_id = 0;
static guint32 otf2_treenode_id = 1;
static guint32 otf2_locationgroup_id = 0;
static guint32 otf2_attribute_id = 0;
static guint32 otf2_comm_id = 0;
static guint otf2_ioparadigm_id = 0;
static guint otf2_iohandle_id = 1;
static guint otf2_group_id = 0;



static GHashTable* otf2_region_table = NULL;
static GHashTable* otf2_location_table = NULL ;
static GHashTable* otf2_string_table = NULL ;
static GHashTable* otf2_handle_table = NULL;

G_LOCK_DEFINE_STATIC(j_trace_otf2);
#endif

static void j_trace_thread_default_free(gpointer);

static GPrivate j_trace_thread_default = G_PRIVATE_INIT(j_trace_thread_default_free);
static GHashTable* j_trace_summary_table = NULL;

G_LOCK_DEFINE_STATIC(j_trace_echo);
G_LOCK_DEFINE_STATIC(j_trace_summary);

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
	trace_thread->function_depth = 0;
	trace_thread->stack = g_array_new(FALSE, FALSE, sizeof(JTraceStack));

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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		trace_thread->otf2.process_id= otf2_location_id;
		OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, trace_thread->thread_name);
		OTF2_GlobalDefWriter_WriteLocation(otf2_globaldefwriter, otf2_location_id, otf2_string_id, OTF2_LOCATION_GROUP_TYPE_PROCESS, 100, 0);
		g_atomic_int_add(&otf2_string_id, 1);
		if (otf2_comm==0)
			{
				OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, "Unique Communicator");
				guint32 comm_name=otf2_string_id;
				g_atomic_int_add(&otf2_string_id, 1);
                OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, "Unique Group for the Communicator");
				OTF2_GlobalDefWriter_WriteGroup(otf2_globaldefwriter, otf2_group_id, otf2_string_id, OTF2_GROUP_TYPE_UNKNOWN, OTF2_PARADIGM_USER, OTF2_GROUP_FLAG_NONE, OTF2_PARADIGM_UNKNOWN, 1);
				OTF2_GlobalDefWriter_WriteComm(otf2_globaldefwriter, otf2_comm_id, comm_name, otf2_group_id, OTF2_UNDEFINED_COMM, OTF2_COMM_FLAG_NONE);
				g_atomic_int_add(&otf2_string_id, 1);
                g_atomic_int_add(&otf2_group_id, 1);
				g_atomic_int_add(&otf2_comm_id, 1);
				g_atomic_int_add(&otf2_comm, 1);
			}
		OTF2_Archive_OpenDefFiles(otf2_archive);
		otf2_defwriter=OTF2_Archive_GetDefWriter(otf2_archive, trace_thread->otf2.process_id);
		OTF2_DefWriter_WriteString(otf2_defwriter, otf2_string_id, "First string of this process");
		OTF2_Archive_CloseDefWriter(otf2_archive, otf2_defwriter);
		OTF2_Archive_CloseDefFiles(otf2_archive);
		OTF2_Archive_OpenEvtFiles(otf2_archive);
		otf2_evtwriter=OTF2_Archive_GetEvtWriter(otf2_archive, trace_thread->otf2.process_id);
        OTF2_EvtWriter_ThreadCreate(otf2_evtwriter, NULL, g_get_real_time(), otf2_comm, 0);
		OTF2_Archive_CloseEvtWriter(otf2_archive, otf2_evtwriter);
		OTF2_Archive_CloseEvtFiles(otf2_archive);
		g_atomic_int_add(&otf2_string_id, 1);
		g_atomic_int_add(&otf2_location_id,1);
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

#ifdef HAVE_OTF
	if (j_trace_flags & J_TRACE_OTF)
	{
		OTF_Writer_writeEndProcess(otf_writer, g_get_real_time(), trace_thread->otf.process_id);
	}
#endif

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		OTF2_Archive_OpenEvtFiles(otf2_archive);
		otf2_evtwriter=OTF2_Archive_GetEvtWriter(otf2_archive, trace_thread->otf2.process_id);
        OTF2_EvtWriter_ThreadEnd(otf2_evtwriter, NULL, g_get_real_time(), otf2_comm, 0);
		OTF2_Archive_CloseEvtFiles(otf2_archive);
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
		else if (g_strcmp0(trace_parts[i], "otf2") == 0)
		{
			j_trace_flags |= J_TRACE_OTF2;
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
#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		g_assert(otf2_archive==NULL);
		otf2_archive = OTF2_Archive_Open("/home/urz/perrin/julea/example", name, OTF2_FILEMODE_WRITE, 1024 * 1024, 4*  1024 * 1024, OTF2_SUBSTRATE_POSIX, OTF2_COMPRESSION_NONE);
		OTF2_Archive_SetFlushCallbacks(otf2_archive, &flush_callbacks, NULL);
		OTF2_Archive_SetSerialCollectiveCallbacks(otf2_archive);
		g_assert(otf2_archive!=NULL);
		otf2_globaldefwriter = OTF2_Archive_GetGlobalDefWriter(otf2_archive);
		g_assert(otf2_globaldefwriter!=NULL);
		OTF2_GlobalDefWriter_WriteClockProperties(otf2_globaldefwriter, 1, 0, 2, OTF2_UNDEFINED_TIMESTAMP);
		OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, "void string by default");
		g_atomic_int_add(&otf2_string_id,1);
		OTF2_GlobalDefWriter_WriteRegion(otf2_globaldefwriter, 0, 0, 0, 0, OTF2_REGION_ROLE_FUNCTION, OTF2_PARADIGM_USER, OTF2_REGION_FLAG_NONE, 0, 0, 0);
		otf2_handle_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		otf2_region_table=g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		otf2_string_table=g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
		OTF2_GlobalDefWriter_WriteSystemTreeNode(otf2_globaldefwriter, 0, 0, 0, OTF2_UNDEFINED_SYSTEM_TREE_NODE);
		g_atomic_int_add(&otf2_treenode_id,1);
		OTF2_GlobalDefWriter_WriteLocationGroup(otf2_globaldefwriter, 0, 0, OTF2_LOCATION_GROUP_TYPE_PROCESS, 0, OTF2_UNDEFINED_LOCATION_GROUP);
		g_atomic_int_add(&otf2_locationgroup_id,1);
	}
#endif

	if (j_trace_flags & J_TRACE_SUMMARY)
	{
		j_trace_summary_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		OTF2_Archive_CloseGlobalDefWriter(otf2_archive, otf2_globaldefwriter);
		OTF2_Archive_Close(otf2_archive);

		otf2_archive = NULL;
		otf2_globaldefwriter = NULL;

		g_hash_table_unref(otf2_handle_table);
		otf2_handle_table = NULL;

		g_hash_table_unref(otf2_region_table);	
		otf2_region_table = NULL;

		g_hash_table_unref(otf2_location_table);	
		otf2_location_table = NULL;
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

JTrace*
j_trace_enter(gchar const* name, gchar const* format, ...)
{
	JTraceThread* trace_thread;
	JTrace* trace;
	guint64 timestamp;
	va_list args;

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

	va_start(args, format);

	if (j_trace_flags & J_TRACE_ECHO)
	{
		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);

		if (format != NULL)
		{
			g_autofree gchar* arguments = NULL;

			arguments = g_strdup_vprintf(format, args);
			g_printerr("ENTER %s (%s)\n", name, arguments);
		}
		else
		{
			g_printerr("ENTER %s\n", name);
		}

		G_UNLOCK(j_trace_echo);
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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		gpointer value;
		guint32 region_id;

		G_LOCK(j_trace_otf2);

		if ((value = g_hash_table_lookup(otf2_region_table, name)) == NULL)
		{
			OTF2_Archive_OpenDefFiles(otf2_archive);
			OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, name); 
			g_hash_table_insert(otf2_string_table, g_strdup(name), GUINT_TO_POINTER(otf2_string_id));
			OTF2_GlobalDefWriter_WriteRegion(otf2_globaldefwriter, otf2_region_id, otf2_string_id, otf2_string_id, 0, OTF2_REGION_ROLE_FUNCTION, OTF2_PARADIGM_USER, OTF2_REGION_FLAG_NONE, 0, 0, 0);
			g_hash_table_insert(otf2_region_table, g_strdup(name), GUINT_TO_POINTER(otf2_region_id));
			OTF2_Archive_CloseDefFiles(otf2_archive);
			region_id=otf2_region_id;
			g_atomic_int_add(&otf2_region_id,1);
			g_atomic_int_add(&otf2_string_id, 1);
		}
		else
		{
			region_id = GPOINTER_TO_UINT(value);
		}

		G_UNLOCK(j_trace_otf2);

		OTF2_Archive_OpenEvtFiles( otf2_archive );
		otf2_evtwriter=OTF2_Archive_GetEvtWriter(otf2_archive, trace_thread->otf2.process_id);
		g_assert(otf2_evtwriter!=NULL);
		OTF2_EvtWriter_Enter(otf2_evtwriter, NULL, timestamp, region_id);
		OTF2_Archive_CloseEvtFiles(otf2_archive);
		OTF2_Archive_CloseEvtWriter(otf2_archive, otf2_evtwriter);
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

	va_end(args);

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
	{/*
		guint64 duration;

		duration = timestamp - trace->enter_time;

		G_LOCK(j_trace_echo);
		j_trace_echo_printerr(trace_thread, timestamp);
		g_printerr("LEAVE %s", trace->name);
		g_printerr(" [%" G_GUINT64_FORMAT ".%06" G_GUINT64_FORMAT "s]\n", duration / G_USEC_PER_SEC, duration % G_USEC_PER_SEC);
		G_UNLOCK(j_trace_echo);*/
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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		gpointer value;
		guint32 ref_id;
		G_LOCK(j_trace_otf2);
		value=g_hash_table_lookup(otf2_region_table, trace->name);
		g_assert(value!=NULL);
		ref_id=GPOINTER_TO_UINT(value);
		G_UNLOCK(j_trace_otf2);
		OTF2_Archive_OpenEvtFiles(otf2_archive);
		otf2_evtwriter=OTF2_Archive_GetEvtWriter(otf2_archive, trace_thread->otf2.process_id);
		g_assert(otf2_evtwriter!=NULL);
		OTF2_EvtWriter_Leave(otf2_evtwriter,NULL, timestamp, ref_id);
		OTF2_Archive_CloseEvtFiles(otf2_archive);
		OTF2_Archive_CloseEvtWriter(otf2_archive, otf2_evtwriter);
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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		gpointer value;
		guint32 handle_id;
		guint32 ioparadigm_id;
		guint32 iofile_id;

		G_LOCK(j_trace_otf2);

		if ((value = g_hash_table_lookup(otf2_handle_table, path)) == NULL)
		{
			if (otf2_comm==0)
			{
				OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, "Unique Communicator");
				guint32 comm_name=otf2_string_id;
				g_atomic_int_add(&otf2_string_id, 1);
                OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, "Unique Group for the Communicator");
				OTF2_GlobalDefWriter_WriteGroup(otf2_globaldefwriter, otf2_group_id, otf2_string_id, OTF2_GROUP_TYPE_UNKNOWN, OTF2_PARADIGM_USER, OTF2_GROUP_FLAG_NONE, OTF2_PARADIGM_UNKNOWN, 1);
				OTF2_GlobalDefWriter_WriteComm(otf2_globaldefwriter, otf2_comm_id, comm_name, otf2_group_id, OTF2_UNDEFINED_COMM, OTF2_COMM_FLAG_NONE);
				g_atomic_int_add(&otf2_string_id, 1);
                g_atomic_int_add(&otf2_group_id, 1);
				g_atomic_int_add(&otf2_comm_id, 1);
				g_atomic_int_add(&otf2_comm, 1);
			}
			OTF2_GlobalDefWriter_WriteString(otf2_globaldefwriter, otf2_string_id, path);
			OTF2_GlobalDefWriter_WriteIoRegularFile(otf2_globaldefwriter, otf2_file_id, otf2_string_id, 0);
			OTF2_GlobalDefWriter_WriteAttribute(otf2_globaldefwriter, otf2_attribute_id, otf2_string_id, 0, OTF2_TYPE_NONE);
			//OTF2_AttributeValue attributeValue = { .attributeRef = otf2_attribute_id};
			OTF2_GlobalDefWriter_WriteIoParadigm(otf2_globaldefwriter, otf2_ioparadigm_id, otf2_string_id, otf2_string_id, OTF2_IO_PARADIGM_CLASS_SERIAL, OTF2_IO_PARADIGM_FLAG_NONE, 0, NULL, OTF2_TYPE_NONE, otf2_attribute_id);
			OTF2_GlobalDefWriter_WriteIoHandle(otf2_globaldefwriter, otf2_iohandle_id, otf2_string_id, otf2_file_id, otf2_ioparadigm_id, OTF2_IO_HANDLE_FLAG_NONE, 0, 0);
			handle_id=otf2_iohandle_id;
			ioparadigm_id=ioparadigm_id;
			iofile_id=iofile_id;
			g_hash_table_insert(otf2_handle_table, g_strdup(path), GUINT_TO_POINTER(otf2_iohandle_id));
			g_atomic_int_add(&otf2_string_id, 1);
			g_atomic_int_add(&otf2_file_id, 1);
			g_atomic_int_add(&otf2_attribute_id, 1);
			g_atomic_int_add(&otf2_ioparadigm_id, 1);
			g_atomic_int_add(&otf2_iohandle_id, 1);
		}
		else
		{
			handle_id = GPOINTER_TO_UINT(value);
		}

		G_UNLOCK(j_trace_otf2);

		OTF2_Archive_OpenEvtFiles(otf2_archive);
		otf2_evtwriter=OTF2_Archive_GetEvtWriter(otf2_archive, trace_thread->otf2.process_id);
		g_assert(otf2_evtwriter!=NULL);

		switch (op)
		{
			case J_TRACE_FILE_CLOSE:
				OTF2_EvtWriter_IoDestroyHandle(otf2_evtwriter, NULL, timestamp, handle_id);
				break;
			case J_TRACE_FILE_CREATE:
				OTF2_EvtWriter_IoCreateHandle(otf2_evtwriter, NULL, timestamp, handle_id, OTF2_IO_ACCESS_MODE_READ_WRITE, OTF2_IO_CREATION_FLAG_CREATE, OTF2_IO_STATUS_FLAG_NONE );
				break;
			case J_TRACE_FILE_DELETE:
				OTF2_EvtWriter_IoDeleteFile(otf2_evtwriter, NULL, timestamp, ioparadigm_id, iofile_id);
				break;
			case J_TRACE_FILE_OPEN:
				OTF2_EvtWriter_IoCreateHandle(otf2_evtwriter, NULL, timestamp, handle_id, OTF2_IO_ACCESS_MODE_READ_WRITE, OTF2_IO_CREATION_FLAG_NONE, OTF2_IO_STATUS_FLAG_NONE);		
				break;
			case J_TRACE_FILE_READ:
				OTF2_EvtWriter_IoOperationBegin( otf2_evtwriter, NULL, timestamp, handle_id, OTF2_IO_OPERATION_MODE_READ, OTF2_IO_OPERATION_FLAG_NONE, 0, 0);
				break;
			case J_TRACE_FILE_SEEK:
				OTF2_EvtWriter_IoSeek(otf2_evtwriter, NULL, timestamp, handle_id, 0, OTF2_IO_SEEK_FROM_START, 0);
				break;
			case J_TRACE_FILE_STATUS:
				//TODO
				break;
			case J_TRACE_FILE_SYNC:
				OTF2_EvtWriter_IoOperationBegin(otf2_evtwriter, NULL, timestamp, handle_id, OTF2_IO_OPERATION_MODE_FLUSH, OTF2_IO_OPERATION_FLAG_NONE, 0, 0);
				break;
			case J_TRACE_FILE_WRITE:
				OTF2_EvtWriter_IoOperationBegin(otf2_evtwriter, NULL, timestamp, handle_id, OTF2_IO_OPERATION_MODE_WRITE, OTF2_IO_OPERATION_FLAG_NONE, 0, 0);
				break;
			default:
				g_warn_if_reached();
				break;
		}
	OTF2_Archive_CloseEvtFiles(otf2_archive);
	OTF2_Archive_CloseEvtWriter(otf2_archive, otf2_evtwriter);
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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		gpointer value;
		guint32 iohandle_id;

		G_LOCK(j_trace_otf2);

		value = g_hash_table_lookup(otf2_handle_table, path);
		g_assert(value != NULL);
		iohandle_id = GPOINTER_TO_UINT(value);

		G_UNLOCK(j_trace_otf2);

		OTF2_Archive_OpenEvtFiles(otf2_archive);
		otf2_evtwriter = OTF2_Archive_GetEvtWriter(otf2_archive, trace_thread->otf2.process_id);
		OTF2_EvtWriter_IoOperationComplete(otf2_evtwriter, NULL, timestamp, iohandle_id, length, 0);
		OTF2_Archive_CloseEvtFiles(otf2_archive);
		OTF2_Archive_CloseEvtWriter(otf2_archive, otf2_evtwriter);
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

#ifdef HAVE_OTF2
	if (j_trace_flags & J_TRACE_OTF2)
	{
		//TODO compteur ?
	}
#endif
}

/**
 * @}
 **/
