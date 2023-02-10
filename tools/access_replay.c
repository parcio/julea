/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2023-2023 Julian Benda
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
#include <locale.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <julea.h>

JBackend* object_backend = NULL;
JBackend* kv_backend = NULL;
JBackend* db_backend = NULL;

static gboolean
setup_backend(JConfiguration* configuration, JBackendType type, gchar const* port_str, GModule** module, JBackend** j_backend)
{
  gchar const* backend;
  gchar const* component;
  g_autofree gchar* path;

  backend = j_configuration_get_backend(configuration, type);
  component = j_configuration_get_backend_component(configuration, type);
  path = j_helper_str_replace(j_configuration_get_backend_path(configuration, type), "{PORT}", port_str);

  if(j_backend_load_server(backend, component, type, module, j_backend)) {
    gboolean res = TRUE;
    if(j_backend == NULL) {
      g_warning("Failed to load server: %s.", backend);
      return FALSE;
    }
    switch (type) {
      case J_BACKEND_TYPE_OBJECT:  res = j_backend_object_init(*j_backend, path); break;
      case J_BACKEND_TYPE_KV: res = j_backend_kv_init(*j_backend, path); break;
      case J_BACKEND_TYPE_DB: res = j_backend_db_init(*j_backend, path); break;
    }
    if (!res) {
      g_warning("Failed to initelize backend: %s", backend);
      return FALSE;
    }
    return TRUE;
  }
  return FALSE;
}

static gboolean
split(char* line, const char* parts[9]) {
  int cnt = 0;
  const char* section;
  section = strtok(line, ",");
  while(section != NULL) {
    if (cnt == 9) {
      g_warning("to many parts in line");
      return FALSE;
    }
    parts[cnt++] = section;
    section = strtok(NULL, ",");
  }
  if (cnt != 9) {
    g_warning("to few parts in line");
    return FALSE;
  }
  return TRUE;
}
#define BACKEND 3
#define NAMESPACE 4
#define NAME 5
#define OPERATION 6
#define SIZE 7


static gboolean
replay_object(JMemoryChunk* memory_chunk, guint64 memory_chunk_size, char* parts[9]){
  static gpointer data = NULL;
  const char* op = parts[OPERATION];
  gboolean ret = FALSE;
  (void) memory_chunk_size; /// \TODO usage?
  if(strcmp(op, "create") == 0) {
    if (data != NULL) { g_warning("open new object while still one open"); }
    ret = j_backend_object_create(object_backend, parts[NAMESPACE], parts[NAME], &data);
  } else if (strcmp(op, "open") == 0) {
    if (data != NULL) { g_warning("open new object while still one open"); }
    ret = j_backend_object_open(object_backend, parts[NAMESPACE], parts[NAME], &data);
  } else if (strcmp(op, "get_all") == 0) {
    if (data != NULL) { g_warning("open new object while still one open"); }
    ret = j_backend_object_get_all(object_backend, parts[NAMESPACE], data);
  } else if (strcmp(op, "ge_by_prefix") == 0) {
    if (data != NULL) { g_warning("open new object while still one open"); }
    ret = j_backend_kv_get_by_prefix(object_backend, parts[NAMESPACE], parts[NAME], &data);
  } else if (strcmp(op, "read") == 0) {
    static guint64 bytes_read = 0;
    guint64 size = strtoul(parts[SIZE], NULL, 10);
    if (size > memory_chunk_size) {
      g_warning("unable to replay: chunk size to samll!%lu vs %lu", size, memory_chunk_size);
    } else {
      ret = j_backend_object_read(object_backend, data, memory_chunk, size, 0, &bytes_read);
    }
  } else if (strcmp(op, "write") == 0) {
    static guint64 bytes_written = 0;
    guint64 size = strtoul(parts[SIZE], NULL, 10);
    if (size > memory_chunk_size) {
      g_warning("unable to replay: chunk size to samll!%lu vs %lu", size, memory_chunk_size);
    } else {
    ret = j_backend_object_write(object_backend, data, memory_chunk, size, 0, &bytes_written);
      }
  } else if (strcmp(op, "status") == 0) {
    static gint64 modification_time;
    static guint64 size;
    ret = j_backend_object_status(object_backend, data, &modification_time, &size);
  } else if (strcmp(op, "sync") == 0) {
    ret = j_backend_object_sync(object_backend, data);
  } else if (strcmp(op, "iterate") == 0) {
    static const char* name = NULL;
    ret = j_backend_object_iterate(object_backend, data, &name);
    if (!ret) { data = NULL; ret = TRUE; }
  } else if (strcmp(op, "close") == 0) {
    ret = j_backend_object_close(object_backend, data);
    data = NULL;
  } else if (strcmp(op, "delete") == 0) {
    ret = j_backend_object_delete(object_backend, data);
    data = NULL;
  } else if (strcmp(op, "init") == 0) {
    ret = TRUE;
  } else if (strcmp(op, "fini") == 0) {
    ret = TRUE;
  } else {
    g_warning("unkown object operation: '%s'", op);
  }
  return ret;
}
static gboolean
replay_kv(JMemoryChunk* memory_chunk, guint64 memory_chunk_size, char* parts[9]){
  gboolean ret = FALSE;
  const char* op = parts[OPERATION];

  if (strcmp(op, "batch_start") == 0) {
  } else if (strcmp(op, "put") == 0) {
  } else if (strcmp(op, "delete") == 0) {
  } else if (strcmp(op, "get") == 0) {
  } else if (strcmp(op, "get_all") == 0) {
  } else if (strcmp(op, "get_by_prefix") == 0) {
  } else if (strcmp(op, "iterate") == 0) {
  } else if (strcmp(op, "init") == 0) {
  } else if (strcmp(op, "fini") == 0) {
  } else if (strcmp(op, "batch_execute") == 0) {
  } else {
    g_warning("unkown operation: '%s'", op);
  }
  return ret;
}
static gboolean
replay_db(JMemoryChunk* memory_chunk, guint64 memory_chunk_size, char* parts[9]){
  gboolean ret = FALSE;
  static gpointer batch;
  /// \TODO how to determine semantics used?
  static JSemantics* semantics = NULL; 
  GError* error;
  const char* op = parts[OPERATION];
  if (semantics == NULL) { semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT); }

  if (strcmp(op, "batch_start") == 0) {
    if (batch != NULL) { g_warning("starting a new batch, but old one is not finished"); }
    ret = j_backend_db_batch_start(db_backend, parts[NAMESPACE], semantics, &batch, &error);
  } else if (strcmp(op, "schema_create") == 0) {
    bson_t* schema;
    ret = j_backend_db_schema_create(db_backend, batch, parts[NAME], schema, &error);
  } else if (strcmp(op, "schema_get") == 0) {
  } else if (strcmp(op, "schema_delete") == 0) {
  } else if (strcmp(op, "insert") == 0) {
  } else if (strcmp(op, "delete") == 0) {
  } else if (strcmp(op, "update") == 0) {
  } else if (strcmp(op, "delete") == 0) {
  } else if (strcmp(op, "query") == 0) {
  } else if (strcmp(op, "iterate") == 0) {
    static bson_t* entry;
    ret = j_backend_db_iterate(db_backend, batch, entry, &error);
    if(!ret) { batch = NULL; ret = TRUE; }
  } else if (strcmp(op, "init") == 0) {
    ret = TRUE;
  } else if (strcmp(op, "fini") == 0) {
    ret = TRUE;
  } else if (strcmp(op, "batch_execute") == 0) {
    j_backend_db_batch_execute(db_backend, batch, &error);
    batch = NULL;
  } else {
    g_warning("unknown operation: '%s'", op);
  }
  return ret && error == NULL;
}

static gboolean
replay(JMemoryChunk* memory_chunk, guint64 memory_chunck_size, char* line)
{
  const char* parts[9];
  if (!split(line, parts)) { return FALSE; }

  if(strcmp(parts[3], " object") == 0) { if(!replay_object(memory_chunk, memory_chunck_size, parts)) return FALSE; }
  if(strcmp(parts[3], " kv") == 0) { if(!replay_kv(memory_chunk, memory_chunck_size, parts)) return FALSE; }
  if(strcmp(parts[3], " db") == 0) { if(!replay_db(memory_chunk, memory_chunck_size, parts)) return FALSE; }
  
  return TRUE;
}

int
main(int argc, char** argv)
{
  JConfiguration* configuration = NULL;
  const char* record_file_name = NULL;
  FILE* record_file = NULL;
  JTrace* trace = NULL;
  GModule* db_module = NULL;
  GModule* kv_module = NULL;
  GModule* object_module = NULL;
  g_autofree gchar* port_str = NULL;
  gboolean res = FALSE;
  JMemoryChunk* memory_chunk = NULL;
  guint64 memory_chunck_size = 0;

  setlocal(LC_ALL, "C.UTF-8");

  configuration = j_configuration();
  if (configuration == NULL) {
    g_warning("unable to read config");
    goto end;
    if (argc < 2) {
      g_warning("useage: %s <access_dump.csv>", argv[0]);
      goto end;
    }
  }
  record_file_name = argv[1];
    if (access(record_file_name, R_OK) != 0) {
    g_warning("file does not exists, or no read permission '%s'", record_file_name);
    goto end;
  }
  record_file = fopen(record_file_name, "r");
  if (record_file == NULL) {
    g_warning("failed to open file '%s'", record_file_name);
    goto end;
  }

  j_trace_init("julea-replay");
  trace = j_trace_enter(G_STRFUNC, NULL);

  port_str = g_strdup_printf("%d", j_configuration_get_port(configuration));
  if(!setup_backend(configuration, J_BACKEND_TYPE_OBJECT, port_str, &object_module, &object_backend)) { goto end; }
  if(!setup_backend(configuration, J_BACKEND_TYPE_KV, port_str, &kv_module, &kv_backend)) { goto end; }
  if(!setup_backend(configuration, J_BACKEND_TYPE_DB, port_str, &db_module, &db_backend)) { goto end; }

  memory_chunck_size = j_configuration_get_max_operation_size(configuration);
  memory_chunk = j_memory_chunk_new(memory_chunck_size);

  {
    char* line = NULL;
    size_t len = 0;
    ssize_t read = 0;
    read = getline(&line, &len, record_file);
    if (read == -1 || strcmp(line, "time, process_uid, program_name, backend, namespace, name, operation, size, complexity, duration") != 0) {
      g_warning("Invalid header in dump!");
      goto end;
    }
    while ((read = getline(&line, &len, record_file)) != -1) {
      if (!replay(memory_chunk, memory_chunck_size, line)) {
        g_warning("failed to replay dump!");
        goto end;
      }
    }
  }
  res = TRUE;
end:
  if (record_file) {
    fclose(record_file);
  }
  if (db_backend != NULL)
  {
    j_backend_db_fini(db_backend);
  }
  if (kv_backend != NULL)
  {
    j_backend_kv_fini(kv_backend);
  }
  if(object_backend != NULL)
  {
    j_backend_db_fini(db_backend);
  }
  if(db_module != NULL) { g_module_close(db_module); }
  if(kv_module != NULL) { g_module_close(kv_module); }
  if(object_module != NULL) { g_module_close(object_module); }
  j_configuration_unref(configuration);
  j_tarce_leave(trace);
  j_trace_fini();
  return res;
}