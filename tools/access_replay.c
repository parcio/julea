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

	if (strcmp(component, "server") != 0)
	{
		return TRUE;
	}
	if (j_backend_load_server(backend, component, type, module, j_backend))
	{
		gboolean res = TRUE;
		if (j_backend == NULL)
		{
			g_warning("Failed to load server: %s.", backend);
			return FALSE;
		}
		switch (type)
		{
			case J_BACKEND_TYPE_OBJECT:
				res = j_backend_object_init(*j_backend, path);
				break;
			case J_BACKEND_TYPE_KV:
				res = j_backend_kv_init(*j_backend, path);
				break;
			case J_BACKEND_TYPE_DB:
				res = j_backend_db_init(*j_backend, path);
				break;
			default:
				g_warning("unknown backend type: (%d), unable to setup backend!", type);
				res = FALSE;
		}
		if (!res)
		{
			g_warning("Failed to initelize backend: %s", backend);
			return FALSE;
		}
		return TRUE;
	}
	return FALSE;
}

enum row_fields
{
	TIME,
	PROCESS_UID,
	PROGRAM_NAME,
	BACKEND,
	TYPE,
	PATH,
	NAMESPACE,
	NAME,
	OPERATION,
	SEMANTIC,
	SIZE,
	COMPLEXITY,
	DURATION,
	JSON,
	ROW_LEN
};

static gboolean
split(char* line, char* parts[ROW_LEN])
{
	int cnt = 0;
	char* section;
	char* save;
	section = strtok_r(line, ",", &save);
	while (section != NULL)
	{
		parts[cnt++] = section;
		if (cnt == ROW_LEN - 1)
		{
			break;
		}
		while (*save == ',')
		{
			*save = '\0';
			parts[cnt++] = save++;
		}
		section = strtok_r(NULL, ",", &save);
	}
	if (section == NULL)
	{
		g_warning("to few parts in line");
		return FALSE;
	}
	while (*++section)
		;
	parts[cnt] = section + 1;
	parts[JSON] += 1;
	{
		char* itr = parts[JSON];
		while (*++itr)
			;
		itr[-1] = 0;
	}
	return TRUE;
}

static guint64
parse_ul(const char* str)
{
	return strtoul(str, NULL, 10);
}

static gboolean
replay_object(void* memory_chunk, guint64 memory_chunk_size, char** parts)
{
	static gpointer data = NULL;
	const char* op = parts[OPERATION];
	gboolean ret = FALSE;
	(void)memory_chunk_size; /// \TODO usage?
	if (strcmp(op, "create") == 0)
	{
		if (data != NULL)
		{
			g_warning("open new object while still one open");
		}
		ret = j_backend_object_create(object_backend, parts[NAMESPACE], parts[NAME], &data);
	}
	else if (strcmp(op, "open") == 0)
	{
		if (data != NULL)
		{
			g_warning("open new object while still one open");
		}
		ret = j_backend_object_open(object_backend, parts[NAMESPACE], parts[NAME], &data);
	}
	else if (strcmp(op, "get_all") == 0)
	{
		if (data != NULL)
		{
			g_warning("open new object while still one open");
		}
		ret = j_backend_object_get_all(object_backend, parts[NAMESPACE], data);
	}
	else if (strcmp(op, "ge_by_prefix") == 0)
	{
		if (data != NULL)
		{
			g_warning("open new object while still one open");
		}
		ret = j_backend_kv_get_by_prefix(object_backend, parts[NAMESPACE], parts[NAME], &data);
	}
	else if (strcmp(op, "read") == 0)
	{
		static guint64 bytes_read = 0;
		guint64 size = parse_ul(parts[SIZE]);
		if (size > memory_chunk_size)
		{
			g_warning("unable to replay: chunk size to small! %lu vs %lu", size, memory_chunk_size);
		}
		else
		{
			ret = j_backend_object_read(object_backend, data, memory_chunk, size, parse_ul(parts[COMPLEXITY]), &bytes_read);
		}
	}
	else if (strcmp(op, "write") == 0)
	{
		static guint64 bytes_written = 0;
		guint64 size = parse_ul(parts[SIZE]);
		if (size > memory_chunk_size)
		{
			g_warning("unable to replay: chunk size to small! %lu vs %lu", size, memory_chunk_size);
		}
		else
		{
			ret = j_backend_object_write(object_backend, data, memory_chunk, size, parse_ul(parts[COMPLEXITY]), &bytes_written);
		}
	}
	else if (strcmp(op, "status") == 0)
	{
		static gint64 modification_time;
		static guint64 size;
		ret = j_backend_object_status(object_backend, data, &modification_time, &size);
	}
	else if (strcmp(op, "sync") == 0)
	{
		ret = j_backend_object_sync(object_backend, data);
	}
	else if (strcmp(op, "iterate") == 0)
	{
		static const char* name = NULL;
		ret = j_backend_object_iterate(object_backend, data, &name);
		if (!ret)
		{
			data = NULL;
			ret = TRUE;
		}
	}
	else if (strcmp(op, "close") == 0)
	{
		ret = j_backend_object_close(object_backend, data);
		data = NULL;
	}
	else if (strcmp(op, "delete") == 0)
	{
		ret = j_backend_object_delete(object_backend, data);
		data = NULL;
	}
	else if (strcmp(op, "init") == 0)
	{
		ret = TRUE;
	}
	else if (strcmp(op, "fini") == 0)
	{
		ret = TRUE;
	}
	else
	{
		g_warning("unkown object operation: '%s'", op);
	}
	return ret;
}

static void
update_semantics(JSemantics** semantics, guint32* serial_semantics, const char* parts_semantics_str)
{
	guint32 parts_semantics = parse_ul(parts_semantics_str);
	if (*semantics == NULL || *serial_semantics != parts_semantics)
	{
		*serial_semantics = parts_semantics;
		if (*semantics)
		{
			j_semantics_unref(*semantics);
		}
		*semantics = j_semantics_deserialize(*serial_semantics);
	}
}

static gboolean
replay_kv(void* memory_chunk, guint64 memory_chunk_size, char** parts)
{
	gboolean ret = FALSE;
	const char* op = parts[OPERATION];
	static gpointer batch = NULL;
	static JSemantics* semantics = NULL;
	static guint32 serial_semantics;

	if (strcmp(op, "batch_start") == 0)
	{
		if (batch != NULL)
		{
			g_warning("starting a new batch, but old one is not finished");
		}
		update_semantics(&semantics, &serial_semantics, parts[SEMANTIC]);
		ret = j_backend_kv_batch_start(kv_backend, parts[NAMESPACE], semantics, &batch);
	}
	else if (strcmp(op, "put") == 0)
	{
		guint64 size = parse_ul(parts[SIZE]);
		if (size > memory_chunk_size)
		{
			g_warning("unable to replay: chunk size to small! %lu vs %lu", size, memory_chunk_size);
		}
		else
		{
			ret = j_backend_kv_put(kv_backend, batch, parts[NAME], memory_chunk, size);
		}
	}
	else if (strcmp(op, "delete") == 0)
	{
		ret = j_backend_kv_delete(kv_backend, batch, parts[NAME]);
	}
	else if (strcmp(op, "get") == 0)
	{
		guint32 size;
		gpointer value;
		ret = j_backend_kv_get(kv_backend, batch, parts[NAME], &value, &size);
	}
	else if (strcmp(op, "get_all") == 0)
	{
		if (batch != NULL)
		{
			g_warning("start iterating with remaining batch or iteration");
		}
		ret = j_backend_kv_get_all(kv_backend, parts[NAMESPACE], &batch);
	}
	else if (strcmp(op, "get_by_prefix") == 0)
	{
		if (batch != NULL)
		{
			g_warning("start iterating with remaining batch or iteration");
		}
		ret = j_backend_kv_get_by_prefix(kv_backend, parts[NAMESPACE], parts[NAME], &batch);
	}
	else if (strcmp(op, "iterate") == 0)
	{
		const char* name;
		gconstpointer value;
		guint32 len;
		ret = j_backend_kv_iterate(kv_backend, batch, &name, &value, &len);
		if (!ret)
		{
			batch = NULL;
			ret = TRUE;
		}
	}
	else if (strcmp(op, "init") == 0)
	{
		ret = TRUE;
	}
	else if (strcmp(op, "fini") == 0)
	{
		ret = TRUE;
	}
	else if (strcmp(op, "batch_execute") == 0)
	{
		ret = j_backend_kv_batch_execute(kv_backend, batch);
	}
	else
	{
		g_warning("unkown operation: '%s'", op);
	}
	return ret;
}

struct JSqlBatch
{
	const gchar* namespace;
	JSemantics* semantics;
	gboolean open;
	gboolean aborted;
};

static gboolean
replay_db(char** parts)
{
	gboolean ret = FALSE;
	static gpointer batch = NULL;
	static gpointer itr = NULL;
	static gchar* namespace = NULL;
	static JSemantics* semantics = NULL;
	static guint32 serial_semantics;
	GError* error = NULL;
	const char* op = parts[OPERATION];
	static GArray* bsons = NULL;
	if (bsons == NULL)
	{
		bsons = g_array_sized_new(FALSE, TRUE, sizeof(bson_t*), 0);
	}
	if (semantics == NULL)
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	}

	if (strcmp(op, "batch_start") == 0)
	{
		if (batch != NULL)
		{
			g_warning("starting a new batch, but old one is not finished");
		}
		update_semantics(&semantics, &serial_semantics, parts[SEMANTIC]);
		namespace = strdup(parts[NAMESPACE]);
		ret = j_backend_db_batch_start(db_backend, namespace, semantics, &batch, &error);
	}
	else if (strcmp(op, "schema_create") == 0)
	{
		bson_t* schema = bson_new_from_json((const uint8_t*)parts[JSON], -1, NULL);
		ret = j_backend_db_schema_create(db_backend, batch, parts[NAME], schema, &error);
		g_array_append_val(bsons, schema);
	}
	else if (strcmp(op, "schema_get") == 0)
	{
		bson_t* schema = bson_new();
		ret = j_backend_db_schema_get(db_backend, batch, parts[NAME], schema, &error);
		g_array_append_val(bsons, schema);
		// schema do not exists
		if (error && error->code == 8)
		{
			g_error_free(error);
			error = NULL;
			ret = TRUE;
		}
	}
	else if (strcmp(op, "schema_delete") == 0)
	{
		ret = j_backend_db_schema_delete(db_backend, batch, parts[NAME], &error);
	}
	else if (strcmp(op, "insert") == 0)
	{
		bson_t* entry = bson_new_from_json((const uint8_t*)parts[JSON], -1, NULL);
		bson_t* res = bson_new();
		ret = j_backend_db_insert(db_backend, batch, parts[NAME], entry, res, &error);
		g_array_append_val(bsons, entry);
		g_array_append_val(bsons, res);
	}
	else if (strcmp(op, "delete") == 0)
	{
		bson_t* entry = bson_new_from_json((const uint8_t*)parts[JSON], -1, NULL);
		ret = j_backend_db_delete(db_backend, batch, parts[NAME], entry, &error);
		g_array_append_val(bsons, entry);
	}
	else if (strcmp(op, "update") == 0)
	{
		bson_iter_t bson_iter;
		bson_t* selector;
		bson_t* entry;
		const uint8_t* doc;
		uint32_t len;
		bson_t* bson = bson_new_from_json((const uint8_t*)parts[JSON], -1, NULL);
		bson_iter_init_find(&bson_iter, bson, "entry");
		bson_iter_document(&bson_iter, &len, &doc);
		entry = bson_new_from_data(doc, len);
		bson_iter_init_find(&bson_iter, bson, "selector");
		bson_iter_document(&bson_iter, &len, &doc);
		selector = bson_new_from_data(doc, len);
		ret = j_backend_db_update(db_backend, batch, parts[NAME], selector, entry, &error);
		g_array_append_val(bsons, entry);
		g_array_append_val(bsons, selector);
		g_array_append_val(bsons, bson);
	}
	else if (strcmp(op, "query") == 0)
	{
		bson_t* selector = bson_new_from_json((const uint8_t*)parts[JSON], -1, NULL);
		if (itr != NULL)
		{
			g_warning("start new db iteration without finishing old one!");
		}
		ret = j_backend_db_query(db_backend, batch, parts[NAME], selector, &itr, &error);
		g_array_append_val(bsons, selector);
	}
	else if (strcmp(op, "iterate") == 0)
	{
		bson_t* entry = bson_new();
		ret = j_backend_db_iterate(db_backend, itr, entry, &error);
		g_array_append_val(bsons, entry);
		if (!ret)
		{
			itr = NULL;
			ret = TRUE;
			g_error_free(error);
			error = NULL;
		}
	}
	else if (strcmp(op, "init") == 0)
	{
		ret = TRUE;
	}
	else if (strcmp(op, "fini") == 0)
	{
		ret = TRUE;
	}
	else if (strcmp(op, "batch_execute") == 0)
	{
		ret = j_backend_db_batch_execute(db_backend, batch, &error);
		batch = NULL;
		for (guint i = 0; i < bsons->len; ++i)
		{
			bson_destroy(g_array_index(bsons, bson_t*, i));
		}
		g_array_remove_range(bsons, 0, bsons->len);
		// no operation to do
		if (error && error->code == 7)
		{
			g_error_free(error);
			error = NULL;
			ret = TRUE;
		}
	}
	else
	{
		g_warning("unknown operation: '%s'", op);
	}
	if (error)
	{
		g_warning("db error(%d): %s", error->code, error->message);
	}
	return ret && error == NULL;
}

static gboolean
replay(void* memory_chunk, guint64 memory_chunck_size, char* line)
{
	char* parts[ROW_LEN];
	if (!split(line, parts))
	{
		return FALSE;
	}
	if (strcmp(parts[BACKEND], "object") == 0)
	{
		if (!replay_object(memory_chunk, memory_chunck_size, parts))
			return FALSE;
	}
	if (strcmp(parts[BACKEND], "kv") == 0)
	{
		if (!replay_kv(memory_chunk, memory_chunck_size, parts))
			return FALSE;
	}
	if (strcmp(parts[BACKEND], "db") == 0)
	{
		if (!replay_db(parts))
			return FALSE;
	}

	return TRUE;
}

int
main(int argc, char** argv)
{
	JConfiguration* configuration = NULL;
	const char* record_file_name = NULL;
	FILE* record_file = NULL;
	GModule* db_module = NULL;
	GModule* kv_module = NULL;
	GModule* object_module = NULL;
	g_autofree gchar* port_str = NULL;
	gint res = 1;
	gchar* memory_chunk;
	guint64 memory_chunck_size = 0;

	setlocale(LC_ALL, "C.UTF-8");

	configuration = j_configuration();
	if (configuration == NULL)
	{
		g_warning("unable to read config");
		goto end;
		if (argc < 2)
		{
			g_warning("useage: %s <access_dump.csv>", argv[0]);
			goto end;
		}
	}
	record_file_name = argv[1];
	if (access(record_file_name, R_OK) != 0)
	{
		g_warning("file does not exists, or no read permission '%s'", record_file_name);
		goto end;
	}
	record_file = fopen(record_file_name, "r");
	if (record_file == NULL)
	{
		g_warning("failed to open file '%s'", record_file_name);
		goto end;
	}

	port_str = g_strdup_printf("%d", j_configuration_get_port(configuration));
	if (!setup_backend(configuration, J_BACKEND_TYPE_OBJECT, port_str, &object_module, &object_backend))
	{
		g_warning("failed to initealize object backend");
		goto end;
	}
	if (!setup_backend(configuration, J_BACKEND_TYPE_KV, port_str, &kv_module, &kv_backend))
	{
		g_warning("failed to initealize kv backend");
		goto end;
	}
	if (!setup_backend(configuration, J_BACKEND_TYPE_DB, port_str, &db_module, &db_backend))
	{
		g_warning("failed to initealize db backend");
		goto end;
	}

	memory_chunck_size = j_configuration_get_max_operation_size(configuration);
	{
		// initialize memory_chunk with random values
		guint32* memory = malloc(memory_chunck_size + memory_chunck_size % 4);
		GRand* rng = g_rand_new();
		for (guint64 i = 0; i < (memory_chunck_size + 3) / 4; i += 1)
		{
			memory[i] = g_rand_int(rng);
		}

		g_rand_free(rng);
		memory_chunk = (char*)memory;
	}

	{
		const char* header = "time,process_uid,program_name,backend,type,path,namespace,name,operation,semantics,size,complexity,duration,bson";
		char* line = NULL;
		size_t len = 0;
		ssize_t read = 0;
		read = getline(&line, &len, record_file);
		line[read - 1] = 0;
		if (read == -1 || strcmp(line, header) != 0)
		{
			g_warning("Invalid header in dump!\n'%s'\n'%s'\n", header, line);
			goto end;
		}
		while ((read = getline(&line, &len, record_file)) != -1)
		{
			line[read - 1] = 0;
			if (!replay(memory_chunk, memory_chunck_size, line))
			{
				g_warning("failed to replay dump!");
				goto end;
			}
		}
	}
	res = 0;
end:
	if (record_file)
	{
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
	if (object_backend != NULL)
	{
		j_backend_object_fini(object_backend);
	}
	if (db_module != NULL)
	{
		g_module_close(db_module);
	}
	if (kv_module != NULL)
	{
		g_module_close(kv_module);
	}
	if (object_module != NULL)
	{
		g_module_close(object_module);
	}
	j_configuration_unref(configuration);

	return res;
}
