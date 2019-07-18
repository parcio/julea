/*
 * JULEA - Flexible storage framework
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

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <bson.h>

#include <julea.h>
#include <julea-internal.h>
#include <db/jdb-internal.h>
#include <julea-db.h>

JDBSchema*
j_db_schema_new(gchar const* namespace, gchar const* name, GError** error)
{
	JDBSchema* schema = NULL;
	j_goto_error_frontend(!namespace, JULEA_FRONTEND_ERROR_NAMESPACE_NULL, "");
	j_goto_error_frontend(!name, JULEA_FRONTEND_ERROR_NAME_NULL, "");
	schema = g_slice_new(JDBSchema);
	schema->namespace = g_strdup(namespace);
	schema->name = g_strdup(name);
	schema->bson_initialized = FALSE;
	schema->bson_index_initialized = FALSE;
	schema->ref_count = 1;
	schema->server_side = FALSE;
	bson_init(&schema->bson);
	return schema;
_error:
	return NULL;
}
JDBSchema*
j_db_schema_ref(JDBSchema* schema, GError** error)
{
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	g_atomic_int_inc(&schema->ref_count);
	return schema;
_error:
	return NULL;
}
void
j_db_schema_unref(JDBSchema* schema)
{
	if (schema && g_atomic_int_dec_and_test(&schema->ref_count))
	{
		g_free(schema->namespace);
		g_free(schema->name);
		if (schema->bson_initialized)
			bson_destroy(&schema->bson);
		g_slice_free(JDBSchema, schema);
	}
}
gboolean
j_db_schema_add_field(JDBSchema* schema, gchar const* name, JDBType type, GError** error)
{
	gint ret;
	bson_iter_t iter;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!name, JULEA_FRONTEND_ERROR_VARIABLE_NAME_NULL, "");
	j_goto_error_frontend(type >= _J_DB_TYPE_COUNT, JULEA_FRONTEND_ERROR_DB_TYPE_INVALID, "");
	j_goto_error_frontend(schema->server_side, JULEA_FRONTEND_ERROR_DB_BSON_SERVER, "");
	if (!schema->bson_initialized)
	{
		bson_init(&schema->bson);
		schema->bson_initialized = TRUE;
	}
	ret = bson_iter_init(&iter, &schema->bson);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
	ret = bson_iter_find(&iter, name);
	j_goto_error_frontend(ret, JULEA_FRONTEND_ERROR_BSON_KEY_FOUND, "");
	ret = bson_append_int32(&schema->bson, name, -1, type);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_APPEND_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_schema_get_field(JDBSchema* schema, gchar const* name, JDBType* type, GError** error)
{
	gint ret;
	bson_iter_t iter;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!name, JULEA_FRONTEND_ERROR_VARIABLE_NAME_NULL, "");
	j_goto_error_frontend(!type, JULEA_FRONTEND_ERROR_VARIABLE_TYPE_NULL, "");
	j_goto_error_frontend(!schema->bson_initialized, JULEA_FRONTEND_ERROR_VARIABLE_NOT_FOUND, "");
	j_goto_error_frontend(!g_strcmp0(name, "_index"), JULEA_FRONTEND_ERROR_VARIABLE_NOT_FOUND, "");
	ret = bson_iter_init(&iter, &schema->bson);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
	ret = bson_iter_find(&iter, name);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_VARIABLE_NOT_FOUND, "");
	*type = bson_iter_int32(&iter);
	return TRUE;
_error:
	return FALSE;
}
guint32
j_db_schema_get_all_fields(JDBSchema* schema, gchar*** names, JDBType** types, GError** error)
{
	gint ret;
	bson_iter_t iter;
	guint count;
	guint i;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!names, JULEA_FRONTEND_ERROR_VARIABLE_NAME_NULL, "");
	j_goto_error_frontend(!types, JULEA_FRONTEND_ERROR_VARIABLE_TYPE_NULL, "");
	j_goto_error_frontend(!schema->bson_initialized, JULEA_FRONTEND_ERROR_VARIABLE_NOT_FOUND, "");
	ret = bson_iter_init(&iter, &schema->bson);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
	count = bson_count_keys(&schema->bson) + 1;
	*names = g_new(gchar*, count);
	*types = g_new(JDBType, count);
	i = 0;
	while (bson_iter_next(&iter))
	{
		if (g_strcmp0(bson_iter_key(&iter), "_index"))
		{
			(*names)[i] = g_strdup(bson_iter_key(&iter));
			(*types)[i] = bson_iter_int32(&iter);
			i++;
		}
	}
	(*names)[i] = NULL;
	(*types)[i] = _J_DB_TYPE_COUNT;
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_schema_add_index(JDBSchema* schema, gchar const** names, GError** error)
{
	guint i;
	guint ret;
	bson_t bson;
	const char* key;
	char buf[20];
	gchar const** name;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!names, JULEA_FRONTEND_ERROR_VARIABLE_NAME_NULL, "");
	j_goto_error_frontend(!*names, JULEA_FRONTEND_ERROR_VARIABLE_NAME_NULL, "");
	j_goto_error_frontend(schema->server_side, JULEA_FRONTEND_ERROR_DB_BSON_SERVER, "");
	if (!schema->bson_index_initialized)
	{
		bson_init(&schema->bson_index);
		schema->bson_index_count = 0;
		schema->bson_index_initialized = TRUE;
	}
	bson_uint32_to_string(schema->bson_index_count, &key, buf, sizeof(buf));
	ret = bson_append_array_begin(&schema->bson_index, key, -1, &bson);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_APPEND_FAILED, "");
	i = 0;
	name = names;
	while (name)
	{
		if (*name)
		{
			bson_uint32_to_string(i, &key, buf, sizeof(buf));
			ret = bson_append_utf8(&bson, key, -1, *name, -1);
			j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_APPEND_FAILED, "");
			name++;
		}
		i++;
	}
	ret = bson_append_array_end(&schema->bson_index, &bson);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_APPEND_FAILED, "");
	schema->bson_index_count++;
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_schema_create(JDBSchema* schema, JBatch* batch, GError** error)
{
	gint ret;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!batch, JULEA_FRONTEND_ERROR_BATCH_NULL, "");
	j_goto_error_frontend(schema->server_side, JULEA_FRONTEND_ERROR_DB_BSON_SERVER, "");
	j_goto_error_frontend(!schema->bson_initialized, JULEA_FRONTEND_ERROR_BSON_NOT_INITIALIZED, "");
	if (schema->bson_index_initialized)
	{
		ret = bson_append_array(&schema->bson, "_index", -1, &schema->bson_index);
		j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_APPEND_FAILED, "");
	}
	schema->server_side = TRUE;
	ret = j_db_internal_schema_create(schema->namespace, schema->name, &schema->bson, batch, error);
	j_goto_error_subcommand(!ret);
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_schema_get(JDBSchema* schema, JBatch* batch, GError** error)
{
	gint ret;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!batch, JULEA_FRONTEND_ERROR_BATCH_NULL, "");
	j_goto_error_frontend(schema->server_side, JULEA_FRONTEND_ERROR_DB_BSON_SERVER, "");
	j_goto_error_frontend(schema->bson_initialized, JULEA_FRONTEND_ERROR_BSON_INITIALIZED, "");
	schema->server_side = TRUE;
	schema->bson_initialized = TRUE;
	ret = j_db_internal_schema_get(schema->namespace, schema->name, &schema->bson, batch, error);
	j_goto_error_subcommand(!ret);
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_schema_delete(JDBSchema* schema, JBatch* batch, GError** error)
{
	gint ret;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!batch, JULEA_FRONTEND_ERROR_BATCH_NULL, "");
	ret = j_db_internal_schema_delete(schema->namespace, schema->name, batch, error);
	j_goto_error_subcommand(!ret);
	return TRUE;
_error:
	return FALSE;
}

gboolean
j_db_schema_equals(JDBSchema* schema1, JDBSchema* schema2, gboolean* equal, GError** error)
{
	guint schema1_count;
	guint schema2_count;
	bson_iter_t iter1;
	bson_iter_t iter2;
	gint ret;
	j_goto_error_frontend(!schema1, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!schema2, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	if (schema1 == schema2)
	{
		*equal = TRUE;
	}
	else
	{
		*equal = TRUE;
		*equal = *equal && !g_strcmp0(schema1->namespace, schema2->namespace);
		*equal = *equal && !g_strcmp0(schema1->name, schema2->name);
		*equal = *equal && (schema1->bson_initialized == schema2->bson_initialized);
		if (*equal && schema1->bson_initialized)
		{
			schema1_count = 0;
			schema2_count = 0;
			ret = bson_iter_init(&iter1, &schema1->bson);
			j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
			while (bson_iter_next(&iter1))
				if (g_strcmp0(bson_iter_key(&iter1), "_index"))
				{
					schema1_count++;
					ret = bson_iter_init(&iter2, &schema2->bson);
					j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
					ret = bson_iter_find(&iter2, bson_iter_key(&iter1));
					*equal = *equal && ret;
					if (!*equal)
						break;
					*equal = *equal && bson_iter_int32(&iter1) == bson_iter_int32(&iter2);
				}
			ret = bson_iter_init(&iter2, &schema2->bson);
			j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
			while (bson_iter_next(&iter2))
				if (g_strcmp0(bson_iter_key(&iter2), "_index"))
					schema2_count++;
			*equal = *equal && schema1_count == schema2_count;
		}
	}
	return TRUE;
_error:
	return FALSE;
}
