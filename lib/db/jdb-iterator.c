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

JDBIterator*
j_db_iterator_new(JDBSchema* schema, JDBSelector* selector, JBatch* batch, GError** error)
{
	guint ret;
	JDBIterator* iterator = NULL;
	j_goto_error_frontend(!schema, JULEA_FRONTEND_ERROR_SCHEMA_NULL, "");
	j_goto_error_frontend(!batch, JULEA_FRONTEND_ERROR_BATCH_NULL, "");
	iterator = g_slice_new(JDBIterator);
	iterator->schema = j_db_schema_ref(schema, error);
	j_goto_error_subcommand(!iterator->schema);
	iterator->selector = j_db_selector_ref(selector, error);
	j_goto_error_subcommand(!iterator->selector && selector);
	iterator->iterator = NULL;
	iterator->ref_count = 1;
	iterator->valid = FALSE;
	iterator->bson_valid = FALSE;
	ret = j_db_internal_query(schema->namespace, schema->name, &selector->bson, &iterator->iterator, batch, error);
	j_goto_error_subcommand(!ret);
	iterator->valid = TRUE;
	return iterator;
_error:
	j_db_iterator_unref(iterator);
	return NULL;
}
JDBIterator*
j_db_iterator_ref(JDBIterator* iterator, GError** error)
{
	j_goto_error_frontend(!iterator, JULEA_FRONTEND_ERROR_ITERATOR_NULL, "");
	g_atomic_int_inc(&iterator->ref_count);
	return iterator;
_error:
	return NULL;
}
void
j_db_iterator_unref(JDBIterator* iterator)
{
	if (iterator && g_atomic_int_dec_and_test(&iterator->ref_count))
	{
		while (iterator->valid)
			j_db_iterator_next(iterator, NULL);
		j_db_schema_unref(iterator->schema);
		j_db_selector_unref(iterator->selector);
		if (iterator->bson_valid)
			bson_destroy(&iterator->bson);
		g_slice_free(JDBIterator, iterator);
	}
}
gboolean
j_db_iterator_next(JDBIterator* iterator, GError** error)
{
	guint ret;
	j_goto_error_frontend(!iterator, JULEA_FRONTEND_ERROR_ITERATOR_NULL, "");
	j_goto_error_frontend(!iterator->valid, JULEA_FRONTEND_ERROR_ITERATOR_NO_MORE_ELEMENTS, "");
	if (iterator->bson_valid)
		bson_destroy(&iterator->bson);
	ret = j_db_internal_iterate(iterator->iterator, &iterator->bson, error);
	j_goto_error_subcommand(!ret);
	iterator->bson_valid = TRUE;
	return TRUE;
_error:
	if (iterator)
		iterator->valid = FALSE;
	return FALSE;
}
gboolean
j_db_iterator_get_field(JDBIterator* iterator, gchar const* name, JDBType* type, gpointer* value, guint64* length, GError** error)
{
	uint32_t len;
	const uint8_t* binary;
	gint ret;
	bson_iter_t iter;
	j_goto_error_frontend(!iterator, JULEA_FRONTEND_ERROR_ITERATOR_NULL, "");
	j_goto_error_frontend(!iterator->bson_valid, JULEA_FRONTEND_ERROR_BSON_NOT_INITIALIZED, "");
	j_goto_error_frontend(!name, JULEA_FRONTEND_ERROR_NAME_NULL, "");
	j_goto_error_frontend(!type, JULEA_FRONTEND_ERROR_TYPE_NULL, "");
	j_goto_error_frontend(!value, JULEA_FRONTEND_ERROR_VALUE_NULL, "");
	j_goto_error_frontend(!length, JULEA_FRONTEND_ERROR_LENGTH_NULL, "");
	ret = j_db_schema_get_field(iterator->schema, name, type, error);
	j_goto_error_subcommand(!ret);
	ret = bson_iter_init(&iter, &iterator->bson);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "");
	ret = bson_iter_find(&iter, name);
	j_goto_error_frontend(!ret, JULEA_FRONTEND_ERROR_VARIABLE_NOT_FOUND, "");
	switch (*type)
	{
	case J_DB_TYPE_SINT32:
		j_goto_error_frontend(!BSON_ITER_HOLDS_INT32(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		*value = g_new(gint32, 1);
		*(gint32*)value = bson_iter_int32(&iter);
		*length = sizeof(gint32);
		break;
	case J_DB_TYPE_UINT32:
		j_goto_error_frontend(!BSON_ITER_HOLDS_INT32(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		*value = g_new(guint32, 1);
		*(guint32*)value = bson_iter_int32(&iter);
		*length = sizeof(guint32);
		break;
	case J_DB_TYPE_FLOAT32:
		j_goto_error_frontend(!BSON_ITER_HOLDS_DOUBLE(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		*value = g_new(gfloat, 1);
		*(gfloat*)value = bson_iter_double(&iter);
		*length = sizeof(gfloat);
		break;
	case J_DB_TYPE_SINT64:
		j_goto_error_frontend(!BSON_ITER_HOLDS_INT64(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		*value = g_new(gint64, 1);
		*(gint64*)value = bson_iter_int64(&iter);
		*length = sizeof(gint64);
		break;
	case J_DB_TYPE_UINT64:
		j_goto_error_frontend(!BSON_ITER_HOLDS_INT64(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		*value = g_new(guint64, 1);
		*(guint64*)value = bson_iter_int64(&iter);
		*length = sizeof(guint64);
		break;
	case J_DB_TYPE_FLOAT64:
		j_goto_error_frontend(!BSON_ITER_HOLDS_DOUBLE(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		*value = g_new(gdouble, 1);
		*(gdouble*)value = bson_iter_double(&iter);
		*length = sizeof(gdouble);
		break;
	case J_DB_TYPE_STRING:
		j_goto_error_frontend(!BSON_ITER_HOLDS_UTF8(&iter), JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		bson_iter_utf8(&iter, &len);
		*value = g_new(gchar, len);
		memcpy(*value, bson_iter_utf8(&iter, NULL), len);
		*length = len;
		break;
	case J_DB_TYPE_BLOB:
		if (BSON_ITER_HOLDS_BINARY(&iter))
		{
			bson_iter_binary(&iter, NULL, &len, &binary);
			*value = g_new(gchar, len);
			memcpy(*value, binary, len);
			*length = len;
		}
		else if (BSON_ITER_HOLDS_NULL(&iter))
		{
			*value = NULL;
			*length = 0;
		}
		else
			j_goto_error_frontend(TRUE, JULEA_FRONTENT_ERROR_BSON_INVALID_TYPE, bson_iter_type(&iter));
		break;
	case _J_DB_TYPE_COUNT:
	default:
		abort();
	}
	return TRUE;
_error:
	return FALSE;
}
