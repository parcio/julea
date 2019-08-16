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
#include <db/jdb-internal.h>
#include <julea-db.h>
#include "../../backend/db/jbson.c"

JDBIterator*
j_db_iterator_new(JDBSchema* schema, JDBSelector* selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	guint ret;
	guint ret2 = FALSE;
	JBatch* batch;
	JDBIterator* iterator = NULL;

	g_return_val_if_fail(schema != NULL, FALSE);
	g_return_val_if_fail((selector == NULL) || (selector->schema == schema), FALSE);

	iterator = g_slice_new(JDBIterator);
	iterator->schema = j_db_schema_ref(schema, error);
	if (G_UNLIKELY(!iterator->schema))
	{
		goto _error;
	}
	if (selector)
	{
		iterator->selector = j_db_selector_ref(selector, error);
		if (G_UNLIKELY(!iterator->selector))
		{
			goto _error;
		}
	}
	else
	{
		iterator->selector = NULL;
	}
	iterator->iterator = NULL;
	iterator->ref_count = 1;
	iterator->valid = FALSE;
	iterator->bson_valid = FALSE;
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	ret2 = j_db_internal_query(schema->namespace, schema->name, j_db_selector_get_bson(selector), &iterator->iterator, batch, error);
	ret = ret2 && j_batch_execute(batch);
	j_batch_unref(batch);
	if (G_UNLIKELY(!ret))
	{
		goto _error;
	}
	iterator->valid = TRUE;
	return iterator;
_error:
	if (ret2)
	{
		while (j_db_internal_iterate(iterator->iterator, NULL, NULL))
		{
			/*do nothing*/
		}
	}
	j_db_iterator_unref(iterator);
	return NULL;
}
JDBIterator*
j_db_iterator_ref(JDBIterator* iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, FALSE);
	(void)error;

	g_atomic_int_inc(&iterator->ref_count);
	return iterator;
}
void
j_db_iterator_unref(JDBIterator* iterator)
{
	J_TRACE_FUNCTION(NULL);

	if (iterator && g_atomic_int_dec_and_test(&iterator->ref_count))
	{
		while (iterator->valid)
		{
			j_db_iterator_next(iterator, NULL);
		}
		j_db_schema_unref(iterator->schema);
		j_db_selector_unref(iterator->selector);
		if (iterator->bson_valid)
		{
			j_bson_destroy(&iterator->bson);
		}
		g_slice_free(JDBIterator, iterator);
	}
}
gboolean
j_db_iterator_next(JDBIterator* iterator, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(iterator->valid, FALSE);

	if (iterator->bson_valid)
	{
		j_bson_destroy(&iterator->bson);
	}
	if (G_UNLIKELY(!j_db_internal_iterate(iterator->iterator, &iterator->bson, error)))
	{
		goto _error;
	}
	iterator->bson_valid = TRUE;
	return TRUE;
_error:
	iterator->valid = FALSE;
	iterator->bson_valid = FALSE;
	return FALSE;
}
gboolean
j_db_iterator_get_field(JDBIterator* iterator, gchar const* name, JDBType* type, gpointer* value, guint64* length, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue val;
	bson_iter_t iter;

	g_return_val_if_fail(iterator != NULL, FALSE);
	g_return_val_if_fail(iterator->bson_valid, FALSE);
	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(type != NULL, FALSE);
	g_return_val_if_fail(value != NULL, FALSE);
	g_return_val_if_fail(length != NULL, FALSE);

	if (G_UNLIKELY(!j_db_schema_get_field(iterator->schema, name, type, error)))
	{
		goto _error;
	}
	if (G_UNLIKELY(!j_bson_iter_init(&iter, &iterator->bson, error)))
	{
		goto _error;
	}
	if (G_UNLIKELY(!j_bson_iter_find(&iter, name, error)))
	{
		goto _error;
	}
	if (G_UNLIKELY(!j_bson_iter_value(&iter, *type, &val, error)))
	{
		goto _error;
	}
	switch (*type)
	{
	case J_DB_TYPE_SINT32:
		*value = g_new(gint32, 1);
		*((gint32*)*value) = val.val_sint32;
		*length = sizeof(gint32);
		break;
	case J_DB_TYPE_UINT32:
		*value = g_new(guint32, 1);
		*((guint32*)*value) = val.val_uint32;
		*length = sizeof(guint32);
		break;
	case J_DB_TYPE_FLOAT32:
		*value = g_new(gfloat, 1);
		*((gfloat*)*value) = val.val_float32;
		*length = sizeof(gfloat);
		break;
	case J_DB_TYPE_SINT64:
		*value = g_new(gint64, 1);
		*((gint64*)*value) = val.val_sint64;
		*length = sizeof(gint64);
		break;
	case J_DB_TYPE_UINT64:
		*value = g_new(guint64, 1);
		*((guint64*)*value) = val.val_uint64;
		*length = sizeof(guint64);
		break;
	case J_DB_TYPE_FLOAT64:
		*value = g_new(gdouble, 1);
		*((gdouble*)*value) = val.val_float64;
		*length = sizeof(gdouble);
		break;
	case J_DB_TYPE_STRING:
		*value = g_strdup(val.val_string);
		*length = strlen(val.val_string);
		break;
	case J_DB_TYPE_BLOB:
		*value = g_new(gchar, val.val_blob_length);
		memcpy(*value, val.val_blob, val.val_blob_length);
		*length = val.val_blob_length;
		break;
	case J_DB_TYPE_ID:
	case _J_DB_TYPE_COUNT:
	default:;
	}
	return TRUE;
_error:
	return FALSE;
}
