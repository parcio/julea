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
#include "jdb-internal.h"
#include <julea-db.h>
#include "../../backend/db/jbson.c"

JDBSelector*
j_db_selector_new(JDBSchema* schema, JDBSelectorMode mode, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue val;
	JDBSelector* selector = NULL;

	g_return_val_if_fail(mode < _J_DB_SELECTOR_MODE_COUNT, FALSE);

	selector = g_slice_new(JDBSelector);
	selector->ref_count = 1;
	selector->mode = mode;
	selector->bson_count = 0;
	bson_init(&selector->bson);
	selector->schema = j_db_schema_ref(schema, error);
	if (G_UNLIKELY(!selector->schema))
	{
		goto _error;
	}
	val.val_uint32 = mode;
	if (G_UNLIKELY(!j_bson_append_value(&selector->bson, "_mode", J_DB_TYPE_UINT32, &val, error)))
	{
		goto _error;
	}
	return selector;
_error:
	j_db_selector_unref(selector);
	return NULL;
}
JDBSelector*
j_db_selector_ref(JDBSelector* selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(selector != NULL, FALSE);
	(void)error;

	g_atomic_int_inc(&selector->ref_count);
	return selector;
}
void
j_db_selector_unref(JDBSelector* selector)
{
	J_TRACE_FUNCTION(NULL);

	if (selector && g_atomic_int_dec_and_test(&selector->ref_count))
	{
		j_db_schema_unref(selector->schema);
		bson_destroy(&selector->bson);
		g_slice_free(JDBSelector, selector);
	}
}
gboolean
j_db_selector_add_field(JDBSelector* selector, gchar const* name, JDBSelectorOperator operator, gconstpointer value, guint64 length, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	char buf[20];
	bson_t bson;
	JDBType type;
	JDBTypeValue val;

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(operator<_J_DB_SELECTOR_OPERATOR_COUNT, FALSE);

	if (G_UNLIKELY(selector->bson_count + 1 > 500))
	{
		g_set_error_literal(error, J_FRONTEND_DB_ERROR, J_FRONTEND_DB_ERROR_SELECTOR_TOO_COMPLEX, "selector too complex");
		goto _error;
	}
	if (G_UNLIKELY(!j_db_schema_get_field(selector->schema, name, &type, error)))
	{
		goto _error;
	}
	sprintf(buf, "%d", selector->bson_count);
	if (G_UNLIKELY(!j_bson_append_document_begin(&selector->bson, buf, &bson, error)))
	{
		goto _error;
	}
	val.val_string = name;
	if (G_UNLIKELY(!j_bson_append_value(&bson, "_name", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}
	val.val_uint32 = operator;
	if (G_UNLIKELY(!j_bson_append_value(&bson, "_operator", J_DB_TYPE_UINT32, &val, error)))
	{
		goto _error;
	}
	switch (type)
	{
	case J_DB_TYPE_SINT32:
		val.val_sint32 = *(gint32 const*)value;
		break;
	case J_DB_TYPE_UINT32:
		val.val_uint32 = *(guint32 const*)value;
		break;
	case J_DB_TYPE_FLOAT32:
		val.val_float32 = *(gfloat const*)value;
		break;
	case J_DB_TYPE_SINT64:
		val.val_sint64 = *(gint64 const*)value;
		break;
	case J_DB_TYPE_UINT64:
		val.val_sint64 = *(gint64 const*)value;
		break;
	case J_DB_TYPE_FLOAT64:
		val.val_float64 = *(gdouble const*)value;
		break;
	case J_DB_TYPE_STRING:
		val.val_string = value;
		break;
	case J_DB_TYPE_BLOB:
		val.val_blob = value;
		val.val_blob_length = length;
		break;
	case J_DB_TYPE_ID:
	case _J_DB_TYPE_COUNT:
	default:;
	}
	if (G_UNLIKELY(!j_bson_append_value(&bson, "_value", type, &val, error)))
	{
		goto _error;
	}
	if (G_UNLIKELY(!j_bson_append_document_end(&selector->bson, &bson, error)))
	{
		goto _error;
	}
	selector->bson_count++;
	return TRUE;
_error:
	return FALSE;
}
gboolean
j_db_selector_add_selector(JDBSelector* selector, JDBSelector* sub_selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	char buf[20];

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(sub_selector != NULL, FALSE);
	g_return_val_if_fail(selector != sub_selector, FALSE);
	g_return_val_if_fail(selector->schema == sub_selector->schema, FALSE);

	if (G_UNLIKELY(!sub_selector->bson_count))
	{
		g_set_error_literal(error, J_FRONTEND_DB_ERROR, J_FRONTEND_DB_ERROR_SELECTOR_EMPTY, "selector must not be emoty");
		goto _error;
	}
	if (G_UNLIKELY(selector->bson_count + sub_selector->bson_count > 500))
	{
		g_set_error_literal(error, J_FRONTEND_DB_ERROR, J_FRONTEND_DB_ERROR_SELECTOR_TOO_COMPLEX, "selector too complex");
		goto _error;
	}
	sprintf(buf, "%d", selector->bson_count);
	if (G_UNLIKELY(!j_bson_append_document(&selector->bson, buf, &sub_selector->bson, error)))
	{
		goto _error;
	}
	selector->bson_count += sub_selector->bson_count;
	return TRUE;
_error:
	return FALSE;
}

bson_t*
j_db_selector_get_bson(JDBSelector* selector)
{
	if (selector && selector->bson_count > 0)
	{
		return &selector->bson;
	}
	return NULL;
}
