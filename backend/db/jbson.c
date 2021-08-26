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
#include <gmodule.h>

#include <jtrace.h>

#include <db/jdb-internal.h>

G_GNUC_UNUSED
static gboolean
j_bson_iter_init(bson_iter_t* iter, const bson_t* bson, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_iter_init(iter, bson)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INIT, "bson iter init failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_next(bson_iter_t* iter, gboolean* has_next, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!has_next))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_HAS_NEXT_NULL, "has_next must not be NULL");
		goto _error;
	}

	*has_next = bson_iter_next(iter);

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_key_equals(bson_iter_t* iter, const char* key, gboolean* equals, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!equals))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_EQUALS_NULL, "equals must not be NULL");
		goto _error;
	}

	*equals = !g_strcmp0(bson_iter_key(iter), key);

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static const char*
j_bson_iter_key(bson_iter_t* iter, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	return bson_iter_key(iter);

_error:
	return NULL;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_value(bson_t* bson, const char* name, JDBType type, JDBTypeValue* value, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!name))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NAME_NULL, "name must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!value))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_VALUE_NULL, "value must not be NULL");
		goto _error;
	}

	switch (type)
	{
		case J_DB_TYPE_SINT32:
			if (G_UNLIKELY(!bson_append_int32(bson, name, -1, value->val_sint32)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_UINT32:
			if (G_UNLIKELY(!bson_append_int32(bson, name, -1, value->val_uint32)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_SINT64:
			if (G_UNLIKELY(!bson_append_int64(bson, name, -1, value->val_sint64)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_UINT64:
			if (G_UNLIKELY(!bson_append_int64(bson, name, -1, value->val_uint64)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_FLOAT64:
			if (G_UNLIKELY(!bson_append_double(bson, name, -1, value->val_float64)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_FLOAT32:
			if (G_UNLIKELY(!bson_append_double(bson, name, -1, (gdouble)value->val_float32)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_STRING:
			if (G_UNLIKELY(!bson_append_utf8(bson, name, -1, value->val_string, -1)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				goto _error;
			}

			break;
		case J_DB_TYPE_BLOB:
			if (!value->val_blob)
			{
				if (G_UNLIKELY(!bson_append_null(bson, name, -1)))
				{
					g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
					goto _error;
				}
			}
			else
			{
				if (G_UNLIKELY(!bson_append_binary(bson, name, -1, BSON_SUBTYPE_BINARY, (const uint8_t*)value->val_blob, value->val_blob_length)))
				{
					g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
					goto _error;
				}
			}

			break;
		case J_DB_TYPE_ID:
		default:
			g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_value(bson_iter_t* iter, JDBType type, JDBTypeValue* value, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	memset(value, 0, sizeof(*value));

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!value))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_VALUE_NULL, "value must not be NULL");
		goto _error;
	}

	switch (type)
	{
		case J_DB_TYPE_SINT32:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_INT32(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_sint32 = bson_iter_int32(iter);
			}

			break;
		case J_DB_TYPE_UINT32:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_INT32(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_uint32 = bson_iter_int32(iter);
			}

			break;
		case J_DB_TYPE_FLOAT64:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_DOUBLE(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_float64 = bson_iter_double(iter);
			}

			break;
		case J_DB_TYPE_FLOAT32:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_DOUBLE(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_float32 = bson_iter_double(iter);
			}

			break;
		case J_DB_TYPE_SINT64:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_INT64(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_sint64 = bson_iter_int64(iter);
			}

			break;
		case J_DB_TYPE_UINT64:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_INT64(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_uint64 = bson_iter_int64(iter);
			}

			break;
		case J_DB_TYPE_STRING:
			if (G_UNLIKELY(!BSON_ITER_HOLDS_UTF8(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			if (value)
			{
				value->val_string = bson_iter_utf8(iter, NULL);
			}

			break;
		case J_DB_TYPE_BLOB:
			if (BSON_ITER_HOLDS_NULL(iter))
			{
				value->val_blob_length = 0;
				value->val_blob = NULL;
				break;
			}

			if (G_UNLIKELY(!BSON_ITER_HOLDS_BINARY(iter)))
			{
				g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
				goto _error;
			}

			bson_iter_binary(iter, NULL, &value->val_blob_length, (const uint8_t**)&value->val_blob);
			break;
		case J_DB_TYPE_ID:
		default:
			g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_find(bson_iter_t* iter, const char* key, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!key || !*key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_iter_find(iter, key)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NOT_FOUND, "bson iter can not find key");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_not_find(bson_iter_t* iter, const char* key, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!key || !*key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(bson_iter_find(iter, key)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_FOUND, "bson iter should not find key");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_recurse_array(bson_iter_t* iter, bson_iter_t* iter_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter || !iter_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!BSON_ITER_HOLDS_ARRAY(iter)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		goto _error;
	}

	if (G_UNLIKELY(!bson_iter_recurse(iter, iter_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_RECOURSE, "bson iter recourse failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_recurse_document(bson_iter_t* iter, bson_iter_t* iter_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter || !iter_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!BSON_ITER_HOLDS_DOCUMENT(iter)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		goto _error;
	}

	if (G_UNLIKELY(!bson_iter_recurse(iter, iter_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_RECOURSE, "bson iter recourse failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_iter_copy_document(bson_iter_t* iter, bson_t* bson, GError** error)
{
	const uint8_t* data;
	uint32_t length;

	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!iter))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!BSON_ITER_HOLDS_DOCUMENT(iter)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		goto _error;
	}

	if (bson)
	{
		bson_iter_document(iter, &length, &data);
		bson_init_static(bson, data, length);
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_has_enough_keys(const bson_t* bson, guint32 min_keys, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(bson_count_keys(bson) < min_keys))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NOT_ENOUGH_KEYS, "bson not enough keys");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static void
j_bson_destroy(bson_t* bson)
{
	J_TRACE_FUNCTION(NULL);

	if (bson)
	{
		bson_destroy(bson);
	}
}

G_GNUC_UNUSED
static gboolean
j_bson_init(bson_t* bson, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	bson_init(bson);

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_has_field(bson_t* bson, gchar const* name, gboolean* has_field, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!has_field))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_HAS_FIELD_NULL, "has_field must not be NULL");
		goto _error;
	}

	*has_field = bson_has_field(bson, name);

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_count_keys(bson_t* bson, guint32* count, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!count))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_COUNT_NULL, "count must not be NULL");
		goto _error;
	}

	*count = bson_count_keys(bson);

	return TRUE;

_error:
	return FALSE;
}

/// \todo does more or less the same as j_helper_get_number_string
G_GNUC_UNUSED
static gboolean
j_bson_array_generate_key(guint32 index, const char** key, char* buf, guint buf_length, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!buf))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_BUF_NULL, "buf must not be NULL");
		goto _error;
	}

	bson_uint32_to_string(index, key, buf, buf_length);

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_array(bson_t* bson, const char* key, bson_t* bson_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson || !bson_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!key || !*key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_append_array(bson, key, -1, bson_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_ARRAY_FAILED, "bson append array failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_array_begin(bson_t* bson, const char* key, bson_t* bson_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson || !bson_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!key || !*key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_append_array_begin(bson, key, -1, bson_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_ARRAY_FAILED, "bson append array failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_array_end(bson_t* bson, bson_t* bson_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson || !bson_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_append_array_end(bson, bson_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_ARRAY_FAILED, "bson append array failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_document(bson_t* bson, const char* key, bson_t* bson_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson || !bson_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!key || !*key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_append_document(bson, key, -1, bson_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_DOCUMENT_FAILED, "bson append document failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_document_begin(bson_t* bson, const char* key, bson_t* bson_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson || !bson_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!key || !*key))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_ITER_KEY_NULL, "key must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_append_document_begin(bson, key, -1, bson_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_DOCUMENT_FAILED, "bson append document failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

G_GNUC_UNUSED
static gboolean
j_bson_append_document_end(bson_t* bson, bson_t* bson_child, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	if (G_UNLIKELY(!bson || !bson_child))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		goto _error;
	}

	if (G_UNLIKELY(!bson_append_document_end(bson, bson_child)))
	{
		g_set_error_literal(error, J_BACKEND_BSON_ERROR, J_BACKEND_BSON_ERROR_BSON_APPEND_DOCUMENT_FAILED, "bson append document failed");
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}
