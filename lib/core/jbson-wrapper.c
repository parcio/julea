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

#include <core/jbson-wrapper.h>

#include <jtrace-internal.h>

GQuark
j_bson_error_quark(void)
{
	return g_quark_from_static_string("j-bson-error-quark");
}

gboolean
j_bson_iter_init(bson_iter_t* iter, const bson_t* bson, GError** error)
{
	if (bson == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		return FALSE;
	}
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (!bson_iter_init(iter, bson))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INIT, "bson iter init failed");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_iter_next(bson_iter_t* iter, gboolean* has_next, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	*has_next = bson_iter_next(iter);
	return TRUE;
}

gboolean
j_bson_iter_key_equals(bson_iter_t* iter, const char* key, gboolean* equals, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	*equals = !g_strcmp0(bson_iter_key(iter), key);
	return TRUE;
}

const char*
j_bson_iter_key(bson_iter_t* iter, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return NULL;
	}
	return bson_iter_key(iter);
}

gboolean
j_bson_append_value(bson_t* bson, const char* name, JDBType type, JDBType_value* value, GError** error)
{
	switch (type)
	{
	case J_DB_TYPE_SINT32:
		if (!bson_append_int32(bson, name, -1, value->val_sint32))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_UINT32:
		if (!bson_append_int32(bson, name, -1, value->val_uint32))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_SINT64:
		if (!bson_append_int64(bson, name, -1, value->val_sint64))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_UINT64:
		if (!bson_append_int64(bson, name, -1, value->val_uint64))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_FLOAT64:
		if (!bson_append_double(bson, name, -1, value->val_float64))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_FLOAT32:
		if (!bson_append_double(bson, name, -1, value->val_float32))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_STRING:
		if (!bson_append_utf8(bson, name, -1, value->val_string, -1))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
			return FALSE;
		}
		break;
	case J_DB_TYPE_BLOB:
		if (!value->val_blob)
		{
			if (!bson_append_null(bson, name, -1))
			{
				g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				return FALSE;
			}
		}
		else
		{
			if (!bson_append_binary(bson, name, -1, BSON_SUBTYPE_BINARY, (const uint8_t*)value->val_blob, value->val_blob_length))
			{
				g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_APPEND_FAILED, "bson append failed");
				return FALSE;
			}
		}
		break;
	case _J_DB_TYPE_COUNT:
	default:
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_iter_value(bson_iter_t* iter, JDBType type, JDBType_value* value, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	switch (type)
	{
	case J_DB_TYPE_SINT32:
		if (!BSON_ITER_HOLDS_INT32(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_sint32 = bson_iter_int32(iter);
		break;
	case J_DB_TYPE_UINT32:
		if (!BSON_ITER_HOLDS_INT32(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_uint32 = bson_iter_int32(iter);
		break;
	case J_DB_TYPE_FLOAT64:
		if (!BSON_ITER_HOLDS_DOUBLE(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_float64 = bson_iter_double(iter);
		break;
	case J_DB_TYPE_FLOAT32:
		if (!BSON_ITER_HOLDS_DOUBLE(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_float32 = bson_iter_double(iter);
		break;
	case J_DB_TYPE_SINT64:
		if (!BSON_ITER_HOLDS_INT64(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_sint64 = bson_iter_int64(iter);
		break;
	case J_DB_TYPE_UINT64:
		if (!BSON_ITER_HOLDS_INT64(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_uint64 = bson_iter_int64(iter);
		break;
	case J_DB_TYPE_STRING:
		if (!BSON_ITER_HOLDS_UTF8(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		if (value)
			value->val_string = bson_iter_utf8(iter, NULL);
		break;
	case J_DB_TYPE_BLOB:
		if (BSON_ITER_HOLDS_NULL(iter))
		{
			value->val_blob_length = 0;
			value->val_blob = NULL;
			break;
		}
		if (!BSON_ITER_HOLDS_BINARY(iter))
		{
			g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
			return FALSE;
		}
		bson_iter_binary(iter, NULL, &value->val_blob_length, (const uint8_t**)&value->val_blob);
		break;
	case _J_DB_TYPE_COUNT:
	default:
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		return FALSE;
	}
	return TRUE;
}

char*
j_bson_as_json(const bson_t* bson, GError** error)
{
	if (bson == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		return NULL;
	}
	return bson_as_json(bson, NULL);
}

void
j_bson_free_json(char* json)
{
	bson_free(json);
}

gboolean
j_bson_iter_find(bson_iter_t* iter, const char* key, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (!bson_iter_find(iter, key))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_KEY_NOT_FOUND, "bson iter can not find key");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_iter_recurse_array(bson_iter_t* iter, bson_iter_t* iter_child, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (iter_child == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (!BSON_ITER_HOLDS_ARRAY(iter))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		return FALSE;
	}
	if (!bson_iter_recurse(iter, iter_child))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_RECOURSE, "bson iter recourse failed");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_iter_recurse_document(bson_iter_t* iter, bson_iter_t* iter_child, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (iter_child == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (!BSON_ITER_HOLDS_DOCUMENT(iter))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		return FALSE;
	}
	if (!bson_iter_recurse(iter, iter_child))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_RECOURSE, "bson iter recourse failed");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_iter_copy_document(bson_iter_t* iter, bson_t* bson, GError** error)
{
	const uint8_t* data;
	uint32_t length;
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	if (!BSON_ITER_HOLDS_DOCUMENT(iter))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		return FALSE;
	}
	if (bson)
	{
		bson_iter_document(iter, &length, &data);
		bson_init_static(bson, data, length);
	}
	return TRUE;
}

gboolean
j_bson_init_from_json(bson_t* bson, const char* json, GError** error)
{
	if (bson == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		return FALSE;
	}
	if (!bson_init_from_json(bson, json, -1, NULL))
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_INIT_FROM_JSON_FAILED, "bson init from json failes");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_iter_type_db(bson_iter_t* iter, JDBType* type, GError** error)
{
	if (iter == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_NULL, "bson iter must not be NULL");
		return FALSE;
	}
	switch (bson_iter_type(iter))
	{
	case BSON_TYPE_DOUBLE:
		*type = J_DB_TYPE_FLOAT64;
		break;
	case BSON_TYPE_UTF8:
		*type = J_DB_TYPE_STRING;
		break;
	case BSON_TYPE_INT32:
		*type = J_DB_TYPE_UINT32;
		break;
	case BSON_TYPE_INT64:
		*type = J_DB_TYPE_UINT64;
		break;
	case BSON_TYPE_BINARY:
		*type = J_DB_TYPE_BLOB;
		break;
	case BSON_TYPE_NULL:
		*type = J_DB_TYPE_BLOB;
		break;
	case BSON_TYPE_EOD:
	case BSON_TYPE_DOCUMENT:
	case BSON_TYPE_ARRAY:
	case BSON_TYPE_UNDEFINED:
	case BSON_TYPE_OID:
	case BSON_TYPE_BOOL:
	case BSON_TYPE_DATE_TIME:
	case BSON_TYPE_REGEX:
	case BSON_TYPE_DBPOINTER:
	case BSON_TYPE_CODE:
	case BSON_TYPE_SYMBOL:
	case BSON_TYPE_CODEWSCOPE:
	case BSON_TYPE_TIMESTAMP:
	case BSON_TYPE_DECIMAL128:
	case BSON_TYPE_MAXKEY:
	case BSON_TYPE_MINKEY:
	default:
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_ITER_INVALID_TYPE, "bson iter invalid type");
		return FALSE;
	}
	return TRUE;
}

gboolean
j_bson_has_enough_keys(const bson_t* bson, guint32 min_keys, GError** error)
{
	if (bson == NULL)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_NULL, "bson must not be NULL");
		return FALSE;
	}
	if (bson_count_keys(bson) < min_keys)
	{
		g_set_error_literal(error, J_BSON_ERROR, J_BSON_ERROR_BSON_NOT_ENOUGH_KEYS, "bson not enough keys");
		return FALSE;
	}
	return TRUE;
}

void
j_bson_destroy(bson_t* bson)
{
	if (bson)
		bson_destroy(bson);
}
