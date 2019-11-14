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
#ifndef JULEA_DB_HDF5_DATATYPE_C
#define JULEA_DB_HDF5_DATATYPE_C

#include <julea-config.h>
#include <julea.h>
#include <julea-db.h>
#include <julea-object.h>
#include <glib.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"

#include "jhdf5-db.h"
#include "jhdf5-db-shared.c"

#define _GNU_SOURCE

static
JDBSchema* julea_db_schema_datatype_header = NULL;
#define _bswap16(a, b) *(guint16*)b = (((*(const guint16*)a) & 0x00FF) << 8) | (((*(const guint16*)a) & 0xFF00) >> 8)
#define _bswap32(a, b) *(guint32*)b = (((*(const guint32*)a) & 0x000000FF) << 24) | (((*(const guint32*)a) & 0x0000FF00) << 8) | (((*(const guint32*)a) & 0x00FF0000) >> 8) | (((*(const guint32*)a) & 0xFF000000) >> 24)
#define _bswap64(a, b) *(guint64*)b = (((*(const guint64*)a) & 0x00000000000000FFULL) << 56) | (((*(const guint64*)a) & 0x000000000000FF00ULL) << 40) | (((*(const guint64*)a) & 0x0000000000FF0000ULL) << 24) | \
	(((*(const guint64*)a) & 0x00000000FF000000ULL) << 8) | (((*(const guint64*)a) & 0x000000FF00000000ULL) >> 8) | (((*(const guint64*)a) & 0x0000FF0000000000ULL) >> 24) | (((*(const guint64*)a) & 0x00FF000000000000ULL) >> 40) | (((*(const guint64*)a) & 0xFF00000000000000ULL) >> 56)

static
const void*
H5VL_julea_db_datatype_convert_type_change(hid_t type_id_from, hid_t type_id_to, const char* from_buf, char* target_buf, guint count)
{
	J_TRACE_FUNCTION(NULL);

	g_assert(H5Tget_class(type_id_from) == H5Tget_class(type_id_to));
	g_assert(H5Tget_size(type_id_from) == H5Tget_size(type_id_to));
	switch (H5Tget_class(type_id_from))
	{
	case H5T_FLOAT: // no conversion
		break;
	case H5T_INTEGER: // no conversion
		break;
	case H5T_STRING:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_BITFIELD:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_OPAQUE:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_COMPOUND:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_REFERENCE:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_ENUM:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_VLEN:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_ARRAY:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_NO_CLASS:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_TIME:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	case H5T_NCLASSES:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	default:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	}
	return target_buf;
}

static
const void*
H5VL_julea_db_datatype_convert_type(hid_t type_id_from, hid_t type_id_to, const char* from_buf, char* tmp_buf, guint count)
{
	J_TRACE_FUNCTION(NULL);

	if (H5Tequal(type_id_from, type_id_to))
	{
		return from_buf;
	}
	return H5VL_julea_db_datatype_convert_type_change(type_id_from, type_id_to, from_buf, tmp_buf, count);
}
static
herr_t
H5VL_julea_db_datatype_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_datatype_header)
	{
		j_db_schema_unref(julea_db_schema_datatype_header);
	}
	julea_db_schema_datatype_header = NULL;
	return 0;
}
static
herr_t
H5VL_julea_db_datatype_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	if (!(julea_db_schema_datatype_header = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "datatype_header", NULL)))
	{
		j_goto_error();
	}
	if (!(j_db_schema_get(julea_db_schema_datatype_header, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;
				if (julea_db_schema_datatype_header)
				{
					j_db_schema_unref(julea_db_schema_datatype_header);
				}
				if (!(julea_db_schema_datatype_header = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "datatype_header", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_cache", J_DB_TYPE_BLOB, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_class", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_size", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_order", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_precision", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_offset", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_sign", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_ebias", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_norm", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_inpad", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_cset", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_datatype_header, "type_strpad", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_create(julea_db_schema_datatype_header, batch, &error))
				{
					j_goto_error();
				}
				if (!j_batch_execute(batch))
				{
					j_goto_error();
				}
			}
			else
			{
				g_assert_not_reached();
				j_goto_error();
			}
		}
		else
		{
			g_assert_not_reached();
			j_goto_error();
		}
	}
	return 0;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_datatype_term();
	return 1;
}

static
JHDF5Object_t*
H5VL_julea_db_datatype_decode(void* backend_id, guint64 backend_id_len)
{
	J_TRACE_FUNCTION(NULL);

	g_autofree guint32* tmp_uint32 = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	JHDF5Object_t* object = NULL;
	JDBType type;
	guint64 length;

	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_DATATYPE)))
	{
		j_goto_error();
	}
	if (!(object->backend_id = g_new(char, backend_id_len)))
	{
		j_goto_error();
	}
	memcpy(object->backend_id, backend_id, backend_id_len);
	object->backend_id_len = backend_id_len;

	if (!(selector = j_db_selector_new(julea_db_schema_datatype_header, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!(iterator = j_db_iterator_new(julea_db_schema_datatype_header, selector, &error)))
	{
		j_goto_error();
	}
	if (!j_db_iterator_next(iterator, NULL))
	{
		j_goto_error();
	}
	if (!j_db_iterator_get_field(iterator, "type_cache", &type, &object->datatype.data, &object->datatype.data_size, &error))
	{
		j_goto_error();
	}
	if (!j_db_iterator_get_field(iterator, "type_size", &type, (gpointer*)&tmp_uint32, &length, &error))
	{
		j_goto_error();
	}
	object->datatype.type_total_size = *tmp_uint32;
	if (!(object->datatype.hdf5_id = H5Tdecode(object->datatype.data)))
	{
		j_goto_error();
	}
	return object;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);
	return NULL;
}

static
JHDF5Object_t*
H5VL_julea_db_datatype_encode(hid_t* type_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	JHDF5Object_t* object = NULL;
	JDBType type;
	size_t size;
	guint i;
	H5T_class_t clazz;

	g_return_val_if_fail(type_id != NULL, NULL);
	g_return_val_if_fail(*type_id != -1, NULL);

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	//transform to binary
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_DATATYPE)))
	{
		j_goto_error();
	}
	object->datatype.type_total_size = H5Tget_size(*type_id);
	H5Tencode(*type_id, NULL, &size);
	if (!(object->datatype.data = g_new(char, size)))
	{
		j_goto_error();
	}
	H5Tencode(*type_id, object->datatype.data, &size);
	object->datatype.hdf5_id = *type_id;

	//check if this datatype exists
	if (!(selector = j_db_selector_new(julea_db_schema_datatype_header, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "type_cache", J_DB_SELECTOR_OPERATOR_EQ, object->datatype.data, size, &error))
	{
		j_goto_error();
	}
	if (!(iterator = j_db_iterator_new(julea_db_schema_datatype_header, selector, &error)))
	{
		j_goto_error();
	}
	if (j_db_iterator_next(iterator, NULL))
	{
		if (!j_db_iterator_get_field(iterator, "_id", &type, &object->backend_id, &object->backend_id_len, &error))
		{
			j_goto_error();
		}
		goto _done;
	}

	//create new datatype if it did not exist before
	if (!(entry = j_db_entry_new(julea_db_schema_datatype_header, &error)))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "type_cache", object->datatype.data, size, &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "type_size", &object->datatype.type_total_size, sizeof(object->datatype.type_total_size), &error))
	{
		j_goto_error();
	}
	i = clazz = H5Tget_class(*type_id);
	if (!j_db_entry_set_field(entry, "type_class", &i, sizeof(i), &error))
	{
		j_goto_error();
	}
	i = H5Tget_order(*type_id);
	if (!j_db_entry_set_field(entry, "type_order", &i, sizeof(i), &error))
	{
		j_goto_error();
	}
	i = H5Tget_precision(*type_id);
	if (!j_db_entry_set_field(entry, "type_precision", &i, sizeof(i), &error))
	{
		j_goto_error();
	}
	i = H5Tget_offset(*type_id);
	if (!j_db_entry_set_field(entry, "type_offset", &i, sizeof(i), &error))
	{
		j_goto_error();
	}
	if ((clazz != H5T_FLOAT))
	{
		i = H5Tget_sign(*type_id);
		if (!j_db_entry_set_field(entry, "type_sign", &i, sizeof(i), &error))
		{
			j_goto_error();
		}
	}
	if ((clazz != H5T_INTEGER))
	{
		i = H5Tget_ebias(*type_id);
		if (!j_db_entry_set_field(entry, "type_ebias", &i, sizeof(i), &error))
		{
			j_goto_error();
		}
	}
	if ((clazz != H5T_INTEGER))
	{
		i = H5Tget_norm(*type_id);
		if (!j_db_entry_set_field(entry, "type_norm", &i, sizeof(i), &error))
		{
			j_goto_error();
		}
	}
	if ((clazz != H5T_INTEGER))
	{
		i = H5Tget_inpad(*type_id);
		if (!j_db_entry_set_field(entry, "type_inpad", &i, sizeof(i), &error))
		{
			j_goto_error();
		}
	}
	if ((clazz != H5T_INTEGER) && (clazz != H5T_FLOAT))
	{
		i = H5Tget_cset(*type_id);
		if (!j_db_entry_set_field(entry, "type_cset", &i, sizeof(i), &error))
		{
			j_goto_error();
		}
	}
	if ((clazz != H5T_INTEGER) && (clazz != H5T_FLOAT))
	{
		i = H5Tget_strpad(*type_id);
		if (!j_db_entry_set_field(entry, "type_strpad", &i, sizeof(i), &error))
		{
			j_goto_error();
		}
	}
	if (!j_db_entry_insert(entry, batch, &error))
	{
		j_goto_error();
	}
	if (!j_batch_execute(batch))
	{
		j_goto_error();
	}
	if (!j_db_entry_get_id(entry, &object->backend_id, &object->backend_id_len, &error))
	{
		j_goto_error();
	}
_done:
	return object;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);
	return NULL;
}

static
void*
H5VL_julea_db_datatype_commit(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t type_id,
	hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
void*
H5VL_julea_db_datatype_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name,
	hid_t tapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_datatype_get(void* obj, H5VL_datatype_get_t get_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_datatype_specific(void* obj, H5VL_datatype_specific_t specific_type,
	hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_datatype_optional(void* obj, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_datatype_close(void* dt, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}

#pragma GCC diagnostic pop
#endif
