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

#ifndef JULEA_DB_HDF5_SPACE_C
#define JULEA_DB_HDF5_SPACE_C

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
JDBSchema* julea_db_schema_space_header = NULL;
static
JDBSchema* julea_db_schema_space = NULL;

static
herr_t
H5VL_julea_db_space_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_space_header)
	{
		j_db_schema_unref(julea_db_schema_space_header);
	}
	julea_db_schema_space_header = NULL;
	if (julea_db_schema_space)
	{
		j_db_schema_unref(julea_db_schema_space);
	}
	julea_db_schema_space = NULL;
	return 0;
}
static
herr_t
H5VL_julea_db_space_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(julea_db_schema_space_header = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "space_header", NULL)))
	{
		j_goto_error();
	}
	if (!(j_db_schema_get(julea_db_schema_space_header, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;
				if (julea_db_schema_space_header)
				{
					j_db_schema_unref(julea_db_schema_space_header);
				}
				if (!(julea_db_schema_space_header = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "space_header", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_space_header, "dim_cache", J_DB_TYPE_BLOB, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_space_header, "dim_count", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_space_header, "dim_total_count", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_create(julea_db_schema_space_header, batch, &error))
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
	if (!(julea_db_schema_space = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "space", NULL)))
	{
		j_goto_error();
	}
	if (!(j_db_schema_get(julea_db_schema_space, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;
				if (julea_db_schema_space)
				{
					j_db_schema_unref(julea_db_schema_space);
				}
				if (!(julea_db_schema_space = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "space", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_space, "dim_id", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_space, "dim_index", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_space, "dim_size", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_create(julea_db_schema_space, batch, &error))
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
	H5VL_julea_db_space_term();
	return 1;
}

static
JHDF5Object_t*
H5VL_julea_db_space_decode(void* backend_id, guint64 backend_id_len)
{
	J_TRACE_FUNCTION(NULL);

	g_autofree guint32* tmp_uint32 = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	JHDF5Object_t* object = NULL;
	JDBType type;
	guint64 length;
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_SPACE)))
	{
		j_goto_error();
	}
	if (!(object->backend_id = g_new(char, backend_id_len)))
	{
		j_goto_error();
	}
	memcpy(object->backend_id, backend_id, backend_id_len);
	object->backend_id_len = backend_id_len;
	if (!(selector = j_db_selector_new(julea_db_schema_space_header, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!(iterator = j_db_iterator_new(julea_db_schema_space_header, selector, &error)))
	{
		j_goto_error();
	}
	if (!j_db_iterator_next(iterator, NULL))
	{
		j_goto_error();
	}
	if (!j_db_iterator_get_field(iterator, "dim_cache", &type, &object->space.data, &object->space.data_size, &error))
	{
		j_goto_error();
	}
	if (!j_db_iterator_get_field(iterator, "dim_total_count", &type, (gpointer*)&tmp_uint32, &length, &error))
	{
		j_goto_error();
	}
	object->space.dim_total_count = *tmp_uint32;
	object->space.hdf5_id = H5Sdecode(object->space.data);
	return object;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);
	return NULL;
}

static
JHDF5Object_t*
H5VL_julea_db_space_encode(hid_t* type_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autofree hsize_t* stored_dims = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	gint stored_ndims;
	guint element_count;
	JHDF5Object_t* object = NULL;
	JDBType type;
	size_t size;
	guint i;
	guint j;

	g_return_val_if_fail(type_id != NULL, NULL);
	g_return_val_if_fail(*type_id != -1, NULL);

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	//transform to binary
	{
		J_TRACE("H5Sget_simple_extent_ndims", 0);
		stored_ndims = H5Sget_simple_extent_ndims(*type_id);
	}
	stored_dims = g_new(hsize_t, stored_ndims);
	{
		J_TRACE("H5Sget_simple_extent_dims", 0);
		H5Sget_simple_extent_dims(*type_id, stored_dims, NULL);
	}
	element_count = 1;
	for (i = 0; i < (guint)stored_ndims; i++)
	{
		element_count *= stored_dims[i];
	}
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_SPACE)))
	{
		j_goto_error();
	}
	{
		J_TRACE("H5SencodeNULL", 0);
		H5Sencode(*type_id, NULL, &size);
	}
	if (!(object->space.data = g_new(char, size)))
	{
		j_goto_error();
	}
	{
		J_TRACE("H5Sencode", 0);
		H5Sencode(*type_id, object->space.data, &size);
	}
	object->space.hdf5_id = *type_id;
	object->space.dim_total_count = element_count;

	//check if this space exists
	if (!(selector = j_db_selector_new(julea_db_schema_space_header, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "dim_cache", J_DB_SELECTOR_OPERATOR_EQ, object->space.data, size, &error))
	{
		j_goto_error();
	}
	if (!(iterator = j_db_iterator_new(julea_db_schema_space_header, selector, &error)))
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

	//create new space if it did not exist before
	if (!(entry = j_db_entry_new(julea_db_schema_space_header, &error)))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "dim_cache", object->space.data, size, &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "dim_count", &stored_ndims, sizeof(guint32), &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "dim_total_count", &element_count, sizeof(guint32), &error))
	{
		j_goto_error();
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
	for (i = 0; i < (guint)stored_ndims; i++)
	{
		if (entry)
		{
			j_db_entry_unref(entry);
		}
		entry = NULL;
		if (!(entry = j_db_entry_new(julea_db_schema_space, &error)))
		{
			j_goto_error();
		}
		if (!j_db_entry_set_field(entry, "dim_id", object->backend_id, object->backend_id_len, &error))
		{
			j_goto_error();
		}
		if (!j_db_entry_set_field(entry, "dim_index", &i, sizeof(guint32), &error))
		{
			j_goto_error();
		}
		j = stored_dims[i];
		if (!j_db_entry_set_field(entry, "dim_size", &j, sizeof(guint32), &error))
		{
			j_goto_error();
		}
		if (!j_db_entry_insert(entry, batch, &error))
		{
			j_goto_error();
		}
		if (!j_batch_execute(batch))
		{
			j_goto_error();
		}
	}
_done:
	return object;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);
	return NULL;
}

#pragma GCC diagnostic pop
#endif
