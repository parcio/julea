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

#ifndef JULEA_DB_HDF5_GROUP_C
#define JULEA_DB_HDF5_GROUP_C

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
#include "jhdf5-db-link.c"

#define _GNU_SOURCE

static
JDBSchema* julea_db_schema_group = NULL;

static
herr_t
H5VL_julea_db_group_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_group)
	{
		j_db_schema_unref(julea_db_schema_group);
	}
	julea_db_schema_group = NULL;
	return 0;
}
static
herr_t
H5VL_julea_db_group_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(julea_db_schema_group = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "group", NULL)))
	{
		j_goto_error();
	}
	if (!(j_db_schema_get(julea_db_schema_group, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;
				if (julea_db_schema_group)
				{
					j_db_schema_unref(julea_db_schema_group);
				}
				if (!(julea_db_schema_group = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "group", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_group, "file", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}
				{
					const gchar* index[] = {
						"file",
						NULL,
					};
					if (!j_db_schema_add_index(julea_db_schema_group, index, &error))
					{
						j_goto_error();
					}
				}
				if (!j_db_schema_create(julea_db_schema_group, batch, &error))
				{
					j_goto_error();
				}
				if (!j_batch_execute(batch))
				{
					j_goto_error();
				}
				if (julea_db_schema_group)
				{
					j_db_schema_unref(julea_db_schema_group);
				}
				if (!(julea_db_schema_group = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "group", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_get(julea_db_schema_group, batch, &error))
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
	return 1;
}
static
herr_t
H5VL_julea_db_group_truncate_file(void* obj)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	JHDF5Object_t* file = obj;

	g_return_val_if_fail(file != NULL, 1);
	g_return_val_if_fail(file->type == J_HDF5_OBJECT_TYPE_FILE, 1);

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(selector = j_db_selector_new(julea_db_schema_group, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!(entry = j_db_entry_new(julea_db_schema_group, &error)))
	{
		j_goto_error();
	}
	if (!j_db_entry_delete(entry, selector, batch, &error))
	{
		j_goto_error();
	}
	if (!j_batch_execute(batch))
	{
		if (!error || error->code != J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS)
		{
			j_goto_error();
		}
	}
	return 0;
_error:
	H5VL_julea_db_error_handler(error);
	return 1;
}
static
void*
H5VL_julea_db_group_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name,
	hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autofree char* hex_buf = NULL;
	JHDF5Object_t* object = NULL;
	JHDF5Object_t* parent = obj;
	JHDF5Object_t* file;

	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(parent != NULL, NULL);

	switch (parent->type)
	{
	case J_HDF5_OBJECT_TYPE_FILE:
		file = parent;
		break;
	case J_HDF5_OBJECT_TYPE_DATASET:
		file = parent->dataset.file;
		break;
	case J_HDF5_OBJECT_TYPE_ATTR:
		file = parent->attr.file;
		break;
	case J_HDF5_OBJECT_TYPE_GROUP:
		file = parent->group.file;
		break;
	case J_HDF5_OBJECT_TYPE_DATATYPE:
	case J_HDF5_OBJECT_TYPE_SPACE:
	case _J_HDF5_OBJECT_TYPE_COUNT:
	default:
		g_assert_not_reached();
		j_goto_error();
	}

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_GROUP)))
	{
		j_goto_error();
	}
	if (!(object->group.name = g_strdup(name)))
	{
		j_goto_error();
	}
	if (!(object->group.file = H5VL_julea_db_object_ref(file)))
	{
		j_goto_error();
	}
	if (!(entry = j_db_entry_new(julea_db_schema_group, &error)))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "file", file->backend_id, file->backend_id_len, &error))
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
	if (!H5VL_julea_db_link_create_helper(parent, object, name))
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
void*
H5VL_julea_db_group_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name,
	hid_t gapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	JHDF5Object_t* object = NULL;
	JHDF5Object_t* parent = obj;
	JHDF5Object_t* file;

	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(parent != NULL, NULL);

	switch (parent->type)
	{
	case J_HDF5_OBJECT_TYPE_FILE:
		file = parent;
		break;
	case J_HDF5_OBJECT_TYPE_DATASET:
		file = parent->dataset.file;
		break;
	case J_HDF5_OBJECT_TYPE_ATTR:
		file = parent->attr.file;
		break;
	case J_HDF5_OBJECT_TYPE_GROUP:
		file = parent->group.file;
		break;
	case J_HDF5_OBJECT_TYPE_DATATYPE:
	case J_HDF5_OBJECT_TYPE_SPACE:
	case _J_HDF5_OBJECT_TYPE_COUNT:
	default:
		g_assert_not_reached();
		j_goto_error();
	}

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_GROUP)))
	{
		j_goto_error();
	}
	if (!(object->group.name = g_strdup(name)))
	{
		j_goto_error();
	}
	if (!(object->group.file = H5VL_julea_db_object_ref(file)))
	{
		j_goto_error();
	}
	if (!H5VL_julea_db_link_get_helper(parent, object, name))
	{
		j_goto_error();
	}
	return object;
_error:
	H5VL_julea_db_object_unref(object);
	return NULL;
}
static
herr_t
H5VL_julea_db_group_get(void* obj, H5VL_group_get_t get_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_GROUP, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_group_specific(void* obj, H5VL_group_specific_t specific_type,
	hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_GROUP, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_group_optional(void* obj, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_GROUP, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_group_close(void* obj, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_GROUP, 1);

	H5VL_julea_db_object_unref(object);
	return 0;
}

#pragma GCC diagnostic pop
#endif
