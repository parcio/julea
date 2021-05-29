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

#include <hdf5.h>
#include <H5PLextern.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <hdf5/jhdf5.h>

#include <julea.h>
#include <julea-db.h>
#include <julea-object.h>

#include "jhdf5-db.h"

static JDBSchema* julea_db_schema_attr = NULL;

static herr_t
H5VL_julea_db_attr_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_attr != NULL)
	{
		j_db_schema_unref(julea_db_schema_attr);
		julea_db_schema_attr = NULL;
	}

	return 0;
}

static herr_t
H5VL_julea_db_attr_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	(void)vipl_id;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	if (!(julea_db_schema_attr = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "attr", NULL)))
	{
		j_goto_error();
	}

	if (!(j_db_schema_get(julea_db_schema_attr, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;

				if (julea_db_schema_attr)
				{
					j_db_schema_unref(julea_db_schema_attr);
				}

				if (!(julea_db_schema_attr = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "attr", NULL)))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_attr, "file", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_attr, "datatype", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_attr, "space", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_attr, "data", J_DB_TYPE_BLOB, &error))
				{
					j_goto_error();
				}

				{
					const gchar* index[] = {
						"file",
						NULL,
					};

					if (!j_db_schema_add_index(julea_db_schema_attr, index, &error))
					{
						j_goto_error();
					}
				}

				if (!j_db_schema_create(julea_db_schema_attr, batch, &error))
				{
					j_goto_error();
				}

				if (!j_batch_execute(batch))
				{
					j_goto_error();
				}

				if (julea_db_schema_attr)
				{
					j_db_schema_unref(julea_db_schema_attr);
				}

				if (!(julea_db_schema_attr = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "attr", NULL)))
				{
					j_goto_error();
				}

				if (!j_db_schema_get(julea_db_schema_attr, batch, &error))
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

static herr_t
H5VL_julea_db_attr_truncate_file(void* obj)
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

	if (!(selector = j_db_selector_new(julea_db_schema_attr, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!(entry = j_db_entry_new(julea_db_schema_attr, &error)))
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

static void*
H5VL_julea_db_attr_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	JHDF5Object_t* object = NULL;
	JHDF5Object_t* parent = obj;
	JHDF5Object_t* file;
	gchar dummy[1];

	(void)loc_params;
	(void)acpl_id;
	(void)aapl_id;
	(void)dxpl_id;
	(void)req;

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

	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_ATTR)))
	{
		j_goto_error();
	}

	if (!(object->attr.name = g_strdup(name)))
	{
		j_goto_error();
	}

	if (!(object->attr.file = H5VL_julea_db_object_ref(file)))
	{
		j_goto_error();
	}

	if (!(object->attr.datatype = H5VL_julea_db_datatype_encode(&type_id)))
	{
		j_goto_error();
	}

	if (!(object->attr.space = H5VL_julea_db_space_encode(&space_id)))
	{
		j_goto_error();
	}

	if (!(entry = j_db_entry_new(julea_db_schema_attr, &error)))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "file", file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "datatype", object->attr.datatype->backend_id, object->attr.datatype->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "space", object->attr.space->backend_id, object->attr.space->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "data", dummy, 0, &error))
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

	j_hdf5_log(file->file.name, "a", 'C', NULL, object, parent);
	return object;

_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);

	return NULL;
}

static void*
H5VL_julea_db_attr_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t aapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autofree void* space_id_buf = NULL;
	g_autofree void* datatype_id_buf = NULL;
	JHDF5Object_t* object = NULL;
	JHDF5Object_t* parent = obj;
	JHDF5Object_t* file;
	JDBType type;
	guint64 space_id_buf_len;
	guint64 datatype_id_buf_len;

	(void)loc_params;
	(void)aapl_id;
	(void)dxpl_id;
	(void)req;

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

	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_ATTR)))
	{
		j_goto_error();
	}

	if (!(object->attr.name = g_strdup(name)))
	{
		j_goto_error();
	}

	if (!(object->attr.file = H5VL_julea_db_object_ref(file)))
	{
		j_goto_error();
	}

	if (!H5VL_julea_db_link_get_helper(parent, object, name))
	{
		j_goto_error();
	}

	if (!(selector = j_db_selector_new(julea_db_schema_attr, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!(iterator = j_db_iterator_new(julea_db_schema_attr, selector, &error)))
	{
		j_goto_error();
	}

	if (!j_db_iterator_next(iterator, &error))
	{
		j_goto_error();
	}

	if (!j_db_iterator_get_field(iterator, "space", &type, &space_id_buf, &space_id_buf_len, &error))
	{
		j_goto_error();
	}

	if (!(object->attr.space = H5VL_julea_db_space_decode(space_id_buf, space_id_buf_len)))
	{
		j_goto_error();
	}

	if (!j_db_iterator_get_field(iterator, "datatype", &type, &datatype_id_buf, &datatype_id_buf_len, &error))
	{
		j_goto_error();
	}

	if (!(object->attr.datatype = H5VL_julea_db_datatype_decode(datatype_id_buf, datatype_id_buf_len)))
	{
		j_goto_error();
	}

	g_assert(!j_db_iterator_next(iterator, NULL));

	j_hdf5_log(file->file.name, "a", 'O', NULL, object, parent);
	return object;

_error:

	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);

	return NULL;
}

static herr_t
H5VL_julea_db_attr_read(void* obj, hid_t mem_type_id, void* buf, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	guint64 bytes_read;
	gsize data_size;
	g_autofree gpointer tmp = NULL;
	JHDF5Object_t* object = obj;
	JDBType type;

	(void)mem_type_id;
	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(buf != NULL, 1);
	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_ATTR, 1);

	bytes_read = 0;
	data_size = object->dataset.datatype->datatype.type_total_size;
	data_size *= object->attr.space->space.dim_total_count;

	if (!(selector = j_db_selector_new(julea_db_schema_attr, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!(iterator = j_db_iterator_new(julea_db_schema_attr, selector, &error)))
	{
		j_goto_error();
	}

	if (!j_db_iterator_next(iterator, &error))
	{
		j_goto_error();
	}

	if (!j_db_iterator_get_field(iterator, "data", &type, &tmp, &bytes_read, &error))
	{
		j_goto_error();
	}

	g_assert(!j_db_iterator_next(iterator, NULL));
	g_assert_cmpuint(data_size, ==, bytes_read);

	memcpy(buf, tmp, bytes_read);

	j_hdf5_log(object->attr.file->file.name, "a", 'R', NULL, object, NULL);
	return 0;

_error:
	return 1;
}

static herr_t
H5VL_julea_db_attr_write(void* obj, hid_t mem_type_id, const void* buf, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	gsize data_size;
	JHDF5Object_t* object = obj;

	(void)mem_type_id;
	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(buf != NULL, 1);
	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_ATTR, 1);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	data_size = object->dataset.datatype->datatype.type_total_size;
	data_size *= object->attr.space->space.dim_total_count;

	if (!(selector = j_db_selector_new(julea_db_schema_attr, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!(entry = j_db_entry_new(julea_db_schema_attr, &error)))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "file", object->attr.file->backend_id, object->attr.file->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "datatype", object->attr.datatype->backend_id, object->attr.datatype->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "space", object->attr.space->backend_id, object->attr.space->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "data", buf, data_size, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_update(entry, selector, batch, &error))
	{
		j_goto_error();
	}

	if (!j_batch_execute(batch))
	{
		j_goto_error();
	}

	j_hdf5_log(object->attr.file->file.name, "a", 'W', NULL, object, NULL);
	return 0;

_error:
	return 1;
}

static herr_t
H5VL_julea_db_attr_get(void* obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_ATTR, 1);

	switch (get_type)
	{
		case H5VL_ATTR_GET_SPACE:
			*(va_arg(arguments, hid_t*)) = object->attr.space->space.hdf5_id;
			break;
		case H5VL_ATTR_GET_TYPE:
			*(va_arg(arguments, hid_t*)) = object->attr.datatype->datatype.hdf5_id;
			break;
		case H5VL_ATTR_GET_ACPL:
		case H5VL_ATTR_GET_INFO:
		case H5VL_ATTR_GET_NAME:
		case H5VL_ATTR_GET_STORAGE_SIZE:
		default:
			g_assert_not_reached();
	}

	j_hdf5_log(object->attr.file->file.name, "a", 'G', NULL, object, NULL);
	return 0;
}

static herr_t
H5VL_julea_db_attr_specific(void* obj, const H5VL_loc_params_t* loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)loc_params;
	(void)specific_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_ATTR, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	g_assert_not_reached();
}

static herr_t
H5VL_julea_db_attr_optional(void* obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)opt_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_ATTR, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	g_assert_not_reached();
}

static herr_t
H5VL_julea_db_attr_close(void* obj, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_ATTR, 1);

	j_hdf5_log(object->attr.file->file.name, "a", 'S', NULL, object, NULL);
	H5VL_julea_db_object_unref(object);

	return 0;
}
