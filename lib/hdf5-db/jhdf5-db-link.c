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

JDBSchema* julea_db_schema_link = NULL;

herr_t
H5VL_julea_db_link_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_link != NULL)
	{
		j_db_schema_unref(julea_db_schema_link);
		julea_db_schema_link = NULL;
	}

	return 0;
}

herr_t
H5VL_julea_db_link_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	(void)vipl_id;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	if (!(julea_db_schema_link = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "link", NULL)))
	{
		j_goto_error();
	}

	if (!(j_db_schema_get(julea_db_schema_link, batch, &error) && j_batch_execute(batch)))
	{
		if (error != NULL)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;

				if (julea_db_schema_link)
				{
					j_db_schema_unref(julea_db_schema_link);
				}

				if (!(julea_db_schema_link = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "link", NULL)))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_link, "file", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_link, "parent", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_link, "parent_type", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_link, "child", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_link, "child_type", J_DB_TYPE_UINT32, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_link, "name", J_DB_TYPE_STRING, &error))
				{
					j_goto_error();
				}

				{
					const gchar* index[] = {
						"parent",
						"parent_type",
						"name",
						NULL,
					};

					if (!j_db_schema_add_index(julea_db_schema_link, index, &error))
					{
						j_goto_error();
					}
				}

				{
					const gchar* index[] = {
						"file",
						NULL,
					};

					if (!j_db_schema_add_index(julea_db_schema_link, index, &error))
					{
						j_goto_error();
					}
				}

				if (!j_db_schema_create(julea_db_schema_link, batch, &error))
				{
					j_goto_error();
				}

				if (!j_batch_execute(batch))
				{
					j_goto_error();
				}

				if (julea_db_schema_link)
				{
					j_db_schema_unref(julea_db_schema_link);
				}

				if (!(julea_db_schema_link = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "link", NULL)))
				{
					j_goto_error();
				}

				/// \todo Use same key type for every db backend to remove get for every new schema.
				if (!j_db_schema_get(julea_db_schema_link, batch, &error))
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

herr_t
H5VL_julea_db_link_truncate_file(void* obj)
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

	if (!(selector = j_db_selector_new(julea_db_schema_link, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!(entry = j_db_entry_new(julea_db_schema_link, &error)))
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

gboolean
H5VL_julea_db_link_get_helper(JHDF5Object_t* parent, JHDF5Object_t* child, const char* name)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	JHDF5Object_t* file;
	JDBType type;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(parent != NULL, FALSE);
	g_return_val_if_fail(child != NULL, FALSE);

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

	switch (child->type)
	{
		case J_HDF5_OBJECT_TYPE_DATASET:
			g_assert(file == child->dataset.file);
			break;
		case J_HDF5_OBJECT_TYPE_ATTR:
			g_assert(file == child->attr.file);
			break;
		case J_HDF5_OBJECT_TYPE_GROUP:
			g_assert(file == child->group.file);
			break;
		case J_HDF5_OBJECT_TYPE_FILE:
		case J_HDF5_OBJECT_TYPE_DATATYPE:
		case J_HDF5_OBJECT_TYPE_SPACE:
		case _J_HDF5_OBJECT_TYPE_COUNT:
		default:
			g_assert_not_reached();
			j_goto_error();
	}

	if (!(selector = j_db_selector_new(julea_db_schema_link, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "parent", J_DB_SELECTOR_OPERATOR_EQ, parent->backend_id, parent->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "parent_type", J_DB_SELECTOR_OPERATOR_EQ, &parent->type, sizeof(parent->type), &error))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error))
	{
		j_goto_error();
	}

	if (!(iterator = j_db_iterator_new(julea_db_schema_link, selector, &error)))
	{
		j_goto_error();
	}

	if (!j_db_iterator_next(iterator, &error))
	{
		//name must exist for parent before
		j_goto_error();
	}

	if (!j_db_iterator_get_field(iterator, "child", &type, &child->backend_id, &child->backend_id_len, &error))
	{
		j_goto_error();
	}

	//TODO g_assert (iteartor->'child_type' == child->type)
	g_assert(!j_db_iterator_next(iterator, NULL));

	return TRUE;

_error:
	H5VL_julea_db_error_handler(error);

	return FALSE;
}

gboolean
H5VL_julea_db_link_create_helper(JHDF5Object_t* parent, JHDF5Object_t* child, const char* name)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	JHDF5Object_t* file;

	g_return_val_if_fail(name != NULL, FALSE);
	g_return_val_if_fail(parent != NULL, FALSE);
	g_return_val_if_fail(child != NULL, FALSE);

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

	switch (child->type)
	{
		case J_HDF5_OBJECT_TYPE_DATASET:
			g_assert(file == child->dataset.file);
			break;
		case J_HDF5_OBJECT_TYPE_ATTR:
			g_assert(file == child->attr.file);
			break;
		case J_HDF5_OBJECT_TYPE_GROUP:
			g_assert(file == child->group.file);
			break;
		case J_HDF5_OBJECT_TYPE_FILE:
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

	if (!(selector = j_db_selector_new(julea_db_schema_link, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "parent", J_DB_SELECTOR_OPERATOR_EQ, parent->backend_id, parent->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "parent_type", J_DB_SELECTOR_OPERATOR_EQ, &parent->type, sizeof(parent->type), &error))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error))
	{
		j_goto_error();
	}

	if (!(iterator = j_db_iterator_new(julea_db_schema_link, selector, &error)))
	{
		j_goto_error();
	}

	if (j_db_iterator_next(iterator, NULL))
	{
		//name must not exist for parent before
		j_goto_error();
	}

	if (!(entry = j_db_entry_new(julea_db_schema_link, &error)))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "file", file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "parent", parent->backend_id, parent->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "parent_type", &parent->type, sizeof(parent->type), &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "child", child->backend_id, child->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "child_type", &child->type, sizeof(child->type), &error))
	{
		j_goto_error();
	}

	if (!j_db_entry_set_field(entry, "name", name, strlen(name), &error))
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

	return TRUE;

_error:
	H5VL_julea_db_error_handler(error);

	return FALSE;
}

herr_t
H5VL_julea_db_link_create(H5VL_link_create_type_t create_type, void* obj, const H5VL_loc_params_t* loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void** req, va_list argumenmts)
{
	J_TRACE_FUNCTION(NULL);

	(void)create_type;
	(void)obj;
	(void)loc_params;
	(void)lcpl_id;
	(void)lapl_id;
	(void)dxpl_id;
	(void)req;
	(void)argumenmts;

	g_warning("%s called but not implemented!", __func__);
	return -1;
}

herr_t
H5VL_julea_db_link_copy(void* src_obj, const H5VL_loc_params_t* loc_params1, void* dst_obj, const H5VL_loc_params_t* loc_params2, hid_t lcpl, hid_t lapl, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	(void)src_obj;
	(void)loc_params1;
	(void)dst_obj;
	(void)loc_params2;
	(void)lcpl;
	(void)lapl;
	(void)dxpl_id;
	(void)req;

	g_warning("%s called but not implemented!", __func__);
	return -1;
}

herr_t
H5VL_julea_db_link_move(void* src_obj, const H5VL_loc_params_t* loc_params1, void* dst_obj, const H5VL_loc_params_t* loc_params2, hid_t lcpl, hid_t lapl, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	(void)src_obj;
	(void)loc_params1;
	(void)dst_obj;
	(void)loc_params2;
	(void)lcpl;
	(void)lapl;
	(void)dxpl_id;
	(void)req;

	g_warning("%s called but not implemented!", __func__);
	return -1;
}

herr_t
H5VL_julea_db_link_get(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_get_t get_type,
		       hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	(void)obj;
	(void)loc_params;
	(void)get_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_warning("%s called but not implemented!", __func__);
	return -1;
}

static herr_t
H5VL_julea_db_link_iterate(JHDF5Object_t* object, hbool_t recursive, H5_index_t idx_type, 
					H5_iter_order_t order, hsize_t* idx_p, H5L_iterate_t op, void* op_data)
{
	/// \todo handle index and iter order

	// check wether object is file or group

	// query backend id of object

	// build selector (parent == backend_id && parent_type == object/file)

	// iterate over selector -> for every entry get child backend id and child type

	// access child and call op on it

	// is child a group and recursive true? then call this function on it

	// done
}

herr_t
H5VL_julea_db_link_specific(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);
	// possible are delete, exists and iterate

	JHDF5Object_t* object = (JHDF5Object_t*)obj;

	// argument for H5VL_LINK_EXISTS
	htri_t* exists = NULL;

	// arguments for H5VL_LINK_ITER
	hbool_t recursive; // recursivly follow links to subgroups
	H5_index_t idx_type; // index type
	H5_iter_order_t order; // order to iterate over index
	hsize_t* idx_p; // where to start and return where stopped
	H5L_iterate_t op; // operation on visited objects
	void* op_data; // arg for operation
	
	(void)loc_params;
	(void)dxpl_id;
	(void)req;

	switch(specific_type)
	{
		case H5VL_LINK_DELETE:
			/// \todo implement link delete
			return -1;
			break;

		case H5VL_LINK_EXISTS:
			/// \todo implement link exists
			return -1;
			break;

		case H5VL_LINK_ITER:
			// get all arguments
			recursive = va_arg(arguments, hbool_t);
			idx_type = va_arg(arguments, H5_index_t);
			order = va_arg(arguments, H5_iter_order_t);
			idx_p = va_arg(arguments, hsize_t*);
			op = va_arg(arguments, H5L_iterate2_t);
			op_data = va_arg(arguments, void*);

			if(object->type == J_HDF5_OBJECT_TYPE_GROUP || object->type == J_HDF5_OBJECT_TYPE_FILE)
			{
				return H5VL_julea_db_link_iterate(object, recursive, idx_type, order, idx_p, op, op_data);
			}
			else
			{
				return -1;
			}
			break;

		default:
			return -1;
	}
	
}

herr_t
H5VL_julea_db_link_optional(void* obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	(void)obj;
	(void)opt_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_warning("%s called but not implemented!", __func__);
	return -1;
}
