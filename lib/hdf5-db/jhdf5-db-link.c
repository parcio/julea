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

	/// \todo g_assert (iteartor->'child_type' == child->type)
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

	g_warning("%s called but not implemented!", G_STRFUNC);
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

	g_warning("%s called but not implemented!", G_STRFUNC);
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

	g_warning("%s called but not implemented!", G_STRFUNC);
	return -1;
}

herr_t
H5VL_julea_db_link_get_info_helper(JHDF5Object_t* obj, const H5VL_loc_params_t* loc_params, H5L_info2_t* info_out)
{
	(void)obj;
	(void)loc_params;

	/// \todo needs to be changed when other link types are implemented
	info_out->type = H5L_TYPE_HARD;
	info_out->corder_valid = false;
	info_out->corder = 0;
	info_out->cset = H5T_CSET_UTF8;
	info_out->u.val_size = 0;

	return 0;
}

herr_t
H5VL_julea_db_link_get(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_get_t get_type,
		       hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	H5L_info2_t* info_out;
	herr_t ret = -1;

	(void)obj;
	(void)loc_params;
	(void)get_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	switch (get_type)
	{
		case H5VL_LINK_GET_INFO:
			info_out = va_arg(arguments, H5L_info2_t*);
			ret = H5VL_julea_db_link_get_info_helper(obj, loc_params, info_out);
			break;

		case H5VL_LINK_GET_NAME:
		case H5VL_LINK_GET_VAL:
		default:
			/// \todo implement link get name and get value
			g_warning("%s called but not implemented!", G_STRFUNC);
	}

	return ret;
}

herr_t
H5VL_julea_db_link_iterate_helper(JHDF5Object_t* object, hbool_t recursive, gboolean attr, H5_index_t idx_type,
				  H5_iter_order_t order, hsize_t* idx_p, JHDF5Iterate_Func_t op, void* op_data)
{
	g_autoptr(JDBSelector) link_selector = NULL;
	g_autoptr(JDBIterator) link_iterator = NULL;
	JHDF5Object_t* child_obj = NULL;
	JHDF5Object_t* curr_parent_obj = NULL;
	g_autofree JHDF5ObjectType* child_type = NULL;
	JDBType child_type_type;
	guint64 type_length;
	gchar* child_name = NULL;
	JDBType child_name_type;
	guint64 child_name_length;
	H5L_info2_t link_info;
	hid_t curr_parent = H5I_INVALID_HID;
	H5I_type_t curr_parent_type;
	herr_t ret = -1;

	/// \todo handle index, iter order and interruption

	if (object->type == J_HDF5_OBJECT_TYPE_FILE)
	{
		/// \todo root group needs to be faked here (maybe add it as real group?)
		curr_parent_type = H5I_GROUP;
		curr_parent_obj = H5VL_julea_db_group_root_fake_helper(object);
	}
	else
	{
		curr_parent_type = H5VL_julea_db_type_intern_to_hdf(object->type);
		// reference again since H5Idec_ref will free curr_parent_obj which then will be double freed
		curr_parent_obj = H5VL_julea_db_object_ref(object);
	}

	// register object to obtain hid_t for user op
	if ((curr_parent = H5VLwrap_register(curr_parent_obj, curr_parent_type)) == H5I_INVALID_HID)
	{
		j_goto_error();
	}

	// build selector (parent == backend_id && parent_type == object/file)
	if ((link_selector = j_db_selector_new(julea_db_schema_link, J_DB_SELECTOR_MODE_AND, NULL)) == NULL)
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(link_selector, "parent", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, NULL))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(link_selector, "parent_type", J_DB_SELECTOR_OPERATOR_EQ, &object->type, sizeof(JHDF5ObjectType), NULL))
	{
		j_goto_error();
	}

	if ((link_iterator = j_db_iterator_new(julea_db_schema_link, link_selector, NULL)) == NULL)
	{
		j_goto_error();
	}

	while (j_db_iterator_next(link_iterator, NULL))
	{
		if (!j_db_iterator_get_field(link_iterator, "name", &child_name_type, (gpointer)&child_name, &child_name_length, NULL))
		{
			j_goto_error();
		}

		if (!j_db_iterator_get_field(link_iterator, "child_type", &child_type_type, (gpointer*)&child_type, &type_length, NULL))
		{
			g_free(child_name);
			j_goto_error();
		}

		/// \todo needs to be touched when other link types are implemented
		if (H5VL_julea_db_link_get_info_helper(NULL, NULL, &link_info) < 0)
		{
			g_free(child_name);
			j_goto_error();
		}

		// skip other types when iterating attributes and attributes when iterating other types
		if (attr && *child_type == J_HDF5_OBJECT_TYPE_ATTR)
		{
			/// \todo generate attribute info struct
			if (op.attr_op(curr_parent, child_name, NULL, op_data) < 0)
			{
				g_free(child_name);
				j_goto_error();
			}
		}
		else if (!attr && *child_type != J_HDF5_OBJECT_TYPE_ATTR)
		{
			if (op.iter_op(curr_parent, child_name, &link_info, op_data) < 0)
			{
				g_free(child_name);
				j_goto_error();
			}
		}

		// in case of `attr` this is no error since attributes can not be groups
		if (*child_type == J_HDF5_OBJECT_TYPE_GROUP && recursive)
		{
			// get group object associated with child
			if ((child_obj = H5VL_julea_db_group_open(object, NULL, child_name, 0, 0, NULL)) == NULL)
			{
				g_free(child_name);
				j_goto_error();
			}

			// iterate through child group
			if (H5VL_julea_db_link_iterate_helper(child_obj, recursive, attr, idx_type, order, idx_p, op, op_data) < 0)
			{
				g_free(child_name);
				H5VL_julea_db_object_unref(child_obj);
				j_goto_error();
			}

			H5VL_julea_db_object_unref(child_obj);
		}

		g_free(child_name);
	}

	ret = 0;

_error:
	H5Idec_ref(curr_parent);
	return ret;
}

herr_t
H5VL_julea_db_link_exists_helper(JHDF5Object_t* object, const gchar* name, htri_t* exists)
{
	g_autoptr(JDBSelector) link_selector = NULL;
	g_autoptr(JDBIterator) link_iterator = NULL;
	g_autoptr(GError) err = NULL;
	herr_t ret = -1;

	if ((link_selector = j_db_selector_new(julea_db_schema_link, J_DB_SELECTOR_MODE_AND, NULL)) == NULL)
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(link_selector, "parent", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, NULL))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(link_selector, "parent_type", J_DB_SELECTOR_OPERATOR_EQ, &object->type, sizeof(JHDF5ObjectType), NULL))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(link_selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), NULL))
	{
		j_goto_error();
	}

	if ((link_iterator = j_db_iterator_new(julea_db_schema_link, link_selector, NULL)) == NULL)
	{
		j_goto_error();
	}

	*exists = j_db_iterator_next(link_iterator, &err);

	if (err != NULL)
	{
		j_goto_error();
	}
	else
	{
		ret = 0;
	}

_error:
	return ret;
}

herr_t
H5VL_julea_db_link_specific(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);
	// possible are delete, exists and iterate

	JHDF5Object_t* object = H5VL_julea_db_object_ref((JHDF5Object_t*)obj);

	// arguments for H5VL_LINK_ITER
	hbool_t recursive; // recursivly follow links to subgroups
	H5_index_t idx_type; // index type
	H5_iter_order_t order; // order to iterate over index
	hsize_t* idx_p; // where to start and return where stopped
	JHDF5Iterate_Func_t op; // operation on visited objects
	void* op_data; // arg for operation
	herr_t ret = -1;

	// argument for H5VL_LINK_EXISTS
	htri_t* exists;

	(void)dxpl_id;
	(void)req;

	switch (specific_type)
	{
		case H5VL_LINK_DELETE:
			/// \todo implement link delete
			ret = -1;
			break;

		case H5VL_LINK_EXISTS:
			// sanity check
			g_return_val_if_fail(loc_params->type == H5VL_OBJECT_BY_NAME, -1);
			exists = va_arg(arguments, htri_t*);

			ret = H5VL_julea_db_link_exists_helper(object, loc_params->loc_data.loc_by_name.name, exists);
			break;

		case H5VL_LINK_ITER:
			// get all arguments
			recursive = va_arg(arguments, int);
			idx_type = va_arg(arguments, H5_index_t);
			order = va_arg(arguments, H5_iter_order_t);
			idx_p = va_arg(arguments, hsize_t*);
			op.iter_op = va_arg(arguments, H5L_iterate2_t);
			op_data = va_arg(arguments, void*);

			if (object->type == J_HDF5_OBJECT_TYPE_GROUP || object->type == J_HDF5_OBJECT_TYPE_FILE)
			{
				ret = H5VL_julea_db_link_iterate_helper(object, recursive, false, idx_type, order, idx_p, op, op_data);
			}
			else
			{
				ret = -1;
			}
			break;

		default:
			ret = -1;
	}

	H5VL_julea_db_object_unref(object);
	return ret;
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

	g_warning("%s called but not implemented!", G_STRFUNC);
	return -1;
}
