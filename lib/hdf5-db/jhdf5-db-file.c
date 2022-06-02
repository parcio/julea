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

JDBSchema* julea_db_schema_file = NULL;

herr_t
H5VL_julea_db_file_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_file != NULL)
	{
		j_db_schema_unref(julea_db_schema_file);
		julea_db_schema_file = NULL;
	}

	return 0;
}

herr_t
H5VL_julea_db_file_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	(void)vipl_id;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	if (!(julea_db_schema_file = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "file", NULL)))
	{
		j_goto_error();
	}

	if (!(j_db_schema_get(julea_db_schema_file, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;

				if (julea_db_schema_file)
				{
					j_db_schema_unref(julea_db_schema_file);
				}

				if (!(julea_db_schema_file = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "file", NULL)))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_file, "name", J_DB_TYPE_STRING, &error))
				{
					j_goto_error();
				}

				if (!j_db_schema_add_field(julea_db_schema_file, "root_group", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}

				{
					const gchar* index[] = {
						"name",
						NULL,
					};

					if (!j_db_schema_add_index(julea_db_schema_file, index, &error))
					{
						j_goto_error();
					}
				}

				if (!j_db_schema_create(julea_db_schema_file, batch, &error))
				{
					j_goto_error();
				}

				if (!j_batch_execute(batch))
				{
					j_goto_error();
				}

				if (julea_db_schema_file)
				{
					j_db_schema_unref(julea_db_schema_file);
				}

				if (!(julea_db_schema_file = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "file", NULL)))
				{
					j_goto_error();
				}

				/// \todo Use same key type for every db backend to remove get for every new schema.
				if (!j_db_schema_get(julea_db_schema_file, batch, &error))
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
	H5VL_julea_db_file_term();

	g_assert_not_reached();

	return 1;
}

/**
 * \brief Construct the root group for a file object.
 *
 * \param file A file object.
 * \param root_group_id The ID of the respective root group. This function takes the ownership of root_group_id. It should not be freed by the caller.
 * \param root_group_id_len The length of the ID.
 * \attention This function takes the ownership of root_group_id. It should not be freed by the caller.
 * \return gboolean - TRUE for success
 */
static gboolean
file_add_root_group(JHDF5Object_t* file, void* root_group_id, guint64 root_group_id_len)
{
	JHDF5Object_t* root_group;

	g_return_val_if_fail(file != NULL, FALSE);
	g_return_val_if_fail(file->type == J_HDF5_OBJECT_TYPE_FILE, FALSE);

	if (!(root_group = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_GROUP)))
	{
		return FALSE;
	}

	if (!(root_group->group.name = g_strdup("/")))
	{
		g_free(root_group);
		return FALSE;
	}

	// do not change ref count!
	// otherwise the file will never be freed because the user's reference will never be the last one
	root_group->group.file = file;

	root_group->backend_id = root_group_id;
	root_group->backend_id_len = root_group_id_len;

	file->file.root_group = root_group;

	return TRUE;
}

void*
H5VL_julea_db_file_create(const char* name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBSelector) selector_file = NULL;
	void* root_group_id = NULL;
	JHDF5Object_t* object = NULL;
	JDBType type;
	gboolean exist;
	guint64 root_group_id_len;

	(void)fcpl_id;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(name != NULL, NULL);

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	//test for existing file
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_FILE)))
	{
		j_goto_error();
	}

	if (!(object->file.name = g_strdup(name)))
	{
		j_goto_error();
	}

	if (!(selector = j_db_selector_new(julea_db_schema_file, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error))
	{
		j_goto_error();
	}

	if (!(iterator = j_db_iterator_new(julea_db_schema_file, selector, &error)))
	{
		j_goto_error();
	}

	exist = j_db_iterator_next(iterator, NULL);

	if ((flags & H5F_ACC_EXCL) && exist)
	{
		//fail if file exists, and flags define it should not exist
		j_goto_error();
	}

	//create new file
	if (!exist)
	{
		if (!(entry = j_db_entry_new(julea_db_schema_file, &error)))
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

		if (!j_db_entry_get_id(entry, &object->backend_id, &object->backend_id_len, &error))
		{
			j_goto_error();
		}

		// create root group for the file
		// do not use predefined method which would create a link
		j_db_entry_unref(entry);

		if (!(entry = j_db_entry_new(julea_db_schema_group, &error)))
		{
			j_goto_error();
		}

		if (!j_db_entry_set_field(entry, "file", object->backend_id, object->backend_id_len, &error))
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

		if (!j_db_entry_get_id(entry, &root_group_id, &root_group_id_len, &error))
		{
			j_goto_error();
		}

		// update file record with now known root group id
		j_db_entry_unref(entry);

		if (!(entry = j_db_entry_new(julea_db_schema_file, &error)))
		{
			j_goto_error();
		}

		if (!j_db_entry_set_field(entry, "root_group", root_group_id, root_group_id_len, &error))
		{
			j_goto_error();
		}

		if (!(selector_file = j_db_selector_new(julea_db_schema_file, J_DB_SELECTOR_MODE_AND, &error)))
		{
			j_goto_error();
		}

		if (!j_db_selector_add_field(selector_file, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
		{
			j_goto_error();
		}

		if (!j_db_entry_update(entry, selector_file, batch, &error))
		{
			j_goto_error();
		}

		if (!j_batch_execute(batch))
		{
			j_goto_error();
		}

		// construct and store root group object to file struct
		if (!file_add_root_group(object, root_group_id, root_group_id_len))
		{
			j_goto_error();
		}
	}
	else
	{
		if (!j_db_iterator_get_field(iterator, "_id", &type, &object->backend_id, &object->backend_id_len, &error))
		{
			j_goto_error();
		}

		if (!j_db_iterator_get_field(iterator, "root_group", &type, &root_group_id, &root_group_id_len, &error))
		{
			j_goto_error();
		}

		// construct and store root group object to file struct
		if (!file_add_root_group(object, root_group_id, root_group_id_len))
		{
			j_goto_error();
		}

		g_assert(!j_db_iterator_next(iterator, NULL));

		if (flags & H5F_ACC_TRUNC)
		{
			if (H5VL_julea_db_group_truncate_file(object))
			{
				j_goto_error();
			}

			if (H5VL_julea_db_attr_truncate_file(object))
			{
				j_goto_error();
			}

			if (H5VL_julea_db_dataset_truncate_file(object))
			{
				j_goto_error();
			}

			if (H5VL_julea_db_link_truncate_file(object))
			{
				j_goto_error();
			}
		}
	}

	return object;

_error:
	H5VL_julea_db_object_unref(object);
	H5VL_julea_db_error_handler(error);

	return NULL;
}

void*
H5VL_julea_db_file_open(const char* name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	void* root_group_id = NULL;
	JHDF5Object_t* object = NULL;
	guint64 root_group_id_len;
	JDBType type;

	(void)flags;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(name != NULL, NULL);

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_FILE)))
	{
		j_goto_error();
	}

	if (!(object->file.name = g_strdup(name)))
	{
		j_goto_error();
	}

	if (!(selector = j_db_selector_new(julea_db_schema_file, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}

	if (!j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), &error))
	{
		j_goto_error();
	}

	if (!(iterator = j_db_iterator_new(julea_db_schema_file, selector, &error)))
	{
		j_goto_error();
	}

	if (!j_db_iterator_next(iterator, &error))
	{
		j_goto_error();
	}

	if (!j_db_iterator_get_field(iterator, "_id", &type, &object->backend_id, &object->backend_id_len, &error))
	{
		j_goto_error();
	}

	if (!j_db_iterator_get_field(iterator, "root_group", &type, &root_group_id, &root_group_id_len, &error))
	{
		j_goto_error();
	}

	if (!file_add_root_group(object, root_group_id, root_group_id_len))
	{
		j_goto_error();
	}

	g_assert(!j_db_iterator_next(iterator, NULL));

	return object;

_error:
	H5VL_julea_db_object_unref(object);
	H5VL_julea_db_error_handler(error);

	return NULL;
}

herr_t
H5VL_julea_db_file_get(void* obj, H5VL_file_get_t get_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)get_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_FILE, 1);

	g_warning("%s called but not implemented!", G_STRFUNC);
	return -1;
}

herr_t
H5VL_julea_db_file_specific(void* obj, H5VL_file_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)specific_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_FILE, 1);

	g_warning("%s called but not implemented!", G_STRFUNC);
	return -1;
}

herr_t
H5VL_julea_db_file_optional(void* obj, H5VL_file_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)opt_type;
	(void)dxpl_id;
	(void)req;
	(void)arguments;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_FILE, 1);

	g_warning("%s called but not implemented!", G_STRFUNC);
	return -1;
}

herr_t
H5VL_julea_db_file_close(void* obj, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	(void)dxpl_id;
	(void)req;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_FILE, 1);

	H5VL_julea_db_object_unref(object);

	return 0;
}
