/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021-2022 Timm Leon Erxleben
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

void*
H5VL_julea_db_object_open(void* obj, H5VL_loc_params_t* loc_params, H5I_type_t* opened_type, hid_t dxpl, void** req)
{
	JHDF5Object_t* parent = obj;
	JHDF5Object_t* file;
	JHDF5Object_t* child = H5VL_julea_db_object_new(0); // type is not relevant here and will be set by link get

	(void)dxpl;
	(void)req;

	g_return_val_if_fail(loc_params->type == H5VL_OBJECT_BY_NAME, NULL);

	switch (parent->type)
	{
		case J_HDF5_OBJECT_TYPE_FILE:
			file = parent;
			break;
		case J_HDF5_OBJECT_TYPE_GROUP:
			file = parent->group.file;
			break;
		case J_HDF5_OBJECT_TYPE_DATASET:
		case J_HDF5_OBJECT_TYPE_ATTR:
		case J_HDF5_OBJECT_TYPE_DATATYPE:
		case J_HDF5_OBJECT_TYPE_SPACE:
		case _J_HDF5_OBJECT_TYPE_COUNT:
		default:
			g_debug("%s: Unsupported parent type for object open!", G_STRFUNC);
			j_goto_error();
	}

	if (!H5VL_julea_db_link_get_helper(obj, child, loc_params->loc_data.loc_by_name.name))
	{
		g_debug("%s: Could not find object!", G_STRFUNC);
		j_goto_error();
	}

	switch (child->type)
	{
		case J_HDF5_OBJECT_TYPE_GROUP:
			*opened_type = H5I_GROUP;
			child->group.file = H5VL_julea_db_object_ref(file);
			child->group.name = g_strdup(loc_params->loc_data.loc_by_name.name);
			break;

		case J_HDF5_OBJECT_TYPE_DATASET:
			*opened_type = H5I_DATASET;
			child->dataset.file = H5VL_julea_db_object_ref(file);
			child->dataset.name = g_strdup(loc_params->loc_data.loc_by_name.name);
			if (!H5VL_julea_db_dataset_set_info(child, NULL))
			{
				g_debug("%s: Could not set dataset metadata!", G_STRFUNC);
				j_goto_error();
			}
			break;

		case J_HDF5_OBJECT_TYPE_DATATYPE:
			/// \todo implement when committed datatypes are supported
		case J_HDF5_OBJECT_TYPE_FILE:
		case J_HDF5_OBJECT_TYPE_ATTR:
		case J_HDF5_OBJECT_TYPE_SPACE:
		case _J_HDF5_OBJECT_TYPE_COUNT:
		default:
			g_debug("Unsupported type for H5Open()");
			j_goto_error();
	}

	return child;

_error:
	H5VL_julea_db_object_unref(child);
	return NULL;
}