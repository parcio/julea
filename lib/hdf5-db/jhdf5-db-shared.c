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

char*
H5VL_julea_db_buf_to_hex(const char* prefix, const char* buf, guint buf_len)
{
	J_TRACE_FUNCTION(NULL);

	const guint prefix_len = strlen(prefix);
	char* str = g_new(char, buf_len * 2 + 1 + prefix_len);
	unsigned const char* pin = (unsigned const char*)buf;
	unsigned const char* pin_end = pin + buf_len;
	const char* hex = "0123456789ABCDEF";
	char* pout = str;

	memcpy(str, prefix, prefix_len);
	pout += prefix_len;

	while (pin < pin_end)
	{
		*pout++ = hex[(*pin >> 4) & 0xF];
		*pout++ = hex[(*pin++) & 0xF];
	}

	*pout = 0;

	return str;
}

void
H5VL_julea_db_error_handler(GError* error)
{
	J_TRACE_FUNCTION(NULL);

	if (error)
	{
		g_debug("%s %d %s", g_quark_to_string(error->domain), error->code, error->message);
	}
}

JHDF5Object_t*
H5VL_julea_db_object_ref(JHDF5Object_t* object)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(object != NULL, NULL);

	g_atomic_int_inc(&object->ref_count);

	return object;
}

JHDF5Object_t*
H5VL_julea_db_object_new(JHDF5ObjectType type)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object;

	g_return_val_if_fail(type < _J_HDF5_OBJECT_TYPE_COUNT, NULL);

	object = g_new0(JHDF5Object_t, 1);
	object->backend_id = NULL;
	object->backend_id_len = 0;
	object->ref_count = 1;
	object->type = type;

	return object;
}

void
H5VL_julea_db_object_unref(JHDF5Object_t* object)
{
	J_TRACE_FUNCTION(NULL);

	if (object && g_atomic_int_dec_and_test(&object->ref_count))
	{
		switch (object->type)
		{
			case J_HDF5_OBJECT_TYPE_FILE:
				g_free(object->file.name);

				// root group ressources can be directly freed here
				if (object->file.root_group)
				{
					g_free(object->file.root_group->group.name);
					g_free(object->file.root_group->backend_id);
					g_free(object->file.root_group);
				}

				break;
			case J_HDF5_OBJECT_TYPE_DATASET:
				H5VL_julea_db_object_unref(object->dataset.file);
				g_free(object->dataset.name);

				H5VL_julea_db_object_unref(object->dataset.space);
				H5VL_julea_db_object_unref(object->dataset.datatype);

				if (object->dataset.distribution)
				{
					j_distribution_unref(object->dataset.distribution);
				}

				if (object->dataset.object)
				{
					j_distributed_object_unref(object->dataset.object);
				}

				break;
			case J_HDF5_OBJECT_TYPE_ATTR:
				H5VL_julea_db_object_unref(object->attr.file);
				g_free(object->attr.name);

				H5VL_julea_db_object_unref(object->attr.space);
				H5VL_julea_db_object_unref(object->attr.datatype);

				break;
			case J_HDF5_OBJECT_TYPE_GROUP:
				H5VL_julea_db_object_unref(object->group.file);
				g_free(object->group.name);
				break;
			case J_HDF5_OBJECT_TYPE_DATATYPE:
				g_free(object->datatype.data);
				break;
			case J_HDF5_OBJECT_TYPE_SPACE:
				g_free(object->space.data);
				break;
			case _J_HDF5_OBJECT_TYPE_COUNT:
			default:
				g_assert_not_reached();
		}

		g_free(object->backend_id);
		g_free(object);
	}
}

H5I_type_t
H5VL_julea_db_type_intern_to_hdf(JHDF5ObjectType type)
{
	switch (type)
	{
		case J_HDF5_OBJECT_TYPE_FILE:
			return H5I_FILE;
		case J_HDF5_OBJECT_TYPE_DATASET:
			return H5I_DATASET;
		case J_HDF5_OBJECT_TYPE_ATTR:
			return H5I_ATTR;
		case J_HDF5_OBJECT_TYPE_DATATYPE:
			return H5I_DATATYPE;
		case J_HDF5_OBJECT_TYPE_SPACE:
			return H5I_DATASPACE;
		case J_HDF5_OBJECT_TYPE_GROUP:
			return H5I_GROUP;
		case _J_HDF5_OBJECT_TYPE_COUNT:
		default:
			return H5I_BADID;
	}
}
