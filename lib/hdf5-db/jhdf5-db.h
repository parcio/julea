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

#ifndef H5VL_JULEA_DB_H
#define H5VL_JULEA_DB_H

#define H5Sencode_vers 1

#include <hdf5.h>
#include <H5PLextern.h>

#define JULEA_HDF5_DB_NAMESPACE "HDF5_DB"

enum JHDF5ObjectType
{
	J_HDF5_OBJECT_TYPE_FILE = 0,
	J_HDF5_OBJECT_TYPE_DATASET,
	J_HDF5_OBJECT_TYPE_ATTR,
	J_HDF5_OBJECT_TYPE_DATATYPE,
	J_HDF5_OBJECT_TYPE_SPACE,
	J_HDF5_OBJECT_TYPE_GROUP,
	_J_HDF5_OBJECT_TYPE_COUNT
};

typedef enum JHDF5ObjectType JHDF5ObjectType;

typedef struct JHDF5Object_t JHDF5Object_t;
struct JHDF5Object_t
{
	gint ref_count;
	JHDF5ObjectType type;
	void* backend_id;
	guint64 backend_id_len;
	union
	{
		struct
		{
			char* name;
		} file;
		struct
		{
			char* name;
			JHDF5Object_t* file;
			JHDF5Object_t* datatype;
			JHDF5Object_t* space;
			JDistribution* distribution;
			JDistributedObject* object;
			struct
			{
				gint64 min_value_i;
				gdouble min_value_f;
				gint64 max_value_i;
				gdouble max_value_f;
			} statistics;
		} dataset;
		struct
		{
			char* name;
			JHDF5Object_t* file;
			JHDF5Object_t* datatype;
			JHDF5Object_t* space;
			JDistribution* distribution;
			JDistributedObject* object;
		} attr;
		struct
		{
			char* name;
			JHDF5Object_t* file;
		} group;
		struct
		{
			void* data;
			size_t data_size;
			guint type_total_size;
			hid_t hdf5_id;
		} datatype;
		struct
		{
			void* data;
			size_t data_size;
			guint dim_total_count;
			hid_t hdf5_id;
		} space;
	};
};

/*internal helper functions*/

static
void
H5VL_julea_db_error_handler(GError* error);
static
char*
H5VL_julea_db_buf_to_hex(const char* prefix, const char* buf, guint buf_len);

static
JHDF5Object_t*
H5VL_julea_db_object_new(JHDF5ObjectType type);
static
JHDF5Object_t*
H5VL_julea_db_object_ref(JHDF5Object_t* object);
static
void
H5VL_julea_db_object_unref(JHDF5Object_t* object);

#define j_goto_error()                   \
	do                               \
	{                                \
		G_DEBUG_HERE();          \
		g_debug("goto _error;"); \
		goto _error;             \
	} while (0)

#endif
