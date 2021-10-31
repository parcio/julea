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

/// \todo add documentation

#ifndef H5VL_JULEA_DB_H
#define H5VL_JULEA_DB_H

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

/* db schemas */

extern JDBSchema* julea_db_schema_attr;
extern JDBSchema* julea_db_schema_dataset;
extern JDBSchema* julea_db_schema_datatype_header;
extern JDBSchema* julea_db_schema_file;
extern JDBSchema* julea_db_schema_group;
extern JDBSchema* julea_db_schema_link;
extern JDBSchema* julea_db_schema_space_header;
extern JDBSchema* julea_db_schema_space;


/* init and term functions */

herr_t H5VL_julea_db_attr_init(hid_t vipl_id);
herr_t H5VL_julea_db_attr_term(void);
herr_t H5VL_julea_db_dataset_init(hid_t vipl_id);
herr_t H5VL_julea_db_dataset_term(void);
herr_t H5VL_julea_db_datatype_init(hid_t vipl_id);
herr_t H5VL_julea_db_datatype_term(void);
herr_t H5VL_julea_db_file_init(hid_t vipl_id);
herr_t H5VL_julea_db_file_term(void);
herr_t H5VL_julea_db_group_init(hid_t vipl_id);
herr_t H5VL_julea_db_group_term(void);
herr_t H5VL_julea_db_link_init(hid_t vipl_id);
herr_t H5VL_julea_db_link_term(void);
herr_t H5VL_julea_db_space_init(hid_t vipl_id);
herr_t H5VL_julea_db_space_term(void);

/* VOL callbacks */

// attribute
void* H5VL_julea_db_attr_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void** req);
void* H5VL_julea_db_attr_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t aapl_id, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_attr_read(void* obj, hid_t mem_type_id, void* buf, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_attr_write(void* obj, hid_t mem_type_id, const void* buf, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_attr_get(void* obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_attr_specific(void* obj, const H5VL_loc_params_t* loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_attr_optional(void* obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_attr_close(void* obj, hid_t dxpl_id, void** req);

// dataset
void* H5VL_julea_db_dataset_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void** req);
void* H5VL_julea_db_dataset_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t dapl_id, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_dataset_write(void* obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t xfer_plist_id, const void* buf, void** req);
herr_t H5VL_julea_db_dataset_read(void* obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t xfer_plist_id, void* buf, void** req);
herr_t H5VL_julea_db_dataset_get(void* obj, H5VL_dataset_get_t get_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_dataset_specific(void* obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_dataset_optional(void* obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_dataset_close(void* obj, hid_t dxpl_id, void** req);

// datatype
void* H5VL_julea_db_datatype_commit(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void** req);
void* H5VL_julea_db_datatype_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t tapl_id, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_datatype_get(void* obj, H5VL_datatype_get_t get_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_datatype_specific(void* obj, H5VL_datatype_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_datatype_optional(void* obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_datatype_close(void* dt, hid_t dxpl_id, void** req);

// file
void* H5VL_julea_db_file_create(const char* name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void** req);
void* H5VL_julea_db_file_open(const char* name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_file_get(void* obj, H5VL_file_get_t get_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_file_specific(void* obj, H5VL_file_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_file_optional(void* obj, H5VL_file_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_file_close(void* obj, hid_t dxpl_id, void** req);

// group
void* H5VL_julea_db_group_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void** req);
void* H5VL_julea_db_group_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t gapl_id, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_group_get(void* obj, H5VL_group_get_t get_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_group_specific(void* obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_group_optional(void* obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_group_close(void* obj, hid_t dxpl_id, void** req);

// link
herr_t H5VL_julea_db_link_create(H5VL_link_create_type_t create_type, void* obj, const H5VL_loc_params_t* loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void** req, va_list argumenmts);
herr_t H5VL_julea_db_link_copy(void* src_obj, const H5VL_loc_params_t* loc_params1, void* dst_obj, const H5VL_loc_params_t* loc_params2, hid_t lcpl, hid_t lapl, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_link_move(void* src_obj, const H5VL_loc_params_t* loc_params1, void* dst_obj, const H5VL_loc_params_t* loc_params2, hid_t lcpl, hid_t lapl, hid_t dxpl_id, void** req);
herr_t H5VL_julea_db_link_get(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_link_specific(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void** req, va_list arguments);
herr_t H5VL_julea_db_link_optional(void* obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void** req, va_list arguments);

/* internal helper functions */

void H5VL_julea_db_error_handler(GError* error);
char* H5VL_julea_db_buf_to_hex(const char* prefix, const char* buf, guint buf_len);

JHDF5Object_t* H5VL_julea_db_object_new(JHDF5ObjectType type);
JHDF5Object_t* H5VL_julea_db_object_ref(JHDF5Object_t* object);
void H5VL_julea_db_object_unref(JHDF5Object_t* object);

// truncate
herr_t H5VL_julea_db_link_truncate_file(void* obj);
herr_t H5VL_julea_db_attr_truncate_file(void* obj);
herr_t H5VL_julea_db_dataset_truncate_file(void* obj);
herr_t H5VL_julea_db_group_truncate_file(void* obj);

// datatype helper
const void* H5VL_julea_db_datatype_convert_type(hid_t type_id_from, hid_t type_id_to, const char* from_buf, char* tmp_buf, guint count);
JHDF5Object_t* H5VL_julea_db_datatype_decode(void* backend_id, guint64 backend_id_len);
JHDF5Object_t* H5VL_julea_db_datatype_encode(hid_t* type_id);

// space helper
JHDF5Object_t* H5VL_julea_db_space_decode(void* backend_id, guint64 backend_id_len);
JHDF5Object_t* H5VL_julea_db_space_encode(hid_t* type_id);

// link helper
gboolean H5VL_julea_db_link_get_helper(JHDF5Object_t* parent, JHDF5Object_t* child, const char* name);
gboolean H5VL_julea_db_link_create_helper(JHDF5Object_t* parent, JHDF5Object_t* child, const char* name);

#define j_goto_error() \
	do \
	{ \
		G_DEBUG_HERE(); \
		g_debug("goto _error;"); \
		goto _error; \
	} while (0)

#endif
