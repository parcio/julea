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
#ifndef JULEA_DB_HDF5_DATASET_C
#define JULEA_DB_HDF5_DATASET_C

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
#include "jhdf5-db-datatype.c"
#include "jhdf5-db-space.c"
#include "jhdf5-db-link.c"

#define _GNU_SOURCE

static
JDBSchema* julea_db_schema_dataset = NULL;

static
herr_t
H5VL_julea_db_dataset_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (julea_db_schema_dataset)
	{
		j_db_schema_unref(julea_db_schema_dataset);
	}
	julea_db_schema_dataset = NULL;
	return 0;
}

static
herr_t
H5VL_julea_db_dataset_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(julea_db_schema_dataset = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "dataset", NULL)))
	{
		j_goto_error();
	}
	if (!(j_db_schema_get(julea_db_schema_dataset, batch, &error) && j_batch_execute(batch)))
	{
		if (error)
		{
			if (error->code == J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND)
			{
				g_error_free(error);
				error = NULL;
				if (julea_db_schema_dataset)
				{
					j_db_schema_unref(julea_db_schema_dataset);
				}
				if (!(julea_db_schema_dataset = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "dataset", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "file", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "datatype", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "space", J_DB_TYPE_ID, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "min_value_f", J_DB_TYPE_FLOAT64, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "max_value_f", J_DB_TYPE_FLOAT64, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "min_value_i", J_DB_TYPE_SINT64, &error))
				{
					j_goto_error();
				}
				if (!j_db_schema_add_field(julea_db_schema_dataset, "max_value_i", J_DB_TYPE_SINT64, &error))
				{
					j_goto_error();
				}
				{
					const gchar* index[] = {
						"file",
						NULL,
					};
					if (!j_db_schema_add_index(julea_db_schema_dataset, index, &error))
					{
						j_goto_error();
					}
				}
				if (!j_db_schema_create(julea_db_schema_dataset, batch, &error))
				{
					j_goto_error();
				}
				if (!j_batch_execute(batch))
				{
					j_goto_error();
				}
				if (julea_db_schema_dataset)
				{
					j_db_schema_unref(julea_db_schema_dataset);
				}
				if (!(julea_db_schema_dataset = j_db_schema_new(JULEA_HDF5_DB_NAMESPACE, "dataset", NULL)))
				{
					j_goto_error();
				}
				if (!j_db_schema_get(julea_db_schema_dataset, batch, &error))
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
H5VL_julea_db_dataset_truncate_file(void* obj)
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
	if (!(selector = j_db_selector_new(julea_db_schema_dataset, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!(entry = j_db_entry_new(julea_db_schema_dataset, &error)))
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
H5VL_julea_db_dataset_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name,
	hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
	hid_t dapl_id, hid_t dxpl_id, void** req)
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
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_DATASET)))
	{
		j_goto_error();
	}
	object->dataset.statistics.min_value_i = 0;
	object->dataset.statistics.max_value_i = 0;
	object->dataset.statistics.min_value_f = 0;
	object->dataset.statistics.max_value_f = 0;
	if (!(object->dataset.name = g_strdup(name)))
	{
		j_goto_error();
	}
	if (!(object->dataset.file = H5VL_julea_db_object_ref(file)))
	{
		j_goto_error();
	}
	if (!(object->dataset.datatype = H5VL_julea_db_datatype_encode(&type_id)))
	{
		j_goto_error();
	}
	if (!(object->dataset.space = H5VL_julea_db_space_encode(&space_id)))
	{
		j_goto_error();
	}
	if (!(entry = j_db_entry_new(julea_db_schema_dataset, &error)))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "file", file->backend_id, file->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "datatype", object->dataset.datatype->backend_id, object->dataset.datatype->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "space", object->dataset.space->backend_id, object->dataset.space->backend_id_len, &error))
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
	if (!(object->dataset.distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN)))
	{
		j_goto_error();
	}
	if (!(hex_buf = H5VL_julea_db_buf_to_hex("dataset", object->backend_id, object->backend_id_len)))
	{
		j_goto_error();
	}
	if (!(object->dataset.object = j_distributed_object_new(JULEA_HDF5_DB_NAMESPACE, hex_buf, object->dataset.distribution)))
	{
		j_goto_error();
	}
	j_distributed_object_create(object->dataset.object, batch);
	{
		if (!j_batch_execute(batch))
		{
			j_goto_error();
		}
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
H5VL_julea_db_dataset_open(void* obj, const H5VL_loc_params_t* loc_params, const char* name,
	hid_t dapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autofree char* hex_buf = NULL;
	g_autofree void* space_id_buf = NULL;
	g_autofree void* datatype_id_buf = NULL;
	JHDF5Object_t* object = NULL;
	JHDF5Object_t* parent = obj;
	JHDF5Object_t* file;
	JDBType type;
	guint64 len;
	guint64 space_id_buf_len;
	guint64 datatype_id_buf_len;
	guint64* tmp_ptr_i;
	gdouble* tmp_ptr_f;

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
	if (!(object = H5VL_julea_db_object_new(J_HDF5_OBJECT_TYPE_DATASET)))
	{
		j_goto_error();
	}
	if (!(object->dataset.name = g_strdup(name)))
	{
		j_goto_error();
	}
	if (!(object->dataset.file = H5VL_julea_db_object_ref(file)))
	{
		j_goto_error();
	}
	if (!H5VL_julea_db_link_get_helper(parent, object, name))
	{
		j_goto_error();
	}
	if (!(selector = j_db_selector_new(julea_db_schema_dataset, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!(iterator = j_db_iterator_new(julea_db_schema_dataset, selector, &error)))
	{
		j_goto_error();
	}
	if (!j_db_iterator_next(iterator, &error))
	{
		j_goto_error();
	}
	if (!j_db_iterator_get_field(iterator, "min_value_i", &type, (gpointer*)&tmp_ptr_i, &len, &error))
	{
		j_goto_error();
	}
	object->dataset.statistics.min_value_i = *tmp_ptr_i;
	g_free(tmp_ptr_i);
	if (!j_db_iterator_get_field(iterator, "max_value_i", &type, (gpointer*)&tmp_ptr_i, &len, &error))
	{
		j_goto_error();
	}
	object->dataset.statistics.max_value_i = *tmp_ptr_i;
	g_free(tmp_ptr_i);
	if (!j_db_iterator_get_field(iterator, "min_value_f", &type, (gpointer*)&tmp_ptr_f, &len, &error))
	{
		j_goto_error();
	}
	object->dataset.statistics.min_value_f = *tmp_ptr_f;
	g_free(tmp_ptr_f);
	if (!j_db_iterator_get_field(iterator, "max_value_f", &type, (gpointer*)&tmp_ptr_f, &len, &error))
	{
		j_goto_error();
	}
	object->dataset.statistics.max_value_f = *tmp_ptr_f;
	g_free(tmp_ptr_f);
	if (!j_db_iterator_get_field(iterator, "space", &type, &space_id_buf, &space_id_buf_len, &error))
	{
		j_goto_error();
	}
	if (!(object->dataset.space = H5VL_julea_db_space_decode(space_id_buf, space_id_buf_len)))
	{
		j_goto_error();
	}
	if (!j_db_iterator_get_field(iterator, "datatype", &type, &datatype_id_buf, &datatype_id_buf_len, &error))
	{
		j_goto_error();
	}
	if (!(object->dataset.datatype = H5VL_julea_db_datatype_decode(datatype_id_buf, datatype_id_buf_len)))
	{
		j_goto_error();
	}
	g_assert(!j_db_iterator_next(iterator, NULL));
	if (!(object->dataset.distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN)))
	{
		j_goto_error();
	}
	if (!(hex_buf = H5VL_julea_db_buf_to_hex("dataset", object->backend_id, object->backend_id_len)))
	{
		j_goto_error();
	}
	if (!(object->dataset.object = j_distributed_object_new(JULEA_HDF5_DB_NAMESPACE, hex_buf, object->dataset.distribution)))
	{
		j_goto_error();
	}
	return object;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);
	return NULL;
}
struct JHDF5IndexRange
{
	guint64 start;
	guint64 stop;
};
typedef struct JHDF5IndexRange JHDF5IndexRange;

static
GArray*
H5VL_julea_db_space_hdf5_to_range(hid_t mem_space_id, hid_t stored_space_id)
{
	J_TRACE_FUNCTION(NULL);

	g_autofree hsize_t* stored_dims = NULL;
	JHDF5IndexRange range;
	JHDF5IndexRange range_tmp;
	GArray* range_arr = NULL;
	gboolean range_valid = FALSE;
	gint stored_ndims;
	guint i;
	H5S_sel_type sel_type;
	stored_ndims = H5Sget_simple_extent_ndims(stored_space_id);
	stored_dims = g_new(hsize_t, stored_ndims);
	H5Sget_simple_extent_dims(stored_space_id, stored_dims, NULL);
	range_arr = g_array_new(FALSE, FALSE, sizeof(JHDF5IndexRange));
	G_DEBUG_HERE();
	if (mem_space_id == H5S_ALL)
	{
		sel_type = H5S_SEL_ALL;
	}
	else
	{
		sel_type = H5Sget_select_type(mem_space_id);
	}
	switch (sel_type)
	{
	case H5S_SEL_POINTS:
	{
		hssize_t npoints = 0;
		hssize_t point_current = 0;
		hssize_t point_current_index = 0;
		g_autofree hsize_t* point_current_arr = NULL;
		guint skipsize;

		point_current_arr = g_new(hsize_t, stored_ndims);
		if ((npoints = H5Sget_select_elem_npoints(mem_space_id)) <= 0)
		{
			j_goto_error();
		}
	_start:
		while (point_current_index < npoints)
		{
			if (H5Sget_select_elem_pointlist(mem_space_id, point_current_index, 1, point_current_arr) < 0)
			{
				j_goto_error();
			}
			point_current = point_current_arr[stored_ndims - 1];
			skipsize = stored_dims[stored_ndims - 1];
			for (i = 2; i <= (guint)stored_ndims; i++)
			{
				point_current += point_current_arr[stored_ndims - i] * skipsize;
				skipsize *= stored_dims[stored_ndims - i];
			}
			if (range_valid)
			{
				if ((guint64)point_current == range.stop + 1)
				{
					range.stop++;
				}
				else
				{
					g_array_append_val(range_arr, range);
					range.start = point_current;
					range.stop = point_current;
				}
			}
			else
			{
				range.start = point_current;
				range.stop = point_current;
				range_valid = TRUE;
				goto _start;
			}
			point_current_index++;
		}
		if (range_valid)
		{
			g_array_append_val(range_arr, range);
		}
	}
	break;
	case H5S_SEL_HYPERSLABS:
	{
		g_autofree hsize_t* hyperspab_counters = NULL;
		g_autofree hsize_t* hyperspab_coordinates = NULL;
		g_autofree hsize_t* hyperspab_skip_size = NULL;
		hssize_t hyperslab_count;
		hssize_t hyperslab_index;
		gint current_counter;
		gboolean found;
		gboolean first;

		if ((hyperslab_count = H5Sget_select_hyper_nblocks(mem_space_id)) < 0)
		{
			j_goto_error();
		}
		hyperspab_coordinates = g_new(hsize_t, stored_ndims * 2);
		hyperspab_counters = g_new(hsize_t, stored_ndims);
		hyperspab_skip_size = g_new(hsize_t, stored_ndims);
		hyperspab_skip_size[stored_ndims - 1] = 1;
		for (i = stored_ndims - 1; i > 0; i--)
		{
			hyperspab_skip_size[i - 1] = hyperspab_skip_size[i] * stored_dims[i];
		}
		//iterate over all hyperslabs
		for (hyperslab_index = 0; hyperslab_index < hyperslab_count; hyperslab_index++)
		{
			if (H5Sget_select_hyper_blocklist(mem_space_id, hyperslab_index, 1, hyperspab_coordinates) < 0)
			{
				j_goto_error();
			}

			for (i = 0; i < (guint)stored_ndims; i++)
			{
				hyperspab_counters[i] = hyperspab_coordinates[i];
			}
			current_counter = stored_ndims - 1;
			first = TRUE;
			//within a hyperslab - iterate over the dimensions

			do
			{
				//update current counter
				found = FALSE;
				while (current_counter >= 0)
				{
					if ((hyperspab_counters[current_counter] < hyperspab_coordinates[stored_ndims + current_counter]) || first)
					{
						found = TRUE;
						first = FALSE;
						break;
					}
					for (i = 0; i < (guint)stored_ndims; i++)
					{
						//reset counters
						hyperspab_counters[current_counter] = hyperspab_coordinates[i];
					}
					current_counter--;
				}
				if (!found)
				{
					break;
				}
				//calculate output range
				range_tmp.start = 0;
				for (i = 0; i < (guint)stored_ndims; i++)
				{
					range_tmp.start += hyperspab_counters[i] * hyperspab_skip_size[i];
				}
				range_tmp.stop = range_tmp.start + hyperspab_coordinates[stored_ndims * 2 - 1] - hyperspab_coordinates[stored_ndims - 1];
				if (!range_valid)
				{
					range.start = range_tmp.start;
					range.stop = range_tmp.stop;
					range_valid = TRUE;
				}
				else
				{
					if (range.stop + 1 == range_tmp.start)
					{ //merge consecutive ranges
						range.stop = range_tmp.stop;
					}
					else
					{ //append range if the next has a gap
						g_array_append_val(range_arr, range);
						range.start = range_tmp.start;
						range.stop = range_tmp.stop;
					}
				}
				hyperspab_counters[current_counter]++;
			} while (1);
		}
		if (range_valid)
		{
			g_array_append_val(range_arr, range);
		}
	}
	break;
	case H5S_SEL_ALL:
	{
		range.start = 0;
		range.stop = 1;
		for (i = 0; i < (guint)stored_ndims; i++)
		{
			range.stop *= stored_dims[i];
		}
		g_array_append_val(range_arr, range);
	}
	break;
	case H5S_SEL_N:
	case H5S_SEL_ERROR:
	case H5S_SEL_NONE:
	default:
		g_critical("%s NOT implemented !!", G_STRLOC);
		abort();
	}
	//TODO for speedup: sort the array of ranges and merge if possible
	return range_arr;
_error:
	g_array_free(range_arr, TRUE);
	return NULL;
}

#define calculate_statistics_helper(_buf, _target_extension)                                    \
	do                                                                                      \
	{                                                                                       \
		for (i = 0; i < n; i++)                                                         \
		{                                                                               \
			if (_buf < object->dataset.statistics.min_value##_target_extension)     \
				object->dataset.statistics.min_value##_target_extension = _buf; \
			if (_buf > object->dataset.statistics.max_value##_target_extension)     \
				object->dataset.statistics.max_value##_target_extension = _buf; \
		}                                                                               \
	} while (0)
static
void
calculate_statistics(JHDF5Object_t* object, const void* buf, gsize bytes, hid_t memory_type)
{
	guint i;
	guint n;
	guint element_size = H5Tget_size(memory_type);
	n = bytes / element_size;
	if (H5Tget_class(memory_type) == H5T_FLOAT)
	{
		if (element_size == 4)
		{
			calculate_statistics_helper(((gdouble)((const gfloat*)buf)[i]), _f);
		}
		else if (element_size == 8)
		{
			calculate_statistics_helper(((const gdouble*)buf)[i], _f);
		}
	}
	else if (H5Tget_class(memory_type) == H5T_INTEGER)
	{
		if (H5Tget_sign(memory_type) == H5T_SGN_NONE)
		{
			if (element_size == 1)
			{
				calculate_statistics_helper(((gint8)((const guint8*)buf)[i]), _i);
			}
			else if (element_size == 2)
			{
				calculate_statistics_helper(((gint16)((const guint16*)buf)[i]), _i);
			}
			else if (element_size == 4)
			{
				calculate_statistics_helper(((gint32)((const guint32*)buf)[i]), _i);
			}
			else if (element_size == 8)
			{
				calculate_statistics_helper(((gint64)((const guint64*)buf)[i]), _i);
			}
		}
		else
		{
			if (element_size == 1)
			{
				calculate_statistics_helper(((const gint8*)buf)[i], _i);
			}
			else if (element_size == 2)
			{
				calculate_statistics_helper(((const gint16*)buf)[i], _i);
			}
			else if (element_size == 4)
			{
				calculate_statistics_helper(((const gint32*)buf)[i], _i);
			}
			else if (element_size == 8)
			{
				calculate_statistics_helper(((const gint64*)buf)[i], _i);
			}
		}
	}
}

static
herr_t
H5VL_julea_db_dataset_write(void* obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
	hid_t xfer_plist_id, const void* buf, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autofree void* local_buf_org = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GArray) mem_space_arr = NULL;
	g_autoptr(GArray) file_space_arr = NULL;
	const void* local_buf;
	guint64 bytes_written;
	gsize data_size;
	gsize data_count;
	JHDF5Object_t* object = obj;
	guint mem_space_idx;
	guint file_space_idx;
	JHDF5IndexRange* mem_space_range = NULL;
	JHDF5IndexRange* file_space_range = NULL;
	guint64 current_count1;
	guint64 current_count2;
	guint i;

	g_return_val_if_fail(buf != NULL, 1);
	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_DATASET, 1);

	data_size = object->dataset.datatype->datatype.type_total_size;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}

	if (!(mem_space_arr = H5VL_julea_db_space_hdf5_to_range(mem_space_id, object->dataset.space->space.hdf5_id)))
	{
		j_goto_error();
	}
	if (!(file_space_arr = H5VL_julea_db_space_hdf5_to_range(file_space_id, object->dataset.space->space.hdf5_id)))
	{
		j_goto_error();
	}

	data_count = 0;
	for (i = 0; i < mem_space_arr->len; i++)
	{
		mem_space_range = &g_array_index(mem_space_arr, JHDF5IndexRange, i);
		data_count += mem_space_range->stop - mem_space_range->start;
	}

	local_buf_org = g_new(char, data_size* data_count);

	local_buf = H5VL_julea_db_datatype_convert_type(mem_type_id, object->dataset.datatype->datatype.hdf5_id, buf, local_buf_org, data_count);
	mem_space_idx = 0;
	file_space_idx = 0;
	while ((mem_space_idx < mem_space_arr->len) && (file_space_idx < file_space_arr->len))
	{
		if (mem_space_range == NULL)
		{
			mem_space_range = &g_array_index(mem_space_arr, JHDF5IndexRange, mem_space_idx++);
		}
		if (file_space_range == NULL)
		{
			file_space_range = &g_array_index(file_space_arr, JHDF5IndexRange, file_space_idx++);
		}
		current_count1 = mem_space_range->stop - mem_space_range->start;
		current_count2 = file_space_range->stop - file_space_range->start;
		current_count1 = current_count1 < current_count2 ? current_count1 : current_count2;
		calculate_statistics(object, ((const char*)local_buf) + mem_space_range->start * data_size, data_size * current_count1, mem_type_id);
		j_distributed_object_write(object->dataset.object, ((const char*)local_buf) + mem_space_range->start * data_size, data_size * current_count1, file_space_range->start * data_size, &bytes_written, batch);
		if (mem_space_range->start + current_count1 == mem_space_range->stop)
		{
			mem_space_range = NULL;
		}
		if (file_space_range->start + current_count1 == file_space_range->stop)
		{
			file_space_range = NULL;
		}
	}
	if (!j_batch_execute(batch))
	{
		j_goto_error();
	}
	return 0;
_error:
	return 1;
}
static
herr_t
H5VL_julea_db_dataset_read(void* obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
	hid_t xfer_plist_id, void* buf, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autofree void* local_buf_org = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(GArray) mem_space_arr = NULL;
	g_autoptr(GArray) file_space_arr = NULL;
	const void* local_buf;
	guint64 bytes_read;
	gsize data_size;
	gsize data_count;
	JHDF5Object_t* object = obj;
	guint mem_space_idx;
	guint file_space_idx;
	JHDF5IndexRange* mem_space_range = NULL;
	JHDF5IndexRange* file_space_range = NULL;
	guint64 current_count1;
	guint64 current_count2;
	guint i;

	g_return_val_if_fail(buf != NULL, 1);
	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_DATASET, 1);

	data_size = object->dataset.datatype->datatype.type_total_size;

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(mem_space_arr = H5VL_julea_db_space_hdf5_to_range(mem_space_id, object->dataset.space->space.hdf5_id)))
	{
		j_goto_error();
	}
	if (!(file_space_arr = H5VL_julea_db_space_hdf5_to_range(file_space_id, object->dataset.space->space.hdf5_id)))
	{
		j_goto_error();
	}

	data_count = 0;
	for (i = 0; i < mem_space_arr->len; i++)
	{
		mem_space_range = &g_array_index(mem_space_arr, JHDF5IndexRange, i);
		data_count += mem_space_range->stop - mem_space_range->start;
	}

	local_buf_org = g_new(char, data_size* data_count);

	mem_space_idx = 0;
	file_space_idx = 0;
	while ((mem_space_idx < mem_space_arr->len) && (file_space_idx < file_space_arr->len))
	{
		if (mem_space_range == NULL)
		{
			mem_space_range = &g_array_index(mem_space_arr, JHDF5IndexRange, mem_space_idx++);
		}
		if (file_space_range == NULL)
		{
			file_space_range = &g_array_index(file_space_arr, JHDF5IndexRange, file_space_idx++);
		}
		current_count1 = mem_space_range->stop - mem_space_range->start;
		current_count2 = file_space_range->stop - file_space_range->start;
		current_count1 = current_count1 < current_count2 ? current_count1 : current_count2;
		j_distributed_object_read(object->dataset.object, ((char*)buf) + mem_space_range->start * data_size, data_size * current_count1, file_space_range->start * data_size, &bytes_read, batch);
		if (mem_space_range->start + current_count1 == mem_space_range->stop)
		{
			mem_space_range = NULL;
		}
		if (file_space_range->start + current_count1 == file_space_range->stop)
		{
			file_space_range = NULL;
		}
	}
	if (!j_batch_execute(batch))
	{
		j_goto_error();
	}

	local_buf = H5VL_julea_db_datatype_convert_type(mem_type_id, object->dataset.datatype->datatype.hdf5_id, buf, local_buf_org, data_count);
	if (local_buf != buf)
	{
		memcpy(buf, local_buf, data_size * data_count);
	}
	return 0;
_error:
	return 1;
}
static
herr_t
H5VL_julea_db_dataset_get(void* obj, H5VL_dataset_get_t get_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_DATASET, 1);

	switch (get_type)
	{
	case H5VL_DATASET_GET_SPACE:
		*(va_arg(arguments, hid_t*)) = object->dataset.space->space.hdf5_id;
		break;
	case H5VL_DATASET_GET_TYPE:
		*(va_arg(arguments, hid_t*)) = object->dataset.datatype->datatype.hdf5_id;
		break;
	case H5VL_DATASET_GET_DAPL:
	case H5VL_DATASET_GET_DCPL:
	case H5VL_DATASET_GET_OFFSET:
	case H5VL_DATASET_GET_SPACE_STATUS:
	case H5VL_DATASET_GET_STORAGE_SIZE:
	default:
		g_assert_not_reached();
		exit(1);
	}
	return 0;
}
static
herr_t
H5VL_julea_db_dataset_specific(void* obj, H5VL_dataset_specific_t specific_type,
	hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_DATASET, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_dataset_optional(void* obj, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHDF5Object_t* object = obj;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_DATASET, 1);

	g_critical("%s NOT implemented !!", G_STRLOC);
	abort();
}
static
herr_t
H5VL_julea_db_dataset_close(void* obj, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GError) error = NULL;
	g_autoptr(JBatch) batch = NULL;
	JHDF5Object_t* object = obj;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JDBSelector) selector = NULL;

	g_return_val_if_fail(object->type == J_HDF5_OBJECT_TYPE_DATASET, 1);

	if (!(batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT)))
	{
		j_goto_error();
	}
	if (!(selector = j_db_selector_new(julea_db_schema_dataset, J_DB_SELECTOR_MODE_AND, &error)))
	{
		j_goto_error();
	}
	if (!j_db_selector_add_field(selector, "_id", J_DB_SELECTOR_OPERATOR_EQ, object->backend_id, object->backend_id_len, &error))
	{
		j_goto_error();
	}
	if (!(entry = j_db_entry_new(julea_db_schema_dataset, &error)))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "min_value_i", &object->dataset.statistics.min_value_i, sizeof(object->dataset.statistics.min_value_i), &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "max_value_i", &object->dataset.statistics.max_value_i, sizeof(object->dataset.statistics.max_value_i), &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "min_value_f", &object->dataset.statistics.min_value_f, sizeof(object->dataset.statistics.min_value_f), &error))
	{
		j_goto_error();
	}
	if (!j_db_entry_set_field(entry, "max_value_f", &object->dataset.statistics.max_value_f, sizeof(object->dataset.statistics.max_value_f), &error))
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
	H5VL_julea_db_object_unref(object);
	return 0;
_error:
	H5VL_julea_db_error_handler(error);
	H5VL_julea_db_object_unref(object);
	return 1;
}
#pragma GCC diagnostic pop
#endif
