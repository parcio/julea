/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2018-2019 Johannes Coym
 * Copyright (C) 2019-2021 Michael Kuhn
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

#include <julea.h>

#include "test.h"

#ifdef HAVE_HDF5

#include <julea-hdf5.h>

#include <hdf5.h>

#include "hdf-helper.h"

static void
create_open_close_delete(hid_t file, H5S_class_t space_type, gchar const *name, hid_t data_type, hsize_t rank, hsize_t* dims, hsize_t* maxdims, hsize_t* chunk_size)
{
	hid_t space, set, dcpl;
	herr_t error;

	if(rank == 0)
	{
		space = H5Screate(space_type);
	}
	else
	{
		space = H5Screate_simple(rank, dims, maxdims);
	}
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	dcpl = H5Pcopy(H5P_DATASET_CREATE_DEFAULT);
	g_assert_cmpint(dcpl, !=, H5I_INVALID_HID);

	if(chunk_size != NULL)
	{
		error = H5Pset_chunk(dcpl, rank, chunk_size);
		g_assert_cmpint(error, >=, 0);
	}

	set = H5Dcreate(file, name, data_type, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	set = H5Dopen(file, name, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Ldelete(file, name, H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_dataset_create_delete_open_close(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hsize_t dims[] = {1,2,3};
	hsize_t unlimitted_dim[] = {H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
	hsize_t chunk_size[] = {3,3,3};
	hsize_t zero_dim[] = {1,0,0};

	(void) udata;

	// null
	create_open_close_delete(file, H5S_NULL, "null", H5T_NATIVE_INT, 0, NULL, NULL, NULL);
	
	// scalar
	create_open_close_delete(file, H5S_SCALAR, "scalar", H5T_NATIVE_INT, 0, NULL, NULL, NULL);
	
	// simple
	create_open_close_delete(file, H5S_SIMPLE, "simple", H5T_NATIVE_INT, 3, dims, dims, NULL);
	
	// simple zero_dim
	create_open_close_delete(file, H5S_SIMPLE, "simple_zero", H5T_NATIVE_INT, 3, zero_dim, zero_dim, NULL);
	
	// chunked	
	create_open_close_delete(file, H5S_SIMPLE, "chunked", H5T_NATIVE_INT, 3, dims, unlimitted_dim, chunk_size);
}

static void
test_hdf_dataset_extend(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set, dcpl, new_space;
	hsize_t dims[] = {1,2,3};
	hsize_t new_dims[] = {30,40,50};
	hsize_t unlimitted_dim[] = {H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
	hsize_t chunk_size[] = {3,3,3};
	hsize_t *loaded_dims = NULL;
	hsize_t *loaded_maxdims = NULL;
	int new_rank;
	herr_t error;

	(void) udata;

	space = H5Screate_simple(3, dims, unlimitted_dim);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	dcpl = H5Pcopy(H5P_DATASET_CREATE_DEFAULT);
	g_assert_cmpint(dcpl, !=, H5I_INVALID_HID);

	error = H5Pset_chunk(dcpl, 3, chunk_size);
	g_assert_cmpint(error, >=, 0);

	set = H5Dcreate(file, "chunked", H5T_NATIVE_INT, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Dset_extent(set, new_dims);
	g_assert_cmpint(error, >=, 0);

	new_space = H5Dget_space(set);
	g_assert_cmpint(new_space, !=, H5I_INVALID_HID);

	new_rank = H5Sget_simple_extent_dims(new_space, loaded_dims, loaded_maxdims);
	g_assert_cmpint(new_rank, ==, 3);
	// g_assert_cmpint(loaded_dims[0], ==, 30);
	// g_assert_cmpint(loaded_dims[1], ==, 40);
	// g_assert_cmpint(loaded_dims[2], ==, 50);
	// g_assert_true(loaded_maxdims[0] == H5S_UNLIMITED && loaded_maxdims[1] == H5S_UNLIMITED && loaded_maxdims[2] == H5S_UNLIMITED);

	free(loaded_maxdims);
	free(loaded_dims);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Ldelete(file, "chunked", H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_dataset_invalid_extend(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set, dcpl;
	hsize_t dims[] = {1,2,3};
	hsize_t new_dims[] = {30,40,50};
	hsize_t max_dims[] = {3, 4, 5};
	hsize_t chunk_size[] = {3,3,3};
	herr_t error;

	(void) udata;

	space = H5Screate_simple(3, dims, max_dims);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	dcpl = H5Pcopy(H5P_DATASET_CREATE_DEFAULT);
	g_assert_cmpint(dcpl, !=, H5I_INVALID_HID);

	error = H5Pset_chunk(dcpl, 3, chunk_size);
	g_assert_cmpint(error, >=, 0);

	set = H5Dcreate(file, "chunked", H5T_NATIVE_INT, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Dset_extent(set, new_dims);
	g_assert_cmpint(error, <, 0);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Ldelete(file, "chunked", H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_dataset_write_read(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set;
	hsize_t rank = 2;
	hsize_t dim[] = {1000,1000};
	g_autofree int *data = NULL;
	herr_t error;

	(void) udata;

	// create dataspace and set
	space = H5Screate_simple(rank, dim, dim);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	set = H5Dcreate(file, "write_read_test", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	// generate data
	data = g_malloc(1000000 * sizeof(int));
	for(int i = 0; i < 999999; ++i) data[i] = i;

	// write data
	error = H5Dwrite(set, H5T_NATIVE_INT, space, space, H5P_DEFAULT, data);
	g_assert_cmpint(error, >=, 0);

	// read data
	error = H5Dread(set, H5T_NATIVE_INT, space, space, H5P_DEFAULT, data);
	g_assert_cmpint(error, >=, 0);

	// check data
	for(int i = 0; i < 999999; ++i) g_assert_cmpint(data[i], ==, i);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_dataset_write_read_selection(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set, hyperslab, mem_space;
	hsize_t rank = 2;
	hsize_t dim[] = {1000, 1000};
	hsize_t start[] = {0, 0};
	hsize_t stride[] = {2, 2};
	hsize_t count[] = {500, 500};
	g_autofree int *data = NULL;
	g_autofree int *write_data = NULL;
	g_autofree int *read_data = NULL;
	herr_t error;

	(void) udata;

	// create dataspace and set
	space = H5Screate_simple(rank, dim, dim);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	set = H5Dcreate(file, "write_read_test", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	// generate data
	data = g_malloc(1000000 * sizeof(int));
	for(int i = 0; i < 999999; ++i) data[i] = 9001;

	// write data
	error = H5Dwrite(set, H5T_NATIVE_INT, space, space, H5P_DEFAULT, data);
	g_assert_cmpint(error, >=, 0);

	// overwrite every second value
	hyperslab = H5Scopy(space);
	g_assert_cmpint(hyperslab, !=, H5I_INVALID_HID);

	error = H5Sselect_hyperslab(hyperslab, H5S_SELECT_SET, start, stride, count, NULL);
	g_assert_cmpint(error, >=, 0);

	write_data = g_malloc(250000 * sizeof(int));
	for(int i = 0; i < 249999; ++i) write_data[i] = 42;

	// space of mem data
	mem_space = H5Screate_simple(rank, count, count);
	g_assert_cmpint(mem_space, !=, H5I_INVALID_HID);

	// scatter write
	error = H5Dwrite(set, H5T_NATIVE_INT, mem_space, hyperslab, H5P_DEFAULT, write_data);
	g_assert_cmpint(error, >=, 0);

	// gather read
	read_data = g_malloc(250000 * sizeof(int));
	error = H5Dread(set, H5T_NATIVE_INT, mem_space, hyperslab, H5P_DEFAULT, read_data);

	// check data
	for(int i = 0; i < 249999; ++i) g_assert_cmpint(read_data[i], ==, 42);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(mem_space);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(hyperslab);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_dataset_write_read_chunked(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set, dcpl;
	hsize_t rank = 2;
	hsize_t dim[] = {1000,1000};
	hsize_t chunk_size[] = {100,100};
	g_autofree int *data = NULL;
	herr_t error;

	(void) udata;

	// create dataspace and set
	space = H5Screate_simple(rank, dim, dim);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	dcpl = H5Pcopy(H5P_DATASET_CREATE_DEFAULT);
	g_assert_cmpint(dcpl, !=, H5I_INVALID_HID);

	error = H5Pset_chunk(dcpl, rank, chunk_size);
	g_assert_cmpint(error, >=, 0);

	set = H5Dcreate(file, "write_read_chunked_test", H5T_NATIVE_INT, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	// generate data
	data = g_malloc(1000000 * sizeof(int));
	for(int i = 0; i < 999999; ++i) data[i] = i;

	// write data
	error = H5Dwrite(set, H5T_NATIVE_INT, space, space, H5P_DEFAULT, data);
	g_assert_cmpint(error, >=, 0);

	// read data
	error = H5Dread(set, H5T_NATIVE_INT, space, space, H5P_DEFAULT, data);
	g_assert_cmpint(error, >=, 0);

	// check data
	for(int i = 0; i < 999999; ++i) g_assert_cmpint(data[i], ==, i);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_dataset_write_read_single(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set, elements, mem_space;
	hsize_t rank = 2;
	hsize_t dim[] = {1000, 1000};
	hsize_t n = 1;
	hsize_t coord[2] = {3,3};
	g_autofree int *data = NULL;
	int write = 42;
	int read = 0;
	herr_t error;

	(void) udata;

	// create dataspace and set
	space = H5Screate_simple(rank, dim, dim);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	set = H5Dcreate(file, "write_read_test", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	// generate data
	data = g_malloc(1000000 * sizeof(int));
	for(int i = 0; i < 999999; ++i) data[i] = 9001;

	// write data
	error = H5Dwrite(set, H5T_NATIVE_INT, space, space, H5P_DEFAULT, data);
	g_assert_cmpint(error, >=, 0);

	// overwrite single value at (3,3)
	elements = H5Scopy(space);
	g_assert_cmpint(elements, !=, H5I_INVALID_HID);

	error = H5Sselect_elements(elements, H5S_SELECT_SET, n, coord);
	g_assert_cmpint(error, >=, 0);

	// space of mem data
	mem_space = H5Screate(H5S_SCALAR);
	g_assert_cmpint(mem_space, !=, H5I_INVALID_HID);

	// scatter write
	error = H5Dwrite(set, H5T_NATIVE_INT, mem_space, elements, H5P_DEFAULT, &write);
	g_assert_cmpint(error, >=, 0);

	// gather read
	error = H5Dread(set, H5T_NATIVE_INT, mem_space, elements, H5P_DEFAULT, &read);

	// check data
	g_assert_cmpint(read, ==, 42);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(mem_space);
	g_assert_cmpint(error, >=, 0);

	error = H5Sclose(elements);
	g_assert_cmpint(error, >=, 0);
}

#endif

void
test_hdf_dataset(void)
{
#ifdef HAVE_HDF5
	if (g_getenv("HDF5_VOL_CONNECTOR") == NULL)
	{
		// Running tests only makes sense for our HDF5 clients
		return;
	}
	g_test_add("/hdf5/dataset-create_delete_open_close", hid_t, "set_open_close.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_create_delete_open_close, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/dataset-extend", hid_t, "set_extend.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_extend, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/dataset-invalid_extend", hid_t, "set_invalid_extend.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_invalid_extend, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/dataset-write_read", hid_t, "set_write_read.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_write_read, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/dataset-write_read_selection", hid_t, "set_write_read_sel.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_write_read_selection, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/dataset-write_read_chunked", hid_t, "set_write_read_chunked.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_write_read_chunked, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/dataset-write_read_single", hid_t, "set_write_read_single.h5", j_test_hdf_file_fixture_setup, test_hdf_dataset_write_read_single, j_test_hdf_file_fixture_teardown);

#endif
}
