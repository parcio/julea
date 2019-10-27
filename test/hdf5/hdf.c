/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2018-2019 Johannes Coym
 * Copyright (C) 2019 Michael Kuhn
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

static void
write_dataset (hid_t file)
{
	hid_t attribute;
	hid_t dataset;
	hid_t dataspace_attr;
	hid_t dataspace_ds;

	hsize_t dims_attr[1];
	hsize_t dims_ds[2];

	int data_attr[3];
	int data_ds[6][7];

	dims_ds[0] = 6;
	dims_ds[1] = 7;
	dataspace_ds = H5Screate_simple(2, dims_ds, NULL);
	dataset = H5Dcreate2(file, "TestDataset", H5T_NATIVE_INT, dataspace_ds, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	for (guint i = 0; i < 6; i++)
	{
		for (guint j = 0; j < 7; j++)
		{
			data_ds[i][j] = i + j;
		}
	}

	H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_ds);

	dims_attr[0] = 3;
	dataspace_attr = H5Screate_simple(1, dims_attr, NULL);
	attribute = H5Acreate2(dataset, "TestAttribute", H5T_NATIVE_INT, dataspace_attr, H5P_DEFAULT, H5P_DEFAULT);

	data_attr[0] = 10;
	data_attr[1] = 20;
	data_attr[2] = 30;

	H5Awrite(attribute, H5T_NATIVE_INT, data_attr);

	H5Sclose(dataspace_attr);
	H5Aclose(attribute);

	H5Sclose(dataspace_ds);
	H5Dclose(dataset);
}

static void
read_dataset (hid_t file)
{
	hid_t attribute;
	hid_t dataset;
	hid_t dataspace_attr;
	hid_t dataspace_ds;
	hid_t datatype;

	hssize_t elements;

	int data_attr[3];
	int data_ds[6][7];

	dataset = H5Dopen2(file, "TestDataset", H5P_DEFAULT);
	attribute = H5Aopen(dataset, "TestAttribute", H5P_DEFAULT);

	dataspace_attr = H5Aget_space(attribute);
	elements = H5Sget_simple_extent_npoints(dataspace_attr);
	g_assert_cmpuint(elements, ==, 3);

	datatype = H5Aget_type(attribute);
	g_assert(H5Tget_class(datatype) == H5T_INTEGER);

	H5Aread(attribute, H5T_NATIVE_INT, data_attr);

	g_assert_cmpint(data_attr[0], ==, 10);
	g_assert_cmpint(data_attr[1], ==, 20);
	g_assert_cmpint(data_attr[2], ==, 30);

	dataspace_ds = H5Dget_space(dataset);
	elements = H5Sget_simple_extent_npoints(dataspace_ds);
	g_assert_cmpuint(elements, ==, 6 * 7);

	datatype = H5Dget_type(dataset);
	g_assert(H5Tget_class(datatype) == H5T_INTEGER);

	H5Dread(dataset, H5T_NATIVE_INT, dataspace_ds, H5S_ALL, H5P_DEFAULT, data_ds);

	for (guint i = 0; i < 6; i++)
	{
		for (guint j = 0; j < 7; j++)
		{
			g_assert_cmpint(data_ds[i][j], ==, i + j);
		}
	}

	H5Aclose(attribute);
	H5Dclose(dataset);
}

static void
test_hdf_read_write (void)
{
	hid_t file;

	file = H5Fcreate("JULEA.h5", H5F_ACC_TRUNC, H5P_DEFAULT, j_hdf5_get_fapl());

	write_dataset(file);
	read_dataset(file);

	H5Fclose(file);
}

#endif

void
test_hdf (void)
{
#ifdef HAVE_HDF5
	g_test_add_func("/hdf5/read_write", test_hdf_read_write);
#endif
}
