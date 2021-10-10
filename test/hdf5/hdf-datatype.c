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
test_hdf_datatype_create_compound(hid_t* file_fixture, gconstpointer udata)
{
	hid_t compound_type, array_type, another_compound_type;
	hid_t t0, t1, t2, t3; // retrieved types from compound_type
	hid_t file = *file_fixture;
	herr_t error;
	hsize_t dim = 4;

	// example to be build as hdf5 type
	typedef struct example_compound
	{
		int some_int;
		float some_float;
		float some_array[4];
		struct
		{
			int another_int;
			int yet_another_int;
		} some_struct;
	} example_compound;

	(void)udata;

	// create datatypes
	compound_type = H5Tcreate(H5T_COMPOUND, sizeof(example_compound));
	g_assert_cmpint(compound_type, >=, 0);

	array_type = H5Tarray_create2(H5T_NATIVE_FLOAT, 1, &dim);
	g_assert_cmpint(array_type, >=, 0);

	another_compound_type = H5Tcreate(H5T_COMPOUND, 2 * sizeof(int));
	g_assert_cmpint(another_compound_type, >=, 0);

	// fill compounds
	error = H5Tinsert(another_compound_type, "another_int", 0, H5T_NATIVE_INT);
	g_assert_cmpint(error, >=, 0);
	error = H5Tinsert(another_compound_type, "yet_another_int", sizeof(int), H5T_NATIVE_INT);
	g_assert_cmpint(error, >=, 0);

	error = H5Tinsert(compound_type, "some_int", HOFFSET(example_compound, some_int), H5T_NATIVE_INT);
	g_assert_cmpint(error, >=, 0);
	error = H5Tinsert(compound_type, "some_float", HOFFSET(example_compound, some_float), H5T_NATIVE_FLOAT);
	g_assert_cmpint(error, >=, 0);
	error = H5Tinsert(compound_type, "some_array", HOFFSET(example_compound, some_array), array_type);
	g_assert_cmpint(error, >=, 0);
	error = H5Tinsert(compound_type, "some_struct", HOFFSET(example_compound, some_struct), another_compound_type);
	g_assert_cmpint(error, >=, 0);

	// commit type (e.g. save it to file)
	error = H5Tcommit2(file, "/test_dtype", compound_type, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	// refresh type (reopens and loads from file)
	error = H5Trefresh(compound_type);
	g_assert_cmpint(error, >=, 0);

	// check type
	g_assert_cmpint(H5Tget_class(compound_type), ==, H5T_COMPOUND);
	g_assert_cmpint(H5Tget_size(compound_type), ==, sizeof(example_compound));
	g_assert_cmpint(H5Tget_nmembers(compound_type), ==, 4);

	// check members
	t0 = H5Tget_member_type(compound_type, 0);
	g_assert_cmpint(H5Tequal(t0, H5T_NATIVE_INT), >, 0);
	t1 = H5Tget_member_type(compound_type, 1);
	g_assert_cmpint(H5Tequal(t1, H5T_NATIVE_FLOAT), >, 0);
	t2 = H5Tget_member_type(compound_type, 2);
	g_assert_cmpint(H5Tequal(t2, array_type), >, 0);
	t3 = H5Tget_member_type(compound_type, 3);
	g_assert_cmpint(H5Tequal(t3, another_compound_type), >, 0);

	// clean up
	error = H5Tclose(compound_type);
	g_assert_cmpint(error, >=, 0);
	error = H5Tclose(another_compound_type);
	g_assert_cmpint(error, >=, 0);
	error = H5Tclose(array_type);
	g_assert_cmpint(error, >=, 0);

	g_test_incomplete("datatype commit is not yet implemented!");
	/// \todo This causes a critical error and test abortion. Isolation test cases in different processes resolves this.
}

static void
test_hdf_datatype_create_array(hid_t* file_fixture, gconstpointer udata)
{
	hid_t array_type;
	hid_t file = *file_fixture;
	herr_t error;
	hsize_t dim = 4;
	(void)udata;

	array_type = H5Tarray_create2(H5T_NATIVE_FLOAT, 1, &dim);
	g_assert_cmpint(array_type, >=, 0);

	// commit type (e.g. save it to file)
	error = H5Tcommit2(file, "/test_dtype", array_type, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	// refresh type (reopens and loads from file)
	error = H5Trefresh(array_type);
	g_assert_cmpint(error, >=, 0);

	// check type
	g_assert_cmpint(H5Tget_class(array_type), ==, H5T_ARRAY);
	g_assert_cmpint(H5Tget_size(array_type), ==, dim * sizeof(int));

	// clean up
	error = H5Tclose(array_type);
	g_assert_cmpint(error, >=, 0);

	g_test_incomplete("datatype commit is not yet implemented!");
	/// \todo This causes a critical error and test abortion. Isolation test cases in different processes resolves this.
}

#endif

void
test_hdf_datatype(void)
{
#ifdef HAVE_HDF5
	if (g_getenv("HDF5_VOL_CONNECTOR") == NULL)
	{
		// Running tests only makes sense for our HDF5 clients
		return;
	}
	g_test_add("/hdf5/datatype-create_array", hid_t, "array.h5", j_test_hdf_file_fixture_setup, test_hdf_datatype_create_array, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/datatype-create_compound", hid_t, "compound.h5", j_test_hdf_file_fixture_setup, test_hdf_datatype_create_compound, j_test_hdf_file_fixture_teardown);

#endif
}
