/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2022 Timm Leon Erxleben
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
test_hdf_object_open_close(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, set, group;
	herr_t error;
	(void)udata;

	J_TEST_TRAP_START;

	space = H5Screate(H5S_SCALAR);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	// check set
	set = H5Dcreate(file, "some_set", H5T_NATIVE_FLOAT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	set = H5Oopen(file, "some_set", H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);
	g_assert_cmpint(H5Iget_type(set), ==, H5I_DATASET);

	error = H5Oclose(set);
	g_assert_cmpint(error, >=, 0);

	// check group
	group = H5Gcreate(file, "some_group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group, !=, H5I_INVALID_HID);

	error = H5Gclose(group);
	g_assert_cmpint(error, >=, 0);

	group = H5Oopen(file, "some_group", H5P_DEFAULT);
	g_assert_cmpint(group, !=, H5I_INVALID_HID);
	g_assert_cmpint(H5Iget_type(group), ==, H5I_GROUP);

	error = H5Oclose(group);
	g_assert_cmpint(error, >=, 0);

	/// \todo check type when implemented

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	g_test_incomplete("object open for kv not implemented!");
}

static void
test_hdf_object_open_invalid(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, attr;
	herr_t error;
	(void)udata;

	J_TEST_TRAP_START;

	space = H5Screate(H5S_SCALAR);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	attr = H5Acreate(file, "some_attr", H5T_NATIVE_FLOAT, space, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(attr, !=, H5I_INVALID_HID);

	error = H5Aclose(attr);
	g_assert_cmpint(error, >=, 0);

	attr = H5Oopen(file, "some_attr", H5P_DEFAULT);
	g_assert_cmpint(attr, ==, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	g_test_incomplete("object open for kv not implemented!");
}

#endif

void
test_hdf_object(void)
{
#ifdef HAVE_HDF5
	if (g_getenv("HDF5_VOL_CONNECTOR") == NULL)
	{
		// Running tests only makes sense for our HDF5 clients
		return;
	}
	g_test_add("/hdf5/object/open_close", hid_t, "o_open.h5", j_test_hdf_file_fixture_setup, test_hdf_object_open_close, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/object/invalid_open", hid_t, "io_open.h5", j_test_hdf_file_fixture_setup, test_hdf_object_open_invalid, j_test_hdf_file_fixture_teardown);

#endif
}
