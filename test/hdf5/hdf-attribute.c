/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021 Timm Leon Erxleben
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
create_write_read_delete(hid_t loc, H5S_class_t space_type, gchar const* name, hsize_t name_len, hid_t data_type, hsize_t rank, hsize_t* dims, hsize_t len, void* data)
{
	hid_t space, attribute, error;
	gboolean same = true;
	g_autofree gchar* buf = g_malloc(len);
	g_autofree gchar* name_buf = g_malloc(name_len);

	if (space_type == H5S_SCALAR)
	{
		space = H5Screate(H5S_SCALAR);
		g_assert_cmpint(space, !=, H5I_INVALID_HID);
	}
	else
	{
		space = H5Screate_simple(rank, dims, dims);
		g_assert_cmpint(space, !=, H5I_INVALID_HID);
	}

	attribute = H5Acreate(loc, name, data_type, space, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(attribute, !=, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Awrite(attribute, data_type, data);
	g_assert_cmpint(error, >=, 0);

	error = H5Aclose(attribute);
	g_assert_cmpint(error, >=, 0);

	attribute = H5Aopen(loc, name, H5P_DEFAULT);
	g_assert_cmpint(attribute, !=, H5I_INVALID_HID);

	error = H5Aread(attribute, data_type, buf);
	g_assert_cmpint(error, >=, 0);

	error = H5Aget_name(attribute, name_len, name_buf);
	g_assert_cmpint(error, >=, 0);
	g_assert_cmpstr(name, ==, name_buf);

	error = H5Aclose(attribute);
	g_assert_cmpint(error, >=, 0);

	for (hsize_t i = 0; i < len; ++i)
	{
		same = same && buf[i] == ((gchar*)data)[i];
	}
	g_assert_true(same);

	error = H5Adelete(loc, name);
	g_assert_cmpint(error, >=, 0);

	attribute = H5Aopen(loc, name, H5P_DEFAULT);
	g_assert_cmpint(attribute, ==, H5I_INVALID_HID);
}

static void
test_hdf_attribute_group(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hsize_t dims[] = { 2, 2 };
	const gchar name1[] = "attr_name";
	const gchar name2[] = "attr_name2";
	int data[] = { 42, 43, 44, 45 };
	int sdata = 42;

	(void)udata;

	J_TEST_TRAP_START;

	// using file will attach to root group
	create_write_read_delete(file, H5S_SCALAR, name1, sizeof(name1), H5T_NATIVE_INT, 0, NULL, sizeof(int), (void*)&sdata);
	create_write_read_delete(file, H5S_SIMPLE, name2, sizeof(name2), H5T_NATIVE_INT, 2, dims, 4 * sizeof(int), (void*)data);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_attribute_set(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hsize_t dims[] = { 2, 2 };
	hid_t space, set;
	hsize_t set_dims[] = { 12, 12, 12 };
	const gchar name1[] = "attr_name";
	const gchar name2[] = "attr_name2";
	int data[] = { 42, 43, 44, 45 };
	int sdata = 42;
	herr_t error;

	(void)udata;

	J_TEST_TRAP_START;

	space = H5Screate_simple(3, set_dims, set_dims);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	set = H5Dcreate(file, "set", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(set, !=, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	create_write_read_delete(set, H5S_SCALAR, name1, sizeof(name1), H5T_NATIVE_INT, 0, NULL, sizeof(int), (void*)&sdata);
	create_write_read_delete(set, H5S_SIMPLE, name2, sizeof(name2), H5T_NATIVE_INT, 2, dims, 4 * sizeof(int), (void*)data);

	error = H5Dclose(set);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_attribute_type(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t array_type;
	hsize_t dims[] = { 2, 2 };
	const gchar name1[] = "attr_name";
	const gchar name2[] = "attr_name2";
	hsize_t dim = 4;
	int data[] = { 42, 43, 44, 45 };
	int sdata = 42;
	herr_t error;

	(void)udata;

	J_TEST_TRAP_START;

	array_type = H5Tarray_create2(H5T_NATIVE_FLOAT, 1, &dim);
	g_assert_cmpint(array_type, >=, 0);

	// commit type (e.g. save it to file)
	error = H5Tcommit2(file, "/test_dtype", array_type, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	error = H5Tclose(array_type);
	g_assert_cmpint(error, >=, 0);

	array_type = H5Topen(file, "test_dtype", H5P_DEFAULT);
	g_assert_cmpint(array_type, !=, H5I_INVALID_HID);

	create_write_read_delete(array_type, H5S_SCALAR, name1, sizeof(name1), H5T_NATIVE_INT, 0, NULL, sizeof(int), (void*)&sdata);
	create_write_read_delete(array_type, H5S_SIMPLE, name2, sizeof(name2), H5T_NATIVE_INT, 2, dims, 4 * sizeof(int), (void*)data);

	error = H5Tclose(array_type);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_attribute_invalid_object(void)
{
	hid_t error, attribute, space;
	hsize_t rank = 2;
	hsize_t dims[] = { 2, 2 };

	J_TEST_TRAP_START;

	space = H5Screate_simple(rank, dims, dims);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	attribute = H5Acreate(H5I_INVALID_HID, "name", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(attribute, ==, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;
}

static herr_t
it_op(hid_t attr, const char* name, const H5A_info_t* info, void* op_data)
{
	int* count = (int*)op_data;
	(void)info;
	(void)attr;
	(void)name;

	++(*count);
	return 0;
}

static void
test_hdf_attribute_iterate(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t space, a1, a2, a3, error;
	hsize_t dims[] = { 2, 2 };
	hsize_t count = 0;

	(void)udata;

	J_TEST_TRAP_START;

	space = H5Screate_simple(2, dims, dims);
	g_assert_cmpint(space, !=, H5I_INVALID_HID);

	a1 = H5Acreate(file, "a1", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(a1, !=, H5I_INVALID_HID);

	a2 = H5Acreate(file, "a2", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(a2, !=, H5I_INVALID_HID);

	a3 = H5Acreate(file, "a3", H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(a3, !=, H5I_INVALID_HID);

	error = H5Sclose(space);
	g_assert_cmpint(error, >=, 0);

	error = H5Aclose(a1);
	g_assert_cmpint(error, >=, 0);

	error = H5Aclose(a2);
	g_assert_cmpint(error, >=, 0);

	error = H5Aclose(a3);
	g_assert_cmpint(error, >=, 0);

	error = H5Aiterate(file, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, it_op, &count);
	g_assert_cmpint(error, >=, 0);
	g_assert_cmpint(count, ==, 3);

	J_TEST_TRAP_END;

	j_expect_vol_kv_fail();
}

#endif

void
test_hdf_attribute(void)
{
#ifdef HAVE_HDF5
	if (g_getenv("HDF5_VOL_CONNECTOR") == NULL)
	{
		// Running tests only makes sense for our HDF5 clients
		return;
	}
	g_test_add("/hdf5/attribute/group", hid_t, "attribute_file.h5", j_test_hdf_file_fixture_setup, test_hdf_attribute_group, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/attribute/set", hid_t, "attribute_file.h5", j_test_hdf_file_fixture_setup, test_hdf_attribute_set, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/attribute/type", hid_t, "attribute_file.h5", j_test_hdf_file_fixture_setup, test_hdf_attribute_type, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/attribute/iterate", hid_t, "attribute_file.h5", j_test_hdf_file_fixture_setup, test_hdf_attribute_iterate, j_test_hdf_file_fixture_teardown);
	g_test_add_func("/hdf5/attribute/create_invalid", test_hdf_attribute_invalid_object);

#endif
}
