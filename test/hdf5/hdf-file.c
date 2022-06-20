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
test_hdf_file_create_delete(void)
{
	hid_t file;
	herr_t error;

	J_TEST_TRAP_START;

	file = H5Fcreate("test_file.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(file, !=, H5I_INVALID_HID);

	/// \todo maybe implement reopen (part of file specific)
	error = H5Fclose(file);
	g_assert_cmpint(error, >=, 0);

	file = H5Fopen("test_file.h5", H5F_ACC_RDWR, H5P_DEFAULT);
	g_assert_cmpint(file, !=, H5I_INVALID_HID);

	error = H5Fclose(file);
	g_assert_cmpint(error, >=, 0);

	error = H5Fdelete("test_file.h5", H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_file_double_create(void)
{
	hid_t file;
	herr_t error;

	J_TEST_TRAP_START;

	file = H5Fcreate("excl_test_file.h5", H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(file, !=, H5I_INVALID_HID);

	file = H5Fcreate("excl_test_file.h5", H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(file, ==, H5I_INVALID_HID);

	error = H5Fdelete("excl_test_file.h5", H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_file_open_close(void)
{
	hid_t file;
	herr_t error;

	J_TEST_TRAP_START;

	file = H5Fcreate("test_file.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(file, !=, H5I_INVALID_HID);

	error = H5Fclose(file);
	g_assert_cmpint(error, >=, 0);

	file = H5Fopen("test_file.h5", H5F_ACC_RDWR, H5P_DEFAULT);
	g_assert_cmpint(file, !=, H5I_INVALID_HID);

	error = H5Fclose(file);
	g_assert_cmpint(error, >=, 0);

	error = H5Fdelete("test_file.h5", H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_file_open_non_existent(void)
{
	hid_t file;

	J_TEST_TRAP_START;

	file = H5Fopen("non-existent.h5", H5F_ACC_RDWR, H5P_DEFAULT);
	g_assert_cmpint(file, ==, H5I_INVALID_HID);

	J_TEST_TRAP_END;

	j_expect_vol_kv_fail();
}

static void
test_hdf_file_delete(void)
{
	hid_t file;
	herr_t error;

	J_TEST_TRAP_START;

	file = H5Fcreate("test_file.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(file, !=, H5I_INVALID_HID);

	error = H5Fclose(file);
	g_assert_cmpint(error, >=, 0);

	error = H5Fdelete("test_file.h5", H5P_DEFAULT);
	g_assert_cmpint(error, >=, 0);

	file = H5Fopen("test_file.h5", H5F_ACC_RDWR, H5P_DEFAULT);
	g_assert_cmpint(file, ==, H5I_INVALID_HID);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

static void
test_hdf_file_delete_non_existent(void)
{
	herr_t error;

	J_TEST_TRAP_START;

	error = H5Fdelete("non-existent.h5", H5P_DEFAULT);
	g_assert_cmpint(error, <, 0);

	J_TEST_TRAP_END;

	j_expect_vol_db_fail();
	j_expect_vol_kv_fail();
}

#endif

void
test_hdf_file(void)
{
#ifdef HAVE_HDF5
	if (g_getenv("HDF5_VOL_CONNECTOR") == NULL)
	{
		// Running tests only makes sense for our HDF5 clients
		return;
	}
	g_test_add_func("/hdf5/file/create_delete", test_hdf_file_create_delete);
	g_test_add_func("/hdf5/file/double_create", test_hdf_file_double_create);
	g_test_add_func("/hdf5/file/open_close", test_hdf_file_open_close);
	g_test_add_func("/hdf5/file/open_non_existent", test_hdf_file_open_non_existent);
	g_test_add_func("/hdf5/file/delete", test_hdf_file_delete);
	g_test_add_func("/hdf5/file/double_delete", test_hdf_file_delete_non_existent);

#endif
}
