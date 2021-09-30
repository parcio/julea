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
test_hdf_group_create_delete_open_close(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t group, error;
	htri_t ret;

	(void) udata;

	group = H5Gcreate(file, "test_group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group, !=, H5I_INVALID_HID);

	error = H5Gclose(group);
	g_assert_cmpint(error, >=, 0);

	ret = H5Lexists(file, "/test_group", H5P_DEFAULT);
	g_assert_cmpint(ret, >, 0);

	group = H5Gopen(file, "/test_group", H5P_DEFAULT);
	g_assert_cmpint(group, !=, H5I_INVALID_HID);

	error = H5Gclose(group);
	g_assert_cmpint(error, >=, 0);

	error = H5Gunlink(file, "/test_group");
	g_assert_cmpint(error, >=, 0);

	ret = H5Lexists(file, "/test_group", H5P_DEFAULT);
	g_assert_cmpint(ret, ==, 0);
}

static void
test_hdf_link_nested_group(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t group, nested_group, error;
	htri_t ret;

	(void) udata;

	group = H5Gcreate(file, "test_group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group, !=, H5I_INVALID_HID);

	nested_group = H5Gcreate(group, "test_nested", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(nested_group, !=, H5I_INVALID_HID);

	ret = H5Lexists(file, "/test_group/test_nested", H5P_DEFAULT);
	g_assert_cmpint(ret, >, 0);

	ret = H5Lexists(group, "test_nested", H5P_DEFAULT);
	g_assert_cmpint(ret, >, 0);

	error = H5Gclose(group);
	g_assert_cmpint(error, >=, 0);

	error = H5Gclose(nested_group);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_link_hard_delete(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t group1, group2, error;

	(void) udata;

	group1 = H5Gcreate(file, "test_group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group1, !=, H5I_INVALID_HID);

	error = H5Glink(file, H5G_LINK_HARD, "test_group", "test_group2");
	g_assert_cmpint(error, >=, 0);

	group2 = H5Gopen(file, "/test_group2", H5P_DEFAULT);
	g_assert_cmpint(group2, !=, H5I_INVALID_HID);

	error = H5Gclose(group1);
	g_assert_cmpint(error, >=, 0);

	error = H5Gclose(group2);
	g_assert_cmpint(error, >=, 0);

	error = H5Gunlink(file, "/test_group");
	g_assert_cmpint(error, >=, 0);

	group2 = H5Gopen(file, "/test_group2", H5P_DEFAULT);
	g_assert_cmpint(group2, !=, H5I_INVALID_HID);

	error = H5Gclose(group2);
	g_assert_cmpint(error, >=, 0);
}

static void
test_hdf_link_soft(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t group1, group2, error;

	(void) udata;

	group1 = H5Gcreate(file, "test_group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group1, !=, H5I_INVALID_HID);

	error = H5Glink(file, H5G_LINK_SOFT, "test_group", "test_group2");
	g_assert_cmpint(error, >=, 0);

	group2 = H5Gopen(file, "/test_group2", H5P_DEFAULT);
	g_assert_cmpint(group2, !=, H5I_INVALID_HID);

	error = H5Gclose(group1);
	g_assert_cmpint(error, >=, 0);

	error = H5Gclose(group2);
	g_assert_cmpint(error, >=, 0);

	error = H5Gunlink(file, "/test_group");
	g_assert_cmpint(error, >=, 0);

	group2 = H5Gopen(file, "/test_group2", H5P_DEFAULT);
	g_assert_cmpint(group2, ==, H5I_INVALID_HID);
}

static void
test_hdf_link_invalid(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t error;

	(void) udata;

	error = H5Glink(file, H5G_LINK_SOFT, "test_group", "test_group2");
	g_assert_cmpint(error, >=, 0); // creating soft links to non existing objects is allowed

	error = H5Glink(file, H5G_LINK_HARD, "test_group", "test_group2");
	g_assert_cmpint(error, <, 0);
}

static herr_t
it_op(hid_t group, const char *name, const H5L_info2_t *info, void *op_data)
{
	int* count = (int*)op_data;
	(void) info;
	(void) group;
	(void) name;

	++(*count);
	return 0;
}

static void
test_hdf_link_iterate(hid_t* file_fixture, gconstpointer udata)
{
	hid_t file = *file_fixture;
	hid_t group1, group2, error;
	int count = 0;

	(void) udata;

	group1 = H5Gcreate(file, "test_group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group1, !=, H5I_INVALID_HID);

	error = H5Glink(file, H5G_LINK_SOFT, "test_group", "test_group2");
	g_assert_cmpint(error, >=, 0);

	group2 = H5Gcreate(group1, "test_nested", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	g_assert_cmpint(group2, !=, H5I_INVALID_HID);

	error = H5Gclose(group1);
	g_assert_cmpint(error, >=, 0);

	error = H5Gclose(group2);
	g_assert_cmpint(error, >=, 0);

	error = H5Literate(file, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, it_op, &count);
	g_assert_cmpint(error, >=, 0);
	g_assert_cmpint(count, ==, 2);
}

#endif

void
test_hdf_group_link(void)
{
#ifdef HAVE_HDF5
	if (g_getenv("HDF5_VOL_CONNECTOR") == NULL)
	{
		// Running tests only makes sense for our HDF5 clients
		return;
	}

	g_test_add("/hdf5/group-create_delete_open_close", hid_t, "group_open_close.h5", j_test_hdf_file_fixture_setup, test_hdf_group_create_delete_open_close, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/link-nested_group", hid_t, "l_nested.h5", j_test_hdf_file_fixture_setup, test_hdf_link_nested_group, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/link-hard_delete", hid_t, "hardlink_delete.h5", j_test_hdf_file_fixture_setup, test_hdf_link_hard_delete, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/link-soft", hid_t, "softlink_delete.h5", j_test_hdf_file_fixture_setup, test_hdf_link_soft, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/link-invalid", hid_t, "invalid_link.h5", j_test_hdf_file_fixture_setup, test_hdf_link_invalid, j_test_hdf_file_fixture_teardown);
	g_test_add("/hdf5/link-iterate", hid_t, "link_iterate.h5", j_test_hdf_file_fixture_setup, test_hdf_link_iterate, j_test_hdf_file_fixture_teardown);

#endif
}
