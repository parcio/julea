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

#include "hdf-helper.h"

#ifdef HAVE_HDF5

void
j_test_hdf_file_fixture_setup(hid_t* file, gconstpointer udata)
{
	if (g_test_subprocess())
	{
		const gchar* name = udata;

		*file = H5Fcreate(name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	}
}

void
j_test_hdf_file_fixture_teardown(hid_t* file, gconstpointer udata)
{
	if (g_test_subprocess())
	{
		(void)udata;
		H5Fclose(*file);
		/// \todo Delete is not yet implemented
		// const gchar* name = udata;
		// H5Fdelete(name, H5P_DEFAULT);
	}
}

#endif
