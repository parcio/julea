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

#include "benchmark.h"

#ifdef HAVE_HDF5

#include <hdf5.h>
#include <H5PLextern.h>

static void
write_attribute (hid_t file)
{
	hid_t attribute;
	hid_t dataspace;
	hid_t group;

	hsize_t dims[1];

	int data[1024];

	group = H5Gopen2(file, "/", H5P_DEFAULT);

	dims[0] = 1024;
	dataspace = H5Screate_simple(1, dims, NULL);
	attribute = H5Acreate2(group, "BenchmarkAttribute", H5T_NATIVE_INT, dataspace, H5P_DEFAULT, H5P_DEFAULT);

	for (guint i = 0; i < 1024; i++)
	{
		data[i] = i * 10;
	}

	H5Awrite(attribute, H5T_NATIVE_INT, data);

	H5Sclose(dataspace);
	H5Aclose(attribute);
	H5Gclose(group);
}

static void
read_attribute (hid_t file)
{
	hid_t attribute;
	hid_t dataspace;
	hid_t group;

	hssize_t elements;

	int data[1024];

	group = H5Gopen2(file, "/", H5P_DEFAULT);
	attribute = H5Aopen(group, "BenchmarkAttribute", H5P_DEFAULT);

	dataspace = H5Aget_space(attribute);
	elements = H5Sget_simple_extent_npoints(dataspace);
	g_assert_cmpuint(elements, ==, 1024);

	H5Aread(attribute, H5T_NATIVE_INT, data);

	for (guint i = 0; i < 1024; i++)
	{
		g_assert_cmpint(data[i], ==, i * 10);
	}

	H5Aclose(attribute);
	H5Gclose(group);
}

static void
write_dataset (hid_t file, gchar const* name)
{
	hid_t dataset;
	hid_t dataspace;

	hsize_t dims[2];

	int data[1024][1024];

	dims[0] = 1024;
	dims[1] = 1024;
	dataspace = H5Screate_simple(2, dims, NULL);
	dataset = H5Dcreate2(file, name, H5T_NATIVE_INT, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	for (guint i = 0; i < 1024; i++)
	{
		for (guint j = 0; j < 1024; j++)
		{
			data[i][j] = i + j;
		}
	}

	H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

	H5Sclose(dataspace);
	H5Dclose(dataset);
}

static void
read_dataset (hid_t file, gchar const* name)
{
	hid_t dataset;
	hid_t dataspace;

	hssize_t elements;

	int data_ds[1024][1024];

	dataset = H5Dopen2(file, name, H5P_DEFAULT);

	dataspace = H5Dget_space(dataset);
	elements = H5Sget_simple_extent_npoints(dataspace);
	g_assert_cmpuint(elements, ==, 1024 * 1024);

	H5Dread(dataset, H5T_NATIVE_INT, dataspace, H5S_ALL, H5P_DEFAULT, data_ds);

	for (guint i = 0; i < 1024; i++)
	{
		for (guint j = 0; j < 1024; j++)
		{
			g_assert_cmpint(data_ds[i][j], ==, i + j);
		}
	}

	H5Dclose(dataset);
}

static void
benchmark_hdf_attribute_write (BenchmarkResult *result)
{
	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;
	hid_t native_vol_id;

	gdouble elapsed;

	native_vol_id = H5VLget_connector_id("native");
	g_assert(native_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 0);

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	j_benchmark_timer_start();

	file = H5Fcreate("JULEA.h5", H5F_ACC_TRUNC, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < 1024; i++)
	{
		write_attribute(file);
	}

	H5Fclose(file);

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = 1024;
	result->bytes = 1024 * 1024 * 1024 * sizeof(int);
}

static void
benchmark_hdf_attribute_read (BenchmarkResult *result)
{
	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;
	hid_t native_vol_id;

	gdouble elapsed;

	native_vol_id = H5VLget_connector_id("native");
	g_assert(native_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 0);

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("JULEA.h5", H5F_ACC_TRUNC, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < 1024; i++)
	{
		write_attribute(file);
	}

	H5Fclose(file);

	j_benchmark_timer_start();

	file = H5Fcreate("JULEA.h5", H5F_ACC_RDONLY, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < 1024; i++)
	{
		read_attribute(file);
	}

	H5Fclose(file);

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = 1024;
	result->bytes = 1024 * 1024 * 1024 * sizeof(int);
}

static void
benchmark_hdf_dataset_write (BenchmarkResult *result)
{
	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;
	hid_t native_vol_id;

	gdouble elapsed;

	native_vol_id = H5VLget_connector_id("native");
	g_assert(native_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 0);

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	j_benchmark_timer_start();

	file = H5Fcreate("JULEA.h5", H5F_ACC_TRUNC, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < 1024; i++)
	{
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("BenchmarkDataset%u", i);
		write_dataset(file, name);
	}

	H5Fclose(file);

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = 1024;
	result->bytes = 1024 * 1024 * 1024 * sizeof(int);
}

static void
benchmark_hdf_dataset_read (BenchmarkResult *result)
{
	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;
	hid_t native_vol_id;

	gdouble elapsed;

	native_vol_id = H5VLget_connector_id("native");
	g_assert(native_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 0);

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("native") == 1);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("JULEA.h5", H5F_ACC_TRUNC, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < 1024; i++)
	{
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("BenchmarkDataset%u", i);
		write_dataset(file, name);
	}

	H5Fclose(file);

	j_benchmark_timer_start();

	file = H5Fcreate("JULEA.h5", H5F_ACC_RDONLY, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < 1024; i++)
	{
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("BenchmarkDataset%u", i);
		read_dataset(file, name);
	}

	H5Fclose(file);

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = 1024;
	result->bytes = 1024 * 1024 * 1024 * sizeof(int);
}

#endif

void
benchmark_hdf (void)
{
	// FIXME repeated runs exhibit strange behavior, objects are distributed differently etc.
#ifdef HAVE_HDF5
	j_benchmark_run("/hdf5/attribute/write", benchmark_hdf_attribute_write);
	j_benchmark_run("/hdf5/attribute/read", benchmark_hdf_attribute_read);
	j_benchmark_run("/hdf5/dataset/write", benchmark_hdf_dataset_write);
	j_benchmark_run("/hdf5/dataset/read", benchmark_hdf_dataset_read);
#endif
}
