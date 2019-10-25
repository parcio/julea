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

static
hid_t
create_group (hid_t file, gchar const* name)
{
	hid_t group;

	group = H5Gcreate2(file, name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	return group;
}

static
hid_t
open_group (hid_t file, gchar const* name)
{
	hid_t group;

	group = H5Gopen2(file, name, H5P_DEFAULT);

	return group;
}

static
void
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

static
void
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

static
hid_t
create_dataset (hid_t file, gchar const* name)
{
	hid_t dataset;
	hid_t dataspace;

	hsize_t dims[2];

	dims[0] = 1024;
	dims[1] = 1024;
	dataspace = H5Screate_simple(2, dims, NULL);
	dataset = H5Dcreate2(file, name, H5T_NATIVE_INT, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	H5Sclose(dataspace);

	return dataset;
}

static
hid_t
open_dataset (hid_t file, gchar const* name)
{
	hid_t dataset;

	dataset = H5Dopen2(file, name, H5P_DEFAULT);

	return dataset;
}

static
void
write_dataset (hid_t dataset)
{
	int data[1024][1024];

	for (guint i = 0; i < 1024; i++)
	{
		for (guint j = 0; j < 1024; j++)
		{
			data[i][j] = i + j;
		}
	}

	H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
}

static
void
read_dataset (hid_t dataset)
{
	hid_t dataspace;

	hssize_t elements;

	int data[1024][1024];

	dataspace = H5Dget_space(dataset);
	elements = H5Sget_simple_extent_npoints(dataspace);
	g_assert_cmpuint(elements, ==, 1024 * 1024);

	H5Dread(dataset, H5T_NATIVE_INT, dataspace, H5S_ALL, H5P_DEFAULT, data);

	for (guint i = 0; i < 1024; i++)
	{
		for (guint j = 0; j < 1024; j++)
		{
			g_assert_cmpint(data[i][j], ==, i + j);
		}
	}
}

static
void
benchmark_hdf_attribute_write (BenchmarkResult *result)
{
	guint const n = 25000;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	j_benchmark_timer_start();

	file = H5Fcreate("benchmark-attribute-write.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < n; i++)
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
	result->operations = n;
	result->bytes = n * 1024 * sizeof(int);
}

static
void
benchmark_hdf_attribute_read (BenchmarkResult *result)
{
	guint const n = 25000;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("benchmark-attribute-read.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < n; i++)
	{
		write_attribute(file);
	}

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
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
	result->operations = n;
	result->bytes = n * 1024 * sizeof(int);
}

static
void
benchmark_hdf_dataset_create (BenchmarkResult *result)
{
	guint const n = 25000;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	const H5VL_class_t *h5vl_julea;

	hid_t file;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("benchmark-dataset-create.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t dataset;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-dataset-create-%u", i);
		dataset = create_dataset(file, name);
		H5Dclose(dataset);
	}

	elapsed = j_benchmark_timer_elapsed();

	H5Fclose(file);

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_hdf_dataset_open (BenchmarkResult *result)
{
	guint const n = 25000;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	const H5VL_class_t *h5vl_julea;

	hid_t file;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("benchmark-dataset-open.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < n; i++)
	{
		hid_t dataset;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-dataset-open-%u", i);
		dataset = create_dataset(file, name);
		H5Dclose(dataset);
	}

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t dataset;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-dataset-open-%u", i);
		dataset = H5Dopen2(file, name, H5P_DEFAULT);

		H5Dclose(dataset);
	}

	elapsed = j_benchmark_timer_elapsed();

	H5Fclose(file);

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_hdf_dataset_write (BenchmarkResult *result)
{
	guint const n = 512;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	j_benchmark_timer_start();

	file = H5Fcreate("benchmark-dataset-write.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < n; i++)
	{
		hid_t dataset;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-dataset-write-%u", i);
		dataset = create_dataset(file, name);
		write_dataset(dataset);
		H5Dclose(dataset);
	}

	H5Fclose(file);

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
	result->bytes = n * 1024 * 1024 * sizeof(int);
}

static
void
benchmark_hdf_dataset_read (BenchmarkResult *result)
{
	guint const n = 512;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	hid_t file;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("benchmark-dataset-read.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < n; i++)
	{
		hid_t dataset;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-dataset-read-%u", i);
		dataset = create_dataset(file, name);
		write_dataset(dataset);
		H5Dclose(dataset);
	}

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t dataset;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-dataset-read-%u", i);
		dataset = open_dataset(file, name);
		read_dataset(dataset);
		H5Dclose(dataset);
	}

	H5Fclose(file);

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
	result->bytes = n * 1024 * 1024 * sizeof(int);
}

static
void
benchmark_hdf_file_create (BenchmarkResult *result)
{
	guint const n = 100000;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t file;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-file-create-%u.h5", i);
		file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

		H5Fclose(file);
	}

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_hdf_file_open (BenchmarkResult *result)
{
	guint const n = 100000;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	for (guint i = 0; i < n; i++)
	{
		hid_t file;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-file-open-%u.h5", i);
		file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

		H5Fclose(file);
	}

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t file;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-file-open-%u.h5", i);
		file = H5Fopen(name, H5F_ACC_RDWR, acc_tpl);

		H5Fclose(file);
	}

	elapsed = j_benchmark_timer_elapsed();

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_hdf_group_create (BenchmarkResult *result)
{
	guint const n = 100000;

	hid_t file;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("benchmark-group-create.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t group;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-group-create-%u", i);
		group = create_group(file, name);
		H5Gclose(group);
	}

	elapsed = j_benchmark_timer_elapsed();

	H5Fclose(file);

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_hdf_group_open (BenchmarkResult *result)
{
	guint const n = 100000;

	hid_t file;

	hid_t acc_tpl;
	hid_t julea_vol_id;

	const H5VL_class_t *h5vl_julea;

	gdouble elapsed;

	h5vl_julea = H5PLget_plugin_info();
	julea_vol_id = H5VLregister_connector(h5vl_julea, H5P_DEFAULT);
	g_assert(julea_vol_id > 0);
	g_assert(H5VLis_connector_registered("julea") == 1);

	H5VLinitialize(julea_vol_id, H5P_DEFAULT);
	acc_tpl = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_vol(acc_tpl, julea_vol_id, NULL);

	file = H5Fcreate("benchmark-group-open.h5", H5F_ACC_EXCL, H5P_DEFAULT, acc_tpl);

	for (guint i = 0; i < n; i++)
	{
		hid_t group;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-group-open-%u", i);
		group = create_group(file, name);
		H5Gclose(group);
	}

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		hid_t group;

		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-group-open-%u", i);
		group = open_group(file, name);
		H5Gclose(group);
	}

	elapsed = j_benchmark_timer_elapsed();

	H5Fclose(file);

	H5Pclose(acc_tpl);
	H5VLterminate(julea_vol_id);
	H5VLunregister_connector(julea_vol_id);
	g_assert(H5VLis_connector_registered("julea") == 0);

	result->elapsed_time = elapsed;
	result->operations = n;
}

#endif

void
benchmark_hdf (void)
{
	// FIXME repeated runs exhibit strange behavior, objects are distributed differently etc.
#ifdef HAVE_HDF5
	j_benchmark_run("/hdf5/attribute/write", benchmark_hdf_attribute_write);
	j_benchmark_run("/hdf5/attribute/read", benchmark_hdf_attribute_read);
	j_benchmark_run("/hdf5/dataset/create", benchmark_hdf_dataset_create);
	j_benchmark_run("/hdf5/dataset/open", benchmark_hdf_dataset_open);
	j_benchmark_run("/hdf5/dataset/write", benchmark_hdf_dataset_write);
	j_benchmark_run("/hdf5/dataset/read", benchmark_hdf_dataset_read);
	j_benchmark_run("/hdf5/file/create", benchmark_hdf_file_create);
	j_benchmark_run("/hdf5/file/open", benchmark_hdf_file_open);
	j_benchmark_run("/hdf5/group/create", benchmark_hdf_group_create);
	j_benchmark_run("/hdf5/group/open", benchmark_hdf_group_open);
#endif
}
