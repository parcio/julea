/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2018-2019 Johannes Coym
 * Copyright (C) 2019-2020 Michael Kuhn
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

#include <julea-hdf5.h>

#include <hdf5.h>

static void
set_semantics(void)
{
	g_autoptr(JSemantics) semantics = NULL;

	semantics = j_benchmark_get_semantics();
	j_hdf5_set_semantics(semantics);
}

static guint
dimensions_to_size(guint dimensions)
{
	guint size = 1024;

	switch (dimensions)
	{
		case 1:
			size = 1024;
			break;
		case 2:
			size = 1024 * 1024;
			break;
		default:
			g_assert_not_reached();
	}

	return size;
}

static hid_t
create_group(hid_t file, gchar const* name)
{
	hid_t group;

	group = H5Gcreate2(file, name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	return group;
}

static hid_t
open_group(hid_t file, gchar const* name)
{
	hid_t group;

	group = H5Gopen2(file, name, H5P_DEFAULT);

	return group;
}

static void
write_attribute(hid_t group, gchar const* name)
{
	hid_t attribute;
	hid_t dataspace;

	hsize_t dims[1];

	int data[1024];

	dims[0] = 1024;
	dataspace = H5Screate_simple(1, dims, NULL);
	attribute = H5Acreate2(group, name, H5T_NATIVE_INT, dataspace, H5P_DEFAULT, H5P_DEFAULT);

	for (guint i = 0; i < 1024; i++)
	{
		data[i] = i * 10;
	}

	H5Awrite(attribute, H5T_NATIVE_INT, data);

	H5Sclose(dataspace);
	H5Aclose(attribute);
}

static void
read_attribute(hid_t group, gchar const* name)
{
	hid_t attribute;
	hid_t dataspace;

	hssize_t elements;

	int data[1024];

	attribute = H5Aopen(group, name, H5P_DEFAULT);

	dataspace = H5Aget_space(attribute);
	elements = H5Sget_simple_extent_npoints(dataspace);
	g_assert_cmpuint(elements, ==, 1024);

	H5Aread(attribute, H5T_NATIVE_INT, data);

	for (guint i = 0; i < 1024; i++)
	{
		g_assert_cmpint(data[i], ==, i * 10);
	}

	H5Aclose(attribute);
}

static hid_t
create_dataset(hid_t file, gchar const* name, guint dimensions)
{
	hid_t dataset;
	hid_t dataspace;

	hsize_t dims[dimensions];

	for (guint i = 0; i < dimensions; i++)
	{
		dims[i] = 1024;
	}

	dataspace = H5Screate_simple(dimensions, dims, NULL);
	dataset = H5Dcreate2(file, name, H5T_NATIVE_INT, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	H5Sclose(dataspace);

	return dataset;
}

static hid_t
open_dataset(hid_t file, gchar const* name)
{
	hid_t dataset;

	dataset = H5Dopen2(file, name, H5P_DEFAULT);

	return dataset;
}

static void
write_dataset(hid_t dataset, guint dimensions)
{
	guint size;
	g_autofree int* data = NULL;

	size = dimensions_to_size(dimensions);
	data = g_new(int, size);

	for (guint i = 0; i < size; i++)
	{
		data[i] = i;
	}

	H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
}

static void
read_dataset(hid_t dataset, guint dimensions)
{
	hid_t dataspace;

	hssize_t elements;

	guint size;
	g_autofree int* data = NULL;

	size = dimensions_to_size(dimensions);
	data = g_new(int, size);
	dataspace = H5Dget_space(dataset);
	elements = H5Sget_simple_extent_npoints(dataspace);
	g_assert_cmpuint(elements, ==, size);

	H5Dread(dataset, H5T_NATIVE_INT, dataspace, H5S_ALL, H5P_DEFAULT, data);

	for (guint i = 0; i < size; i++)
	{
		g_assert_cmpint(data[i], ==, i);
	}
}

static void
benchmark_hdf_attribute_write(BenchmarkRun* run)
{
	guint const n = 1000;

	hid_t file;
	hid_t group;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-attribute-write.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());
	group = create_group(file, "benchmark-attribute-write");

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-attribute-write-%u", i + (iter * n));
			write_attribute(group, name);
		}

		iter++;
	}

	H5Gclose(group);
	H5Fclose(file);

	j_benchmark_timer_stop(run);

	run->operations = n;
	run->bytes = n * 1024 * sizeof(int);
}

static void
benchmark_hdf_attribute_read(BenchmarkRun* run)
{
	guint const n = 1000;

	hid_t file;
	hid_t group;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-attribute-read.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());
	group = create_group(file, "benchmark-attribute-read");

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-attribute-read-%u", i + (iter * n));
			write_attribute(group, name);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-attribute-read-%u", i + (iter * n));
			read_attribute(group, name);
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	H5Gclose(group);
	H5Fclose(file);

	run->operations = n;
	run->bytes = n * 1024 * sizeof(int);
}

static void
_benchmark_hdf_dataset_create(BenchmarkRun* run, guint dimensions)
{
	guint const n = 1000;

	hid_t file;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-dataset-create.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t dataset;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dataset-create-%u", i + (iter * n));
			dataset = create_dataset(file, name, dimensions);
			H5Dclose(dataset);
		}

		iter++;
	}

	H5Fclose(file);

	j_benchmark_timer_stop(run);

	run->operations = n;
}

static void
benchmark_hdf_dataset_create_4m(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_create(run, 2);
}

static void
benchmark_hdf_dataset_create_4k(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_create(run, 1);
}

static void
_benchmark_hdf_dataset_open(BenchmarkRun* run, guint dimensions)
{
	guint const n = 1000;

	hid_t file;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-dataset-open.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t dataset;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dataset-open-%u", i + (iter * n));
			dataset = create_dataset(file, name, dimensions);
			H5Dclose(dataset);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			hid_t dataset;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dataset-open-%u", i + (iter * n));
			dataset = H5Dopen2(file, name, H5P_DEFAULT);
			H5Dclose(dataset);
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	H5Fclose(file);

	run->operations = n;
}

static void
benchmark_hdf_dataset_open_4m(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_open(run, 2);
}

static void
benchmark_hdf_dataset_open_4k(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_open(run, 1);
}

static void
_benchmark_hdf_dataset_write(BenchmarkRun* run, guint dimensions)
{
	guint const n = 100;

	hid_t file;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-dataset-write.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t dataset;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dataset-write-%u", i + (iter * n));
			dataset = create_dataset(file, name, dimensions);
			write_dataset(dataset, dimensions);
			H5Dclose(dataset);
		}

		iter++;
	}

	H5Fclose(file);

	j_benchmark_timer_stop(run);

	run->operations = n;
	run->bytes = n * dimensions_to_size(dimensions) * sizeof(int);
}

static void
benchmark_hdf_dataset_write_4m(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_write(run, 2);
}

static void
benchmark_hdf_dataset_write_4k(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_write(run, 1);
}

static void
_benchmark_hdf_dataset_read(BenchmarkRun* run, guint dimensions)
{
	guint const n = 100;

	hid_t file;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-dataset-read.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t dataset;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dataset-read-%u", i + (iter * n));
			dataset = create_dataset(file, name, dimensions);
			write_dataset(dataset, dimensions);
			H5Dclose(dataset);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			hid_t dataset;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dataset-read-%u", i + (iter * n));
			dataset = open_dataset(file, name);
			read_dataset(dataset, dimensions);
			H5Dclose(dataset);
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	H5Fclose(file);

	run->operations = n;
	run->bytes = n * dimensions_to_size(dimensions) * sizeof(int);
}

static void
benchmark_hdf_dataset_read_4m(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_read(run, 2);
}

static void
benchmark_hdf_dataset_read_4k(BenchmarkRun* run)
{
	_benchmark_hdf_dataset_read(run, 1);
}

static void
benchmark_hdf_file_create(BenchmarkRun* run)
{
	guint const n = 1000;

	guint iter = 0;

	set_semantics();

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-file-create-%u.h5", i + (iter * n));
			file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

			H5Fclose(file);
		}

		iter++;
	}

	j_benchmark_timer_stop(run);

	run->operations = n;
}

static void
benchmark_hdf_file_open(BenchmarkRun* run)
{
	guint const n = 1000;

	guint iter = 0;

	set_semantics();

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-file-open-%u.h5", i + (iter * n));
			file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());
			H5Fclose(file);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-file-open-%u.h5", i + (iter * n));
			file = H5Fopen(name, H5F_ACC_RDWR, j_hdf5_get_fapl());
			H5Fclose(file);
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	run->operations = n;
}

static void
benchmark_hdf_group_create(BenchmarkRun* run)
{
	guint const n = 1000;

	hid_t file;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-group-create.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t group;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-group-create-%u", i + (iter * n));
			group = create_group(file, name);
			H5Gclose(group);
		}

		iter++;
	}

	H5Fclose(file);

	j_benchmark_timer_stop(run);

	run->operations = n;
}

static void
benchmark_hdf_group_open(BenchmarkRun* run)
{
	guint const n = 1000;

	hid_t file;
	guint iter = 0;

	set_semantics();

	file = H5Fcreate("benchmark-group-open.h5", H5F_ACC_EXCL, H5P_DEFAULT, j_hdf5_get_fapl());

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t group;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-group-open-%u", i + (iter * n));
			group = create_group(file, name);
			H5Gclose(group);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			hid_t group;

			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-group-open-%u", i + (iter * n));
			group = open_group(file, name);
			H5Gclose(group);
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	H5Fclose(file);

	run->operations = n;
}

#endif

void
benchmark_hdf(void)
{
	// FIXME repeated runs exhibit strange behavior, objects are distributed differently etc.
#ifdef HAVE_HDF5
	j_benchmark_add("/hdf5/attribute/write", benchmark_hdf_attribute_write);
	j_benchmark_add("/hdf5/attribute/read", benchmark_hdf_attribute_read);
	j_benchmark_add("/hdf5/dataset4M/create", benchmark_hdf_dataset_create_4m);
	j_benchmark_add("/hdf5/dataset4M/open", benchmark_hdf_dataset_open_4m);
	j_benchmark_add("/hdf5/dataset4M/write", benchmark_hdf_dataset_write_4m);
	j_benchmark_add("/hdf5/dataset4M/read", benchmark_hdf_dataset_read_4m);
	j_benchmark_add("/hdf5/dataset4K/create", benchmark_hdf_dataset_create_4k);
	j_benchmark_add("/hdf5/dataset4K/open", benchmark_hdf_dataset_open_4k);
	j_benchmark_add("/hdf5/dataset4K/write", benchmark_hdf_dataset_write_4k);
	j_benchmark_add("/hdf5/dataset4K/read", benchmark_hdf_dataset_read_4k);
	j_benchmark_add("/hdf5/file/create", benchmark_hdf_file_create);
	j_benchmark_add("/hdf5/file/open", benchmark_hdf_file_open);
	j_benchmark_add("/hdf5/group/create", benchmark_hdf_group_create);
	j_benchmark_add("/hdf5/group/open", benchmark_hdf_group_open);
#endif
}
