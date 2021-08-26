/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2020-2021 Michael Kuhn
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

#include <julea-db.h>
#include <julea-hdf5.h>
#include <julea-kv.h>

#include <hdf5.h>

static gchar const* vol_connector = NULL;

/// \todo redundant (see hdf.c)
static void
set_semantics(void)
{
	g_autoptr(JSemantics) semantics = NULL;

	semantics = j_benchmark_get_semantics();
	j_hdf5_set_semantics(semantics);
}

/// \todo redundant (see hdf.c)
static void
discard_file(gchar const* path)
{
	if (g_strcmp0(vol_connector, "native") == 0)
	{
		j_helper_file_discard(path);
	}
}

static gboolean
deserialize_attribute_data(gconstpointer value, guint32 len, gconstpointer* data, gsize* data_size)
{
	bson_t bson[1];
	bson_iter_t iterator;
	bson_subtype_t bs;

	bson_init_static(bson, value, len);
	bson_iter_init(&iterator, bson);

	bs = BSON_SUBTYPE_BINARY;

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "type") == 0)
		{
			guint32 type;

			type = bson_iter_int32(&iterator);

			if (type != 3)
			{
				return FALSE;
			}
		}
		else if (g_strcmp0(key, "data") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t*)data_size, (const uint8_t**)data);
		}
	}

	return TRUE;
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

static void
benchmark_hdf_dai_native(BenchmarkRun* run)
{
	guint const n = 25;

	guint iter = 0;

	set_semantics();

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			hid_t group;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dai-native-%u.h5", i + (iter * n));
			file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
			group = H5Gcreate2(file, "benchmark-dai-native", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

			for (guint j = 0; j < n * 10; j++)
			{
				g_autofree gchar* aname = NULL;

				aname = g_strdup_printf("benchmark-dai-native-%u", j);
				write_attribute(group, aname);
			}

			H5Gclose(group);
			H5Fclose(file);

			discard_file(name);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			hid_t group;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dai-native-%u.h5", i + (iter * n));
			file = H5Fopen(name, H5F_ACC_RDWR, H5P_DEFAULT);
			group = H5Gopen2(file, "benchmark-dai-native", H5P_DEFAULT);

			for (guint j = 0; j < n * 10; j++)
			{
				g_autofree gchar* aname = NULL;

				aname = g_strdup_printf("benchmark-dai-native-%u", j);
				read_attribute(group, aname);
			}

			H5Gclose(group);
			H5Fclose(file);
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	run->operations = n * n * 10;
}

static void
benchmark_hdf_dai_kv_get(BenchmarkRun* run)
{
	guint const n = 25;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	guint iter = 0;

	set_semantics();

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			hid_t group;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dai-get-%u.h5", i + (iter * n));
			file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
			group = H5Gcreate2(file, "benchmark-dai-get", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

			for (guint j = 0; j < n * 10; j++)
			{
				g_autofree gchar* aname = NULL;

				aname = g_strdup_printf("benchmark-dai-get-%u", j);
				write_attribute(group, aname);
			}

			H5Gclose(group);
			H5Fclose(file);
		}

		j_benchmark_timer_start(run);

		for (guint i = 0; i < n; i++)
		{
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dai-get-%u.h5/benchmark-dai-get", i + (iter * n));

			for (guint j = 0; j < n * 10; j++)
			{
				g_autoptr(JKV) kv = NULL;
				g_autofree gchar* aname = NULL;
				g_autofree gchar* value = NULL;
				guint32 len = 0;
				gconstpointer adata = NULL;
				gsize alen = 0;

				aname = g_strdup_printf("%s/benchmark-dai-get-%u", name, j);
				kv = j_kv_new("hdf5", aname);
				j_kv_get(kv, (gpointer)&value, &len, batch);

				if (!j_batch_execute(batch))
				{
					g_assert_not_reached();
				}

				deserialize_attribute_data(value, len, &adata, &alen);
			}
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	run->operations = n * n * 10;
}

static void
benchmark_hdf_dai_kv_iterator(BenchmarkRun* run)
{
	guint const n = 25;

	guint iter = 0;
	guint attrs = 0;

	set_semantics();

	while (j_benchmark_iterate(run))
	{
		g_autoptr(JKVIterator) kv_iterator = NULL;

		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			hid_t group;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dai-iterator-%u.h5", i + (iter * n));
			file = H5Fcreate(name, H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
			group = H5Gcreate2(file, "benchmark-dai-iterator", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

			for (guint j = 0; j < n * 10; j++)
			{
				g_autofree gchar* aname = NULL;

				aname = g_strdup_printf("benchmark-dai-iterator-%u", j);
				write_attribute(group, aname);
			}

			H5Gclose(group);
			H5Fclose(file);
		}

		j_benchmark_timer_start(run);

		kv_iterator = j_kv_iterator_new("hdf5", "benchmark-dai-iterator-");

		while (j_kv_iterator_next(kv_iterator))
		{
			gchar const* key;
			gconstpointer value;
			guint32 len;
			gconstpointer adata = NULL;
			gsize alen = 0;

			key = j_kv_iterator_get(kv_iterator, &value, &len);

			if (!g_str_has_suffix(key, "_ts"))
			{
				if (deserialize_attribute_data(value, len, &adata, &alen))
				{
					attrs++;
				}
			}
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	run->operations = attrs / iter;
}

static void
benchmark_hdf_dai_db_iterator(BenchmarkRun* run)
{
	guint const n = 25;

	guint iter = 0;
	guint attrs = 0;

	set_semantics();

	while (j_benchmark_iterate(run))
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(JBatch) batch = NULL;
		g_autoptr(JDBIterator) iterator = NULL;
		g_autoptr(JDBSchema) schema = NULL;
		g_autoptr(JDBSelector) selector = NULL;

		gboolean ret;

		for (guint i = 0; i < n; i++)
		{
			hid_t file;
			hid_t group;
			g_autofree gchar* name = NULL;

			name = g_strdup_printf("benchmark-dai-db-iterator-%u.h5", i + (iter * n));
			file = H5Fcreate(name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
			group = H5Gcreate2(file, "benchmark-dai-db-iterator", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

			for (guint j = 0; j < n * 10; j++)
			{
				g_autofree gchar* aname = NULL;

				aname = g_strdup_printf("benchmark-dai-db-iterator-%u", j);
				write_attribute(group, aname);
			}

			H5Gclose(group);
			H5Fclose(file);
		}

		j_benchmark_timer_start(run);

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

		schema = j_db_schema_new("HDF5_DB", "attr", NULL);
		j_db_schema_get(schema, batch, &error);
		ret = j_batch_execute(batch);
		g_assert_true(ret);

		selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
		iterator = j_db_iterator_new(schema, selector, &error);

		while (j_db_iterator_next(iterator, &error))
		{
			JDBType type;
			g_autofree gpointer data = NULL;
			guint64 data_len;

			if (j_db_iterator_get_field(iterator, "data", &type, &data, &data_len, &error))
			{
				attrs++;
			}
		}

		j_benchmark_timer_stop(run);

		iter++;
	}

	run->operations = attrs / iter;
}

#endif

void
benchmark_hdf_dai(void)
{
#ifdef HAVE_HDF5
	vol_connector = g_getenv("HDF5_VOL_CONNECTOR");

	if (vol_connector == NULL)
	{
		// Make sure we do not accidentally run benchmarks for native HDF5
		// If comparisons with native HDF5 are necessary, set HDF5_VOL_CONNECTOR to "native"
		return;
	}

	j_benchmark_add("/hdf5/dai/native", benchmark_hdf_dai_native);

	if (g_strcmp0(vol_connector, "julea-kv") == 0)
	{
		j_benchmark_add("/hdf5/dai/kv-get", benchmark_hdf_dai_kv_get);
		j_benchmark_add("/hdf5/dai/kv-iterator", benchmark_hdf_dai_kv_iterator);
	}
	else if (g_strcmp0(vol_connector, "julea-db") == 0)
	{
		j_benchmark_add("/hdf5/dai/db-iterator", benchmark_hdf_dai_db_iterator);
	}
#endif
}
