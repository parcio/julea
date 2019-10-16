/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Olga Perevalova
 * Copyright (C) 2017 Eugen Betke
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

// FIXME check whether version is up to date: https://github.com/Olgasnezh/hdf5-vol-sqlite-plugin
// FIXME clean up
// FIXME fix memory leaks

// FIXME
#define H5Sencode_vers 1

#include <julea-config.h>
#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <glib.h>
#include <bson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include <hdf5.h>
#include <H5PLextern.h>

#define _GNU_SOURCE

#define JULEA 520

/* structure for file*/
struct JHF_t
{
	char* name;
};

typedef struct JHF_t JHF_t;

/* structure for group*/
struct JHG_t
{
	char* location;
	char* name;
};

typedef struct JHG_t JHG_t;

/* structure for dataset*/
struct JHD_t
{
	char* location;
	char* name;
	size_t data_size;
	JDistribution* distribution;
	JDistributedObject* object;
	JKV* kv;
};

typedef struct JHD_t JHD_t;

/*structure for attribute*/
struct JHA_t
{
	char* location;
	char* name;
	size_t data_size;
	JKV* kv;
	JKV* ts;
};

typedef struct JHA_t JHA_t;

char* j_hdf5_encode_type(const char*, hid_t*, hid_t, size_t*);
char* j_hdf5_encode_space(const char*, hid_t*, hid_t, size_t*);

bson_t* j_hdf5_serialize(const void*, size_t);
bson_t* j_hdf5_serialize_ts (const void*, size_t, const void*, size_t);
bson_t* j_hdf5_serialize_dataset(const void*, size_t, const void*, size_t, size_t, JDistribution*);

void j_hdf5_deserialize(const bson_t*, void*, size_t);
void* j_hdf5_deserialize_type (const bson_t*);
void* j_hdf5_deserialize_space (const bson_t*);
void j_hdf5_deserialize_dataset(const bson_t*, JHD_t*, size_t*);
void j_hdf5_deserialize_size(const bson_t*, size_t*);

char* create_path(const char*, char*);

hid_t native_plugin_id = -1;

/**
 * Initializes the plugin
 *
 * \return err Error
 **/
static
herr_t
H5VL_julea_init (hid_t vipl_id)
{
	(void)vipl_id;

	return 0;
}

/**
 * Terminates the plugin
 *
 * \return err Error
 **/
static
herr_t
H5VL_julea_term (void)
{
	return 0;
}

/**
 * Encodes the type
 *
 * \return type_buf The encoded type
 **/
char*
j_hdf5_encode_type (const char* property, hid_t* type_id, hid_t cpl_id, size_t* type_size)
{
	char* type_buf;
	(void)property;
	(void)cpl_id;
	//H5Pget(cpl_id, property, type_id);
	g_assert(-1 != *type_id);
	H5Tencode(*type_id, NULL, type_size);
	type_buf = (char*) malloc(*type_size);
	H5Tencode(*type_id, type_buf, type_size);
	return type_buf;
}

/**
 * Encodes the space
 *
 * \return type_buf The encoded space
 **/
char*
j_hdf5_encode_space (const char* property, hid_t* space_id, hid_t cpl_id, size_t* space_size)
{
	char* space_buf;
	(void)property;
	(void)cpl_id;
	//H5Pget(cpl_id, property, space_id);
	g_assert(-1 != *space_id);
	H5Sencode(*space_id, NULL, space_size);
	space_buf = (char*) malloc(*space_size);
	H5Sencode(*space_id, space_buf, space_size);
	return space_buf;
}

/**
 * Serializes attribute data
 *
 * \param data The data
 * \param size The size of the data
 *
 * \return b The serialized BSON
 **/
bson_t*
j_hdf5_serialize (const void* data, size_t data_size)
{
	J_TRACE_FUNCTION(NULL);

	bson_t* b;

	b = bson_new();

	bson_append_binary(b, "data", -1, BSON_SUBTYPE_BINARY, data, data_size);
	bson_append_int32(b, "size", -1, (int32_t)data_size);

	return b;
}

/**
 * Serializes type and space data
 *
 * \param type_data The type data
 * \param type_size The size of the type data
 * \param space_data The space data
 * \param space_size The size of the space data
 *
 * \return b The serialized BSON
 **/
bson_t*
j_hdf5_serialize_ts (const void* type_data, size_t type_size, const void* space_data, size_t space_size)
{
	J_TRACE_FUNCTION(NULL);

	bson_t* b;

	b = bson_new();

	bson_append_int32(b, "tsize", -1, (int32_t)type_size);
	bson_append_int32(b, "ssize", -1, (int32_t)space_size);
	bson_append_binary(b, "tdata", -1, BSON_SUBTYPE_BINARY, type_data, type_size);
	bson_append_binary(b, "sdata", -1, BSON_SUBTYPE_BINARY, space_data, space_size);

	return b;
}

/**
 * Serializes all metadata from datasets
 *
 * \param type_data The type data
 * \param type_size The size of the type data
 * \param space_data The space data
 * \param space_size The size of the space data
 * \param data_size The data size of the dataset
 * \param distribution The distribution of the dataset
 *
 * \return b The serialized BSON
 **/
bson_t*
j_hdf5_serialize_dataset (const void* type_data, size_t type_size, const void* space_data, size_t space_size, size_t data_size, JDistribution* distribution)
{
	J_TRACE_FUNCTION(NULL);

	bson_t* b;
	bson_t* b_distribution;

	b_distribution = j_distribution_serialize(distribution);

	b = bson_new();

	bson_append_int32(b, "tsize", -1, (int32_t)type_size);
	bson_append_int32(b, "ssize", -1, (int32_t)space_size);
	bson_append_binary(b, "tdata", -1, BSON_SUBTYPE_BINARY, type_data, type_size);
	bson_append_binary(b, "sdata", -1, BSON_SUBTYPE_BINARY, space_data, space_size);
	bson_append_int32(b, "size", -1, (int32_t)data_size);
	bson_append_document(b, "distribution", -1, b_distribution);

	bson_destroy(b_distribution);

	return b;
}

/**
 * Deserializes only the data from the bson
 *
 * \param b The bson containing the data
 * \param data The pointer where the data is returned
 * \param data_size The size of the data
 **/
void
j_hdf5_deserialize (const bson_t* b, void* data, size_t data_size)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;

	g_return_if_fail(b != NULL);

	bson_iter_init(&iterator, b);

	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "data") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&data_size, (const uint8_t **) &buf);
		}
	}
	memcpy(data, buf, data_size);
}

/**
 * Deserializes the type data from the bson
 *
 * \param b The bson containing the data
 *
 * \return type_data the encoded type data
 **/
void*
j_hdf5_deserialize_type (const bson_t* b)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;
	// FIXME might be uninitialized
	void* type_data;
	size_t type_size;

	bson_iter_init(&iterator, b);
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "tsize") == 0)
		{
			type_size = bson_iter_int32(&iterator);
			type_data = malloc(type_size);
		}
	}
	bson_iter_init(&iterator, b);
	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "tdata") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&type_size, (const uint8_t **) &buf);
		}
	}
	memcpy(type_data, buf, type_size);
	return type_data;
}

/**
 * Deserializes the space data from the bson
 *
 * \param b The bson containing the data
 *
 * \return space_data the encoded space data
 **/
void*
j_hdf5_deserialize_space (const bson_t* b)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;
	// FIXME might be uninitialized
	void* space_data;
	size_t space_size;

	bson_iter_init(&iterator, b);
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "ssize") == 0)
		{
			space_size = bson_iter_int32(&iterator);
			space_data = malloc(space_size);
		}
	}
	bson_iter_init(&iterator, b);
	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "sdata") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&space_size, (const uint8_t **) &buf);
		}
	}
	memcpy(space_data, buf, space_size);
	return space_data;
}

/**
 * Deserializes only the data size from the bson
 *
 * \param b The bson containing the data
 * \param data_size Pointer to the data size to return
 **/
void
j_hdf5_deserialize_size (const bson_t* b, size_t* data_size)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;

	g_return_if_fail(data_size != NULL);
	g_return_if_fail(b != NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "size") == 0)
		{
			*data_size = bson_iter_int32(&iterator);
		}
	}
}

/**
 * Deserializes data size and disttribution from the bson
 *
 * \param b The bson containing the data
 * \param d The dataset whoch should contain the distribution
 * \param data_size Pointer to the data size to return
 **/
void
j_hdf5_deserialize_dataset (const bson_t* b, JHD_t* d, size_t* data_size)
{
	J_TRACE_FUNCTION(NULL);

	bson_iter_t iterator;

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "distribution") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_distribution[1];

			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_distribution, data, len);
			d->distribution = j_distribution_new_from_bson(b_distribution);
			bson_destroy(b_distribution);
		}

		if (g_strcmp0(key, "size") == 0)
		{
			*data_size = bson_iter_int32(&iterator);
		}
	}
}

/**
 * Generates the path of the current element
 *
 * \param name The name of the current element
 * \param prev_path The previous path
 *
 * \return path
 **/
char*
create_path (const char* name, char* prev_path)
{
	static const char* seperator = "/";
	char* path = NULL;

	path = (char*) malloc(strlen(prev_path) + strlen(seperator) + strlen(name) + 1);
	strcpy(path, prev_path);
	strcat(path, seperator);
	strcat(path, name);
	return path;
}

/**
 * Creates a new attribute
 *
 * \return attribute The new attribute
 **/
static
void*
H5VL_julea_attr_create (void* obj, const H5VL_loc_params_t* loc_params, const char* attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHA_t* attribute;

	hsize_t* dims;
	gint ndims;

	gchar* space_buf = NULL;
	gchar* type_buf = NULL;
	gsize space_size;
	gsize type_size;

	gsize data_size;

	bson_t* tmp;
	g_autoptr(JBatch) batch = NULL;
	gchar* tsloc;

	gpointer value;
	guint32 len;

	(void)aapl_id;
	(void)dxpl_id;
	(void)req;

	attribute = g_new(JHA_t, 1);
	attribute->name = g_strdup(attr_name);

	type_buf = j_hdf5_encode_type("attr_type_id", &type_id, acpl_id, &type_size);
	space_buf = j_hdf5_encode_space("attr_space_id", &space_id, acpl_id, &space_size);
	data_size = H5Tget_size(type_id);

	ndims = H5Sget_simple_extent_ndims(space_id);
	dims = g_new(hsize_t, ndims);
	H5Sget_simple_extent_dims(space_id, dims, NULL);

	for (gint i = 0; i < ndims; i++)
	{
		data_size *= dims[i];
	}

	g_free(dims);

	attribute->data_size = data_size;

	switch (loc_params->obj_type)
	{
		case H5I_DATASET:
			{
				JHD_t* o = obj;

				attribute->location = create_path(attr_name, o->location);
				attribute->kv = j_kv_new("hdf5", attribute->location);
			}
			break;
		case H5I_GROUP:
			{
				JHG_t* o = obj;

				attribute->location = create_path(attr_name, o->location);
				attribute->kv = j_kv_new("hdf5", attribute->location);
			}
			break;
		case H5I_ATTR:
		case H5I_BADID:
		case H5I_DATASPACE:
		case H5I_DATATYPE:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_FILE:
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_NTYPES:
		case H5I_SPACE_SEL_ITER:
		case H5I_UNINIT:
		case H5I_VFL:
		case H5I_VOL:
		default:
			g_assert_not_reached();
			exit(1);
	}

	tsloc = g_strdup_printf("%s_ts", attribute->location);
	attribute->ts = j_kv_new("hdf5", tsloc);
	g_free(tsloc);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	tmp = j_hdf5_serialize_ts(type_buf, type_size, space_buf, space_size);
	value = bson_destroy_with_steal(tmp, TRUE, &len);
	j_kv_put(attribute->ts, value, len, bson_free, batch);

	if (!j_batch_execute(batch))
	{
		// FIXME check return value properly
	}

	g_free(type_buf);
	g_free(space_buf);

	return attribute;
}

/**
 * Opens an attribute
 *
 * \return attribute The attribute
 **/
static
void*
H5VL_julea_attr_open(void* obj, const H5VL_loc_params_t* loc_params, const char* attr_name, hid_t aapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHA_t* attribute;

	g_autoptr(JBatch) batch = NULL;
	gchar* tsloc;

	gpointer value;
	guint32 len;

	(void)aapl_id;
	(void)dxpl_id;
	(void)req;

	attribute = g_new(JHA_t, 1);
	attribute->name = g_strdup(attr_name);

	switch (loc_params->obj_type)
	{
		case H5I_DATASET:
			{
				JHD_t* o = obj;
				attribute->location = create_path(attr_name, o->location);
			}
			break;
		case H5I_GROUP:
			{
				JHG_t* o = obj;
				attribute->location = create_path(attr_name, o->location);
			}
			break;
		case H5I_ATTR:
		case H5I_BADID:
		case H5I_DATASPACE:
		case H5I_DATATYPE:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_FILE:
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_NTYPES:
		case H5I_SPACE_SEL_ITER:
		case H5I_UNINIT:
		case H5I_VFL:
		case H5I_VOL:
		default:
			g_assert_not_reached();
			exit(1);
	}

	tsloc = g_strdup_printf("%s_ts", attribute->location);
	attribute->ts = j_kv_new("hdf5", tsloc);
	g_free(tsloc);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	attribute->kv = j_kv_new("hdf5", attribute->location);
	j_kv_get(attribute->kv, &value, &len, batch);

	if (j_batch_execute(batch))
	{
		bson_t data[1];

		bson_init_static(data, value, len);
		j_hdf5_deserialize_size(data, &(attribute->data_size));
		g_free(value);
	}

	return attribute;
}

/**
 * Reads the data from the attribute
 **/
static
herr_t
H5VL_julea_attr_read(void* attr, hid_t dtype_id, void* buf, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHA_t* attribute = attr;

	g_autoptr(JBatch) batch = NULL;

	gpointer value;
	guint32 len;

	(void)dtype_id;
	(void)dxpl_id;
	(void)req;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_kv_get(attribute->kv, &value, &len, batch);

	if (j_batch_execute(batch))
	{
		bson_t b[1];

		bson_init_static(b, value, len);
		j_hdf5_deserialize(b, buf, attribute->data_size);
		g_free(value);
	}

	return 1;
}

/**
 * Writes the data of the attribute
 **/
static
herr_t
H5VL_julea_attr_write (void* attr, hid_t dtype_id, const void* buf, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHA_t* attribute = attr;

	g_autoptr(JBatch) batch = NULL;

	bson_t* tmp;

	gpointer value;
	guint32 len;

	(void)dtype_id;
	(void)dxpl_id;
	(void)req;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	tmp = j_hdf5_serialize(buf, attribute->data_size);
	value = bson_destroy_with_steal(tmp, TRUE, &len);
	j_kv_put(attribute->kv, value, len, bson_free, batch);

	if (!j_batch_execute(batch))
	{
		// FIXME check return value properly
	}

	return 1;
}

/**
 * Provides get Functions of the attribute
 *
 * \return ret_value The error code
 **/
static
herr_t
H5VL_julea_attr_get (void* attr, H5VL_attr_get_t get_type, hid_t dxpl_id, void** req, va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	JHA_t* attribute = attr;

	g_autoptr(JBatch) batch = NULL;

	herr_t ret_value = 0;

	gpointer value;
	guint32 len;

	(void)dxpl_id;
	(void)req;

	switch (get_type)
	{
		case H5VL_ATTR_GET_SPACE:
			{
				hid_t* ret_id = va_arg(arguments, hid_t*);
				void* space;

				batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
				j_kv_get(attribute->ts, &value, &len, batch);

				if (j_batch_execute(batch))
				{
					bson_t b[1];

					bson_init_static(b, value, len);
					space = j_hdf5_deserialize_space(b);
					*ret_id = H5Sdecode(space);
					g_free(space);
					g_free(value);
				}
			}
		break;
		case H5VL_ATTR_GET_TYPE:
			{
				hid_t* ret_id = va_arg(arguments, hid_t*);
				void* type;

				batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
				j_kv_get(attribute->ts, &value, &len, batch);

				if (j_batch_execute(batch))
				{
					bson_t b[1];

					bson_init_static(b, value, len);
					type = j_hdf5_deserialize_type(b);
					*ret_id = H5Tdecode(type);
					g_free(type);
					g_free(value);
				}
			}
			break;
		case H5VL_ATTR_GET_ACPL:
		case H5VL_ATTR_GET_INFO:
		case H5VL_ATTR_GET_NAME:
		case H5VL_ATTR_GET_STORAGE_SIZE:
		default:
			g_assert_not_reached();
			exit(1);
	}

	return ret_value;
}

/**
 * Closes the attribute
 **/
static
herr_t
H5VL_julea_attr_close (void* attr, hid_t dxpl_id, void** req)
{
	JHA_t* attribute = attr;

	(void)dxpl_id;
	(void)req;

	if (attribute->kv != NULL)
	{
		j_kv_unref(attribute->kv);
	}

	if (attribute->ts != NULL)
	{
		j_kv_unref(attribute->ts);
	}

	g_free(attribute->name);
	g_free(attribute->location);
	g_free(attribute);

	return 1;
}

/**
 * Creates a new file
 *
 * \return file The new file
 **/
static
void*
H5VL_julea_file_create (const char* fname, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void** req)
{
	JHF_t* file;

	(void)flags;
	(void)fcpl_id;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	file = g_new(JHF_t, 1);
	file->name = g_strdup(fname);

	return file;
}

/**
 * Opens a file
 *
 * \return file The file
 **/
static
void*
H5VL_julea_file_open (const char* fname, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void** req)
{
	JHF_t* file;

	(void)flags;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	file = g_new(JHF_t, 1);
	file->name = g_strdup(fname);

	return file;
}

/**
 * Closes the file
 **/
static
herr_t
H5VL_julea_file_close (void* file, hid_t dxpl_id, void** req)
{
	JHF_t* f = file;

	(void)dxpl_id;
	(void)req;

	g_free(f->name);
	g_free(f);

	return 1;
}

/**
 * Creates a new group
 *
 * \return group The new group
 **/
static
void*
H5VL_julea_group_create (void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void** req)
{
	JHG_t* group;

	(void)lcpl_id;
	(void)gcpl_id;
	(void)gapl_id;
	(void)dxpl_id;
	(void)req;

	group = g_new(JHG_t, 1);

	switch (loc_params->obj_type)
	{
		case H5I_FILE:
			{
				JHF_t* o = obj;
				group->location = create_path(name, o->name);
				group->name = g_strdup(name);
			}
			break;
		case H5I_GROUP:
			{
				JHG_t* o = obj;
				group->location = create_path(name, o->location);
				group->name = g_strdup(name);
			}
			break;
		case H5I_ATTR:
		case H5I_BADID:
		case H5I_DATASET:
		case H5I_DATASPACE:
		case H5I_DATATYPE:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_NTYPES:
		case H5I_SPACE_SEL_ITER:
		case H5I_UNINIT:
		case H5I_VFL:
		case H5I_VOL:
		default:
			g_assert_not_reached();
			exit(1);
	}

	return group;
}

/**
 * Opens a group
 *
 * \return group The group
 **/
static
void*
H5VL_julea_group_open (void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t gapl_id, hid_t dxpl_id, void** req)
{
	JHG_t* group;

	(void)gapl_id;
	(void)dxpl_id;
	(void)req;

	group = g_new(JHG_t, 1);
	group->name = g_strdup(name);

	switch (loc_params->obj_type)
	{
		case H5I_FILE:
			{
				JHF_t* o = obj;
				group->location = create_path(name, o->name);
			}
			break;
		case H5I_GROUP:
			{
				JHG_t* o = obj;
				group->location = create_path(name, o->location);
			}
			break;
		case H5I_ATTR:
		case H5I_BADID:
		case H5I_DATATYPE:
		case H5I_DATASET:
		case H5I_DATASPACE:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_NTYPES:
		case H5I_SPACE_SEL_ITER:
		case H5I_UNINIT:
		case H5I_VFL:
		case H5I_VOL:
		default:
			g_assert_not_reached();
			exit(1);
	}

	return group;
}

/**
 * Closes the group
 **/
static
herr_t
H5VL_julea_group_close (void* grp, hid_t dxpl_id, void** req)
{
	JHG_t* g = grp;

	(void)dxpl_id;
	(void)req;

	g_free(g->name);
	g_free(g->location);
	g_free(g);

	return 1;
}

/**
 * Creates a new dataset
 *
 * \return dset The new dataset
 **/
static
void*
H5VL_julea_dataset_create (void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void** req)
{
	J_TRACE_FUNCTION(NULL);

	JHD_t* dset;

	hsize_t* dims;
	gint ndims;

	gchar* space_buf = NULL;
	gsize space_size;
	gchar* type_buf = NULL;
	gsize type_size;

	gsize data_size;

	bson_t* tmp;
	g_autoptr(JBatch) batch = NULL;
	gchar* tsloc;

	gpointer value;
	guint32 len;

	(void)lcpl_id;
	(void)dapl_id;
	(void)dxpl_id;
	(void)req;

	dset = g_new(JHD_t, 1);
	dset->name = g_strdup(name);
	dset->distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);

	type_buf = j_hdf5_encode_type("dataset_type_id", &type_id, dcpl_id, &type_size);
	space_buf = j_hdf5_encode_space("dataset_space_id", &space_id, dcpl_id, &space_size);
	data_size = H5Tget_size(type_id);

	ndims = H5Sget_simple_extent_ndims(space_id);
	dims = g_new(hsize_t, ndims);
	H5Sget_simple_extent_dims(space_id, dims, NULL);

	for (gint i = 0; i < ndims; i++)
	{
		data_size *= dims[i];
	}

	g_free(dims);

	dset->data_size = data_size;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	switch (loc_params->obj_type)
	{
		case H5I_FILE:
			{
				JHF_t* o = obj;

				dset->location = create_path(name, o->name);
				dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);
				j_distributed_object_create(dset->object, batch);
			}

			break;
		case H5I_GROUP:
			{
				JHG_t* o = obj;

				dset->location = create_path(name, o->location);
				dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);
				j_distributed_object_create(dset->object, batch);
			}
			break;
		case H5I_ATTR:
		case H5I_BADID:
		case H5I_DATATYPE:
		case H5I_DATASET:
		case H5I_DATASPACE:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_NTYPES:
		case H5I_SPACE_SEL_ITER:
		case H5I_UNINIT:
		case H5I_VFL:
		case H5I_VOL:
		default:
			g_assert_not_reached();
			exit(1);
	}

	tsloc = g_strdup_printf("%s_data", dset->location);
	dset->kv = j_kv_new("hdf5", tsloc);
	g_free(tsloc);

	tmp = j_hdf5_serialize_dataset(type_buf, type_size, space_buf, space_size, data_size, dset->distribution);
	value = bson_destroy_with_steal(tmp, TRUE, &len);
	j_kv_put(dset->kv, value, len, bson_free, batch);

	if (!j_batch_execute(batch))
	{
		// FIXME check return value properly
	}

	g_free(type_buf);
	g_free(space_buf);

	return (void *)dset;
}

/**
 * Opens a dataset
 *
 * \return dset The dataset
 **/
static
void*
H5VL_julea_dataset_open (void* obj, const H5VL_loc_params_t* loc_params, const char* name, hid_t dapl_id, hid_t dxpl_id, void** req)
{
	JHD_t *dset;
	g_autoptr(JBatch) batch = NULL;
	char* tsloc;

	gpointer value;
	guint32 len;

	(void)dapl_id;
	(void)dxpl_id;
	(void)req;

	dset = g_new(JHD_t, 1);
	dset->name = g_strdup(name);

	switch (loc_params->obj_type)
	{
		case H5I_FILE:
			{
				JHF_t *o = obj;
				dset->location = create_path(name, o->name);
			}

			break;
		case H5I_GROUP:
			{
				JHG_t *o = obj;
				dset->location = create_path(name, o->location);
			}
			break;
		case H5I_ATTR:
		case H5I_BADID:
		case H5I_DATATYPE:
		case H5I_DATASET:
		case H5I_DATASPACE:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_NTYPES:
		case H5I_SPACE_SEL_ITER:
		case H5I_UNINIT:
		case H5I_VFL:
		case H5I_VOL:
		default:
			g_assert_not_reached();
			exit(1);
	}

	tsloc = g_strdup_printf("%s_data", dset->location);
	dset->kv = j_kv_new("hdf5", tsloc);
	g_free(tsloc);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_kv_get(dset->kv, &value, &len, batch);

	if (j_batch_execute(batch))
	{
		bson_t kvdata[1];

		bson_init_static(kvdata, value, len);
		j_hdf5_deserialize_dataset(kvdata, dset, &(dset->data_size));
		g_free(value);
	}

	dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);

	return dset;
}

/**
 * Reads the data from the dataset
 **/
static
herr_t
H5VL_julea_dataset_read (void* dset, hid_t mem_type_id  __attribute__((unused)), hid_t mem_space_id  __attribute__((unused)), hid_t file_space_id  __attribute__((unused)), hid_t plist_id  __attribute__((unused)), void* buf, void** req  __attribute__((unused)))
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	JHD_t *d;
	guint64 bytes_read;

	d = (JHD_t *)dset;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	bytes_read = 0;

	g_assert(buf != NULL);

	g_assert(d->object != NULL);

	j_distributed_object_read(d->object, buf, d->data_size, 0, &bytes_read, batch);

	if (!j_batch_execute(batch))
	{
		// FIXME check return value properly
	}

	return 1;
}

/**
 * Provides get Functions of the dataset
 *
 * \return ret_value The error code
 **/
static
herr_t
H5VL_julea_dataset_get (void* dset, H5VL_dataset_get_t get_type, hid_t dxpl_id  __attribute__((unused)), void** req  __attribute__((unused)), va_list arguments)
{
	J_TRACE_FUNCTION(NULL);

	herr_t ret_value = 0;
	JHD_t *d;
	g_autoptr(JBatch) batch = NULL;

	gpointer value;
	guint32 len;

	d = (JHD_t *)dset;

	switch (get_type)
	{
	case H5VL_DATASET_GET_DAPL:
		break;
	case H5VL_DATASET_GET_DCPL:
		break;
	case H5VL_DATASET_GET_OFFSET:
		break;
	case H5VL_DATASET_GET_SPACE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);
		void* space;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->kv, &value, &len, batch);
		if (j_batch_execute(batch))
		{
			bson_t b[1];

			bson_init_static(b, value, len);
			space = j_hdf5_deserialize_space(b);
			*ret_id = H5Sdecode(space);
			free(space);
			g_free(value);
		}
	}
		break;
	case H5VL_DATASET_GET_SPACE_STATUS:
		break;
	case H5VL_DATASET_GET_STORAGE_SIZE:
		break;
	case H5VL_DATASET_GET_TYPE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);
		void* type;

		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->kv, &value, &len, batch);
		if (j_batch_execute(batch))
		{
			bson_t b[1];

			bson_init_static(b, value, len);
			type = j_hdf5_deserialize_type(b);
			*ret_id = H5Tdecode(type);
			free(type);
			g_free(value);
		}
	}
		break;
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}

	return ret_value;
}

/**
 * Writes the data to the dataset
 **/
static
herr_t
H5VL_julea_dataset_write (void* dset, hid_t mem_type_id  __attribute__((unused)), hid_t mem_space_id  __attribute__((unused)), hid_t file_space_id  __attribute__((unused)), hid_t plist_id  __attribute__((unused)), const void* buf, void** req  __attribute__((unused)))
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(JBatch) batch = NULL;
	JHD_t *d;
	guint64 bytes_written;

	d = (JHD_t *)dset;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	bytes_written = 0;

	j_distributed_object_write(d->object, buf, d->data_size, 0, &bytes_written, batch);

	if (!j_batch_execute(batch))
	{
		// FIXME check return value properly
	}

	return 1;
}

/**
 * Closes the dataset
 **/
static
herr_t
H5VL_julea_dataset_close (void* dset, hid_t dxpl_id  __attribute__((unused)), void** req  __attribute__((unused)))
{
	JHD_t *d = (JHD_t *)dset;
	if (d->distribution != NULL)
	{
		j_distribution_unref(d->distribution);
	}

	if (d->kv != NULL)
	{
		j_kv_unref(d->kv);
	}

	if (d->object != NULL)
	{
		j_distributed_object_unref(d->object);
	}

	g_free(d->name);
	free(d->location);
	free(d);
	return 1;
}

/**
 * The class providing the functions to HDF5
 **/
static
const
H5VL_class_t H5VL_julea_g =
{
	0,
	JULEA,
	"julea",	  /* name */
	0,
	H5VL_julea_init, /* initialize */
	H5VL_julea_term, /* terminate */
	{
		0,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	},
	{
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	},
	{
		/* attribute_cls */
		H5VL_julea_attr_create, /* create */
		H5VL_julea_attr_open,   /* open */
		H5VL_julea_attr_read,   /* read */
		H5VL_julea_attr_write,  /* write */
		H5VL_julea_attr_get,	/* get */
		NULL,					 //H5VL_julea_attr_specific,              /* specific */
		NULL,					 //H5VL_julea_attr_optional,              /* optional */
		H5VL_julea_attr_close   /* close */
	},
	{
		/* dataset_cls */
		H5VL_julea_dataset_create, /* create */
		H5VL_julea_dataset_open,   /* open */
		H5VL_julea_dataset_read,   /* read */
		H5VL_julea_dataset_write,  /* write */
		H5VL_julea_dataset_get,	/* get */
		NULL,						//H5VL_julea_dataset_specific,          /* specific */
		NULL,						//H5VL_julea_dataset_optional,          /* optional */
		H5VL_julea_dataset_close   /* close */
	},
	{
		/* datatype_cls */
		NULL, //H5VL_julea_datatype_commit, /* commit */
		NULL, //H5VL_julea_datatype_open,   /* open */
		NULL, //H5VL_julea_datatype_get,	/* get_size */
		NULL,						 //H5VL_julea_datatype_specific,         /* specific */
		NULL,						 //H5VL_julea_datatype_optional,         /* optional */
		NULL, //H5VL_julea_datatype_close   /* close */
	},
	{
		/* file_cls */
		H5VL_julea_file_create, /* create */
		H5VL_julea_file_open,   /* open */
		NULL, //H5VL_julea_file_get,	/* get */
		NULL,					 //H5VL_julea_file_specific,            /* specific */
		NULL,					 //H5VL_julea_file_optional,            /* optional */
		H5VL_julea_file_close   /* close */
	},
	{
		/* group_cls */
		H5VL_julea_group_create, /* create */
		H5VL_julea_group_open,   /* open */
		NULL, //H5VL_julea_group_get,	/* get */
		NULL,					  //H5VL_julea_group_specific,           /* specific */
		NULL,					  //H5VL_julea_group_optional,           /* optional */
		H5VL_julea_group_close   /* close */
	},
	{
		/* link_cls */
		NULL, //H5VL_julea_link_create,                /* create */
		NULL, //H5VL_julea_link_copy,                  /* copy */
		NULL, //H5VL_julea_link_move,                  /* move */
		NULL, //H5VL_julea_link_get,                   /* get */
		NULL, //H5VL_julea_link_specific,              /* specific */
		NULL, //H5VL_julea_link_optional,              /* optional */
	},
	{
		/* object_cls */
		NULL, //H5VL_julea_object_open,                        /* open */
		NULL, //H5VL_julea_object_copy,                /* copy */
		NULL, //H5VL_julea_object_get,                 /* get */
		NULL, //H5VL_julea_object_specific,                    /* specific */
		NULL, //H5VL_julea_object_optional,            /* optional */
	},
	{
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	},
	NULL
};

/**
 * Provides the plugin type
 **/
H5PL_type_t
H5PLget_plugin_type (void)
{
	return H5PL_TYPE_VOL;
}

/**
 * Provides a pointer to the plugin structure
 **/
const void*
H5PLget_plugin_info (void)
{
	return &H5VL_julea_g;
}
